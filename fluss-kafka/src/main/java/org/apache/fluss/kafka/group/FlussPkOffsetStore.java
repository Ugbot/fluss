/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.kafka.group;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.exception.DatabaseAlreadyExistException;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Durable {@link OffsetStore} backed by a Fluss PK table {@code kafka.__consumer_offsets__} (see
 * design doc 0004 §1). Replaces the legacy {@link ZkOffsetStore} implementation: schema and access
 * path are native Fluss, no ZK znodes, no JSON blob.
 *
 * <p>Design decisions:
 *
 * <ul>
 *   <li><b>Lazy bootstrap.</b> The offsets table is created on first use. The database is created
 *       transparently if missing. This mirrors the lazy-bootstrap pattern used elsewhere in the
 *       Kafka plugin (see {@code KafkaTableFactory} for data tables).
 *   <li><b>Write-through cache.</b> A {@link ConcurrentMap} fronts reads; successful upserts
 *       populate the cache. Reads fall back to {@link Lookuper} on a miss.
 *   <li><b>Write path.</b> {@link UpsertWriter#upsert(InternalRow)} per commit; we await the future
 *       so the caller sees a stable durable-write semantic (OffsetCommit is Kafka-slow anyway — ~5s
 *       per consumer).
 *   <li><b>Read path.</b> {@link Lookuper#lookup(InternalRow)} on the full PK {@code (group_id,
 *       topic, partition)}.
 * </ul>
 *
 * <p>{@link #knownGroupIds()} / {@link #groupExists(String)} return the union of the local cache
 * only. This is consistent with the {@link OffsetStore} contract during the lifetime of one tablet
 * server — callers that require cross-restart enumeration must issue a scan instead. This is a
 * Phase-2D trade-off (see 0004 §1 "Open questions" for tombstone/GC follow-up).
 *
 * <p>Thread safety: public methods are safe to call concurrently. The {@link UpsertWriter} and
 * {@link Lookuper} instances held here are bound to a single {@link Table} view and are treated as
 * owned by this object — i.e. the fluss-client's internal locking covers concurrent writes / reads.
 */
@Internal
public final class FlussPkOffsetStore implements OffsetStore, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(FlussPkOffsetStore.class);

    /**
     * Bound on waiting for a single commit/fetch round trip to complete. Commit cadence is ~5s per
     * consumer, and {@code request.timeout.ms} on the Kafka client is typically 30s, so 20s is
     * comfortably below the client's own timeout.
     */
    private static final long ROUND_TRIP_TIMEOUT_SECONDS = 20L;

    private final Connection connection;
    private final TablePath tablePath;
    private final ConcurrentMap<CacheKey, CommittedOffset> cache = new ConcurrentHashMap<>();

    /**
     * Guarded by {@code tableLock}: once a non-null {@link Table} lands here, the underlying Fluss
     * table is known to exist. Created lazily on first use.
     */
    @Nullable private volatile Table table;

    private final Object tableLock = new Object();

    public FlussPkOffsetStore(Connection connection, String kafkaDatabase) {
        this.connection = connection;
        this.tablePath = ConsumerOffsetsTable.tablePath(kafkaDatabase);
    }

    @Override
    public void commit(
            String groupId,
            String topic,
            int partition,
            long offset,
            int leaderEpoch,
            @Nullable String metadata)
            throws Exception {
        Table t = ensureTable();
        UpsertWriter writer = t.newUpsert().createWriter();
        GenericRow row = buildRow(groupId, topic, partition, offset, leaderEpoch, metadata);
        try {
            writer.upsert(row).get(ROUND_TRIP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException ee) {
            throw unwrap(ee);
        }
        writer.flush();
        cache.put(
                new CacheKey(groupId, topic, partition),
                new CommittedOffset(offset, leaderEpoch, metadata));
    }

    @Override
    public Optional<CommittedOffset> fetch(String groupId, String topic, int partition)
            throws Exception {
        CacheKey key = new CacheKey(groupId, topic, partition);
        CommittedOffset cached = cache.get(key);
        if (cached != null) {
            return Optional.of(cached);
        }
        Table t = ensureTable();
        Lookuper lookuper = t.newLookup().createLookuper();
        GenericRow keyRow = new GenericRow(3);
        keyRow.setField(0, BinaryString.fromString(groupId));
        keyRow.setField(1, BinaryString.fromString(topic));
        keyRow.setField(2, partition);
        InternalRow found;
        try {
            found =
                    lookuper.lookup(keyRow)
                            .get(ROUND_TRIP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                            .getSingletonRow();
        } catch (ExecutionException ee) {
            throw unwrap(ee);
        }
        if (found == null) {
            return Optional.empty();
        }
        CommittedOffset co = decodeRow(found);
        cache.put(key, co);
        return Optional.of(co);
    }

    @Override
    public void delete(String groupId, String topic, int partition) throws Exception {
        Table t = ensureTable();
        UpsertWriter writer = t.newUpsert().createWriter();
        // Column order: group_id, topic, partition, committed_offset, leader_epoch, metadata,
        // commit_time, expire_time. Fluss delete requires all columns present even though only
        // the PK is semantically consulted.
        GenericRow full = new GenericRow(8);
        full.setField(0, BinaryString.fromString(groupId));
        full.setField(1, BinaryString.fromString(topic));
        full.setField(2, partition);
        full.setField(3, 0L);
        full.setField(4, null);
        full.setField(5, null);
        full.setField(6, TimestampLtz.fromEpochMillis(System.currentTimeMillis()));
        full.setField(7, null);
        try {
            writer.delete(full).get(ROUND_TRIP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException ee) {
            throw unwrap(ee);
        }
        writer.flush();
        cache.remove(new CacheKey(groupId, topic, partition));
    }

    @Override
    public void deleteGroup(String groupId) {
        // Without a server-side prefix-delete primitive (see 0004 §1 "Open questions"), we can
        // only tombstone entries we currently know about via the local cache. Cross-restart bulk
        // deletion for this store is out of scope for Phase 2D; an explicit cron or admin-driven
        // sweep lands with Phase 2E.
        for (CacheKey k : new HashSet<>(cache.keySet())) {
            if (k.groupId.equals(groupId)) {
                cache.remove(k);
                try {
                    delete(k.groupId, k.topic, k.partition);
                } catch (Exception e) {
                    LOG.warn(
                            "Failed to delete offset tombstone for ({}, {}, {})",
                            k.groupId,
                            k.topic,
                            k.partition,
                            e);
                }
            }
        }
    }

    @Override
    public Set<String> knownGroupIds() {
        Set<String> groups = new HashSet<>();
        for (CacheKey k : cache.keySet()) {
            groups.add(k.groupId);
        }
        return groups;
    }

    @Override
    public boolean groupExists(String groupId) {
        for (CacheKey k : cache.keySet()) {
            if (k.groupId.equals(groupId)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void close() {
        Table t = table;
        if (t != null) {
            try {
                t.close();
            } catch (Exception e) {
                LOG.warn("Failed to close offsets Table handle", e);
            }
        }
        // The Connection is owned by the caller and closed separately.
    }

    private Table ensureTable() throws Exception {
        Table t = table;
        if (t != null) {
            return t;
        }
        synchronized (tableLock) {
            if (table != null) {
                return table;
            }
            bootstrapTableIfMissing();
            table = connection.getTable(tablePath);
            return table;
        }
    }

    private void bootstrapTableIfMissing() throws Exception {
        Admin admin = connection.getAdmin();
        try {
            admin.createDatabase(tablePath.getDatabaseName(), DatabaseDescriptor.EMPTY, true)
                    .get(ROUND_TRIP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (!(cause instanceof DatabaseAlreadyExistException)) {
                throw unwrap(ee);
            }
        }
        try {
            admin.createTable(tablePath, ConsumerOffsetsTable.descriptor(), true)
                    .get(ROUND_TRIP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (!(cause instanceof TableAlreadyExistException)) {
                throw unwrap(ee);
            }
        }
    }

    private GenericRow buildRow(
            String groupId,
            String topic,
            int partition,
            long offset,
            int leaderEpoch,
            @Nullable String metadata) {
        GenericRow row = new GenericRow(8);
        row.setField(0, BinaryString.fromString(groupId));
        row.setField(1, BinaryString.fromString(topic));
        row.setField(2, partition);
        row.setField(3, offset);
        row.setField(4, leaderEpoch);
        row.setField(5, metadata == null ? null : BinaryString.fromString(metadata));
        row.setField(6, TimestampLtz.fromEpochMillis(System.currentTimeMillis()));
        row.setField(7, null);
        return row;
    }

    private static CommittedOffset decodeRow(InternalRow row) {
        long offset = row.getLong(3);
        int leaderEpoch = row.isNullAt(4) ? -1 : row.getInt(4);
        String metadata = row.isNullAt(5) ? null : row.getString(5).toString();
        return new CommittedOffset(offset, leaderEpoch, metadata);
    }

    private static Exception unwrap(ExecutionException ee) {
        Throwable cause = ee.getCause();
        if (cause instanceof Exception) {
            return (Exception) cause;
        }
        return ee;
    }

    /** Primary-key identity (group_id, topic, partition). */
    private static final class CacheKey {
        final String groupId;
        final String topic;
        final int partition;

        CacheKey(String groupId, String topic, int partition) {
            this.groupId = groupId;
            this.topic = topic;
            this.partition = partition;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof CacheKey)) {
                return false;
            }
            CacheKey that = (CacheKey) o;
            return partition == that.partition
                    && groupId.equals(that.groupId)
                    && topic.equals(that.topic);
        }

        @Override
        public int hashCode() {
            return groupId.hashCode() * 31 * 31 + topic.hashCode() * 31 + partition;
        }
    }

    /** For tests: the table path this store is bound to. */
    public TablePath tablePath() {
        return tablePath;
    }
}
