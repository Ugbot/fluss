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
import org.apache.fluss.server.zk.ZooKeeperClient;

import javax.annotation.Nullable;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Durable {@link OffsetStore} backed by ZooKeeper znodes under {@code /fluss/kafka/offsets/...}.
 * Survives tablet-server restarts and is visible to every tablet server (so any Kafka-listener TS
 * can serve OffsetFetch after a coordinator hand-off).
 *
 * <p>Writes go through a fast in-memory cache plus a ZK write per commit. Reads are served from
 * cache when present; on a miss the store falls back to ZK and populates the cache.
 *
 * <p>Layout:
 *
 * <pre>
 *   /fluss/kafka/offsets/&lt;group&gt;/&lt;topic&gt;/&lt;partition&gt;  // JSON blob
 * </pre>
 *
 * <p>{@code group} and {@code topic} segments are URL-encoded so characters like {@code /} cannot
 * break the path. Partitions are plain integers.
 *
 * <p>This is a Phase 2E transitional design. A follow-up moves offsets into a proper Fluss PK table
 * ({@code kafka.__consumer_offsets__}); the path migration is documented in {@code
 * dev-docs/design/0004-kafka-group-coordinator.md}.
 */
@Internal
public final class ZkOffsetStore implements OffsetStore {

    private static final String ROOT = "/fluss/kafka/offsets";

    private final ZooKeeperClient zk;
    private final ConcurrentHashMap<CacheKey, CommittedOffset> cache = new ConcurrentHashMap<>();

    public ZkOffsetStore(ZooKeeperClient zk) {
        this.zk = zk;
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
        CommittedOffset co = new CommittedOffset(offset, leaderEpoch, metadata);
        zk.writeData(pathFor(groupId, topic, partition), encode(co));
        cache.put(new CacheKey(groupId, topic, partition), co);
    }

    @Override
    public Optional<CommittedOffset> fetch(String groupId, String topic, int partition)
            throws Exception {
        CacheKey key = new CacheKey(groupId, topic, partition);
        CommittedOffset cached = cache.get(key);
        if (cached != null) {
            return Optional.of(cached);
        }
        Optional<byte[]> data = zk.getOrEmpty(pathFor(groupId, topic, partition));
        if (!data.isPresent()) {
            return Optional.empty();
        }
        CommittedOffset co = decode(data.get());
        cache.put(key, co);
        return Optional.of(co);
    }

    @Override
    public void delete(String groupId, String topic, int partition) throws Exception {
        zk.deleteRecursive(pathFor(groupId, topic, partition));
        cache.remove(new CacheKey(groupId, topic, partition));
    }

    @Override
    public void deleteGroup(String groupId) throws Exception {
        zk.deleteRecursive(groupPath(groupId));
        cache.keySet().removeIf(k -> k.groupId.equals(groupId));
    }

    @Override
    public Set<String> knownGroupIds() throws Exception {
        List<String> encoded;
        try {
            encoded = zk.getChildren(ROOT);
        } catch (Exception e) {
            // Most likely the root znode does not exist yet.
            return new HashSet<>();
        }
        Set<String> out = new HashSet<>(encoded.size());
        for (String e : encoded) {
            out.add(decodeSegment(e));
        }
        return out;
    }

    @Override
    public boolean groupExists(String groupId) throws Exception {
        try {
            List<String> topics = zk.getChildren(groupPath(groupId));
            return !topics.isEmpty();
        } catch (Exception e) {
            return false;
        }
    }

    private static String pathFor(String groupId, String topic, int partition) {
        return groupPath(groupId) + "/" + encodeSegment(topic) + "/" + partition;
    }

    private static String groupPath(String groupId) {
        return ROOT + "/" + encodeSegment(groupId);
    }

    private static String encodeSegment(String raw) {
        try {
            return URLEncoder.encode(raw, StandardCharsets.UTF_8.name());
        } catch (java.io.UnsupportedEncodingException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static String decodeSegment(String encoded) {
        try {
            return URLDecoder.decode(encoded, StandardCharsets.UTF_8.name());
        } catch (java.io.UnsupportedEncodingException impossible) {
            throw new AssertionError(impossible);
        }
    }

    /**
     * Encode as a trivial line-delimited record. We deliberately avoid JSON to stay free of a JSON
     * dependency in fluss-kafka; the format is: {@code <offset>\t<leaderEpoch>\t<metadata>} with
     * metadata possibly empty (absent). Tab is used instead of newline to keep the whole blob on
     * one logical line.
     */
    private static byte[] encode(CommittedOffset co) {
        StringBuilder sb = new StringBuilder();
        sb.append(co.offset()).append('\t').append(co.leaderEpoch());
        if (co.metadata() != null) {
            sb.append('\t').append(co.metadata());
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static CommittedOffset decode(byte[] bytes) {
        String text = new String(bytes, StandardCharsets.UTF_8);
        String[] parts = text.split("\t", 3);
        long offset = Long.parseLong(parts[0]);
        int leaderEpoch = Integer.parseInt(parts[1]);
        String metadata = parts.length > 2 ? parts[2] : null;
        return new CommittedOffset(offset, leaderEpoch, metadata);
    }

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
}
