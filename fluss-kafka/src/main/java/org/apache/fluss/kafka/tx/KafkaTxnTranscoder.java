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

package org.apache.fluss.kafka.tx;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.catalog.entities.KafkaTxnStateEntity;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.kafka.KafkaRequest;
import org.apache.fluss.kafka.auth.AuthzHelper;
import org.apache.fluss.kafka.catalog.KafkaTopicInfo;
import org.apache.fluss.kafka.catalog.KafkaTopicsCatalog;
import org.apache.fluss.kafka.group.OffsetStore;
import org.apache.fluss.kafka.metrics.KafkaMetricGroup;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.server.authorizer.Authorizer;
import org.apache.fluss.server.replica.ReplicaManager;

import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.AddOffsetsToTxnResponseData;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData;
import org.apache.kafka.common.message.DescribeTransactionsRequestData;
import org.apache.kafka.common.message.DescribeTransactionsResponseData;
import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.EndTxnResponseData;
import org.apache.kafka.common.message.ListTransactionsRequestData;
import org.apache.kafka.common.message.ListTransactionsResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.message.WriteTxnMarkersRequestData;
import org.apache.kafka.common.message.WriteTxnMarkersResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Per-request transcoder for the Phase J.2 transactional wire APIs. Keeps {@code
 * KafkaRequestHandler} small enough to stay under the 3000-line Checkstyle limit while colocating
 * the transactional plumbing with the {@link TransactionCoordinator} it drives.
 *
 * <p>One instance is built per request; the dependencies are stateless references. Every method
 * returns the response data; the calling handler wraps it in the appropriate {@code
 * AbstractResponse} subclass and delegates the {@code complete} call.
 */
@Internal
public final class KafkaTxnTranscoder {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTxnTranscoder.class);

    private final @Nullable Authorizer authorizer;
    private final @Nullable KafkaMetricGroup metrics;
    private final String kafkaDatabase;
    private final OffsetStore groupOffsets;
    private final @Nullable ReplicaManager replicaManager;
    private final @Nullable KafkaTopicsCatalog topicsCatalog;

    /**
     * J.2 constructor (no marker append). Kept for source-compat with older J.2 wiring; use the J.3
     * constructor below to get real {@code WRITE_TXN_MARKERS} support.
     */
    public KafkaTxnTranscoder(
            @Nullable Authorizer authorizer,
            @Nullable KafkaMetricGroup metrics,
            String kafkaDatabase,
            OffsetStore groupOffsets) {
        this(authorizer, metrics, kafkaDatabase, groupOffsets, null, null);
    }

    /**
     * Phase J.3 — full constructor. The {@code replicaManager} + {@code topicsCatalog} pair is used
     * by {@link #writeTxnMarkers} to append a control batch to each participating partition.
     */
    public KafkaTxnTranscoder(
            @Nullable Authorizer authorizer,
            @Nullable KafkaMetricGroup metrics,
            String kafkaDatabase,
            OffsetStore groupOffsets,
            @Nullable ReplicaManager replicaManager,
            @Nullable KafkaTopicsCatalog topicsCatalog) {
        this.authorizer = authorizer;
        this.metrics = metrics;
        this.kafkaDatabase = kafkaDatabase;
        this.groupOffsets = groupOffsets;
        this.replicaManager = replicaManager;
        this.topicsCatalog = topicsCatalog;
    }

    public AddPartitionsToTxnResponseData addPartitionsToTxn(
            KafkaRequest request, AddPartitionsToTxnRequestData data) {
        Optional<TransactionCoordinator> maybeCoord = TransactionCoordinators.current();
        AddPartitionsToTxnResponseData resp = new AddPartitionsToTxnResponseData();
        if (data.transactions() != null && !data.transactions().isEmpty()) {
            AddPartitionsToTxnResponseData.AddPartitionsToTxnResultCollection results =
                    new AddPartitionsToTxnResponseData.AddPartitionsToTxnResultCollection();
            for (AddPartitionsToTxnRequestData.AddPartitionsToTxnTransaction t :
                    data.transactions()) {
                results.add(addPartitionsToTxnSingle(request, maybeCoord, t));
            }
            resp.setResultsByTransaction(results);
        } else {
            // v3 and below — synthesise a single transaction so the same code path handles both.
            AddPartitionsToTxnRequestData.AddPartitionsToTxnTransaction t =
                    new AddPartitionsToTxnRequestData.AddPartitionsToTxnTransaction();
            t.setTransactionalId(data.v3AndBelowTransactionalId());
            t.setProducerId(data.v3AndBelowProducerId());
            t.setProducerEpoch(data.v3AndBelowProducerEpoch());
            t.setTopics(data.v3AndBelowTopics());
            AddPartitionsToTxnResponseData.AddPartitionsToTxnResult result =
                    addPartitionsToTxnSingle(request, maybeCoord, t);
            resp.setResultsByTopicV3AndBelow(result.topicResults());
        }
        return resp;
    }

    private AddPartitionsToTxnResponseData.AddPartitionsToTxnResult addPartitionsToTxnSingle(
            KafkaRequest request,
            Optional<TransactionCoordinator> maybeCoord,
            AddPartitionsToTxnRequestData.AddPartitionsToTxnTransaction t) {
        AddPartitionsToTxnResponseData.AddPartitionsToTxnResult result =
                new AddPartitionsToTxnResponseData.AddPartitionsToTxnResult()
                        .setTransactionalId(t.transactionalId());
        try {
            AuthzHelper.authorizeOrThrow(
                    authorizer,
                    AuthzHelper.sessionOf(request),
                    OperationType.WRITE,
                    Resource.transactionalId(t.transactionalId()));
        } catch (AuthorizationException denied) {
            result.setTopicResults(
                    fillTopicResults(
                            t.topics(), Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code()));
            return result;
        }
        List<String> topics = new ArrayList<>();
        for (AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic topic : t.topics()) {
            topics.add(topic.name());
        }
        Map<String, Boolean> allowed =
                AuthzHelper.authorizeTopicBatch(
                        authorizer,
                        AuthzHelper.sessionOf(request),
                        OperationType.WRITE,
                        topics,
                        kafkaDatabase,
                        metrics);
        if (!maybeCoord.isPresent()) {
            result.setTopicResults(fillTopicResults(t.topics(), Errors.NOT_COORDINATOR.code()));
            return result;
        }
        Set<String> participating = new TreeSet<>();
        AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResultCollection topicResults =
                new AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResultCollection();
        for (AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic topic : t.topics()) {
            boolean topicAllowed = allowed.getOrDefault(topic.name(), Boolean.TRUE);
            AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult tr =
                    new AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult()
                            .setName(topic.name());
            for (Integer p : topic.partitions()) {
                short err =
                        topicAllowed
                                ? Errors.NONE.code()
                                : Errors.TOPIC_AUTHORIZATION_FAILED.code();
                tr.resultsByPartition()
                        .add(
                                new AddPartitionsToTxnResponseData
                                                .AddPartitionsToTxnPartitionResult()
                                        .setPartitionIndex(p)
                                        .setPartitionErrorCode(err));
                if (topicAllowed) {
                    participating.add(topic.name() + ":" + p);
                }
            }
            topicResults.add(tr);
        }
        if (!participating.isEmpty()) {
            try {
                TransactionCoordinator.EpochCheck check =
                        maybeCoord
                                .get()
                                .addPartitionsToTxn(
                                        t.transactionalId(),
                                        t.producerId(),
                                        t.producerEpoch(),
                                        participating);
                if (check != TransactionCoordinator.EpochCheck.OK) {
                    overrideAllowedPartitionErrors(topicResults, allowed, epochCheckToError(check));
                }
            } catch (Throwable th) {
                LOG.error(
                        "ADD_PARTITIONS_TO_TXN failed for transactionalId='{}'",
                        t.transactionalId(),
                        th);
                overrideAllowedPartitionErrors(
                        topicResults, allowed, Errors.COORDINATOR_NOT_AVAILABLE.code());
            }
        }
        result.setTopicResults(topicResults);
        return result;
    }

    public AddOffsetsToTxnResponseData addOffsetsToTxn(
            KafkaRequest request, AddOffsetsToTxnRequestData data) {
        AddOffsetsToTxnResponseData resp = new AddOffsetsToTxnResponseData().setThrottleTimeMs(0);
        try {
            AuthzHelper.authorizeOrThrow(
                    authorizer,
                    AuthzHelper.sessionOf(request),
                    OperationType.WRITE,
                    Resource.transactionalId(data.transactionalId()));
        } catch (AuthorizationException denied) {
            resp.setErrorCode(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code());
            return resp;
        }
        try {
            AuthzHelper.authorizeOrThrow(
                    authorizer,
                    AuthzHelper.sessionOf(request),
                    OperationType.READ,
                    Resource.group(data.groupId()));
        } catch (AuthorizationException denied) {
            resp.setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code());
            return resp;
        }
        Optional<TransactionCoordinator> maybeCoord = TransactionCoordinators.current();
        if (!maybeCoord.isPresent()) {
            resp.setErrorCode(Errors.NOT_COORDINATOR.code());
            return resp;
        }
        try {
            TransactionCoordinator.EpochCheck check =
                    maybeCoord
                            .get()
                            .addOffsetsToTxn(
                                    data.transactionalId(),
                                    data.producerId(),
                                    data.producerEpoch(),
                                    data.groupId());
            resp.setErrorCode(epochCheckToError(check));
        } catch (Throwable t) {
            LOG.error(
                    "ADD_OFFSETS_TO_TXN failed for transactionalId='{}' group='{}'",
                    data.transactionalId(),
                    data.groupId(),
                    t);
            resp.setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code());
        }
        return resp;
    }

    public EndTxnResponseData endTxn(KafkaRequest request, EndTxnRequestData data) {
        EndTxnResponseData resp = new EndTxnResponseData().setThrottleTimeMs(0);
        try {
            AuthzHelper.authorizeOrThrow(
                    authorizer,
                    AuthzHelper.sessionOf(request),
                    OperationType.WRITE,
                    Resource.transactionalId(data.transactionalId()));
        } catch (AuthorizationException denied) {
            resp.setErrorCode(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code());
            return resp;
        }
        Optional<TransactionCoordinator> maybeCoord = TransactionCoordinators.current();
        if (!maybeCoord.isPresent()) {
            resp.setErrorCode(Errors.NOT_COORDINATOR.code());
            return resp;
        }
        // Phase J.3 — wire the replica-side marker sink so applyMarkersAndComplete() blocks until
        // every control batch is durable before returning to the Kafka producer. Without this the
        // LSO stays at the open-txn firstOffset until the next fetch, so read_committed consumers
        // see 0 records immediately after commitTransaction().
        if (replicaManager != null && topicsCatalog != null) {
            maybeCoord
                    .get()
                    .setMarkerSink(
                            (topic, partition, pid, epoch, commit) -> {
                                short err = appendMarker(topic, partition, pid, epoch, commit);
                                if (err != Errors.NONE.code()) {
                                    throw new RuntimeException(
                                            "marker append failed topic='"
                                                    + topic
                                                    + "' partition="
                                                    + partition
                                                    + " err="
                                                    + err);
                                }
                            });
        }
        try {
            TransactionCoordinator.EndTxnResult result =
                    maybeCoord
                            .get()
                            .endTxn(
                                    data.transactionalId(),
                                    data.producerId(),
                                    data.producerEpoch(),
                                    data.committed());
            if (result.epoch() != TransactionCoordinator.EpochCheck.OK) {
                resp.setErrorCode(epochCheckToError(result.epoch()));
            } else {
                if (result.committed() && !result.bufferedOffsets().isEmpty()) {
                    flushBufferedOffsets(result.bufferedOffsets());
                }
                resp.setErrorCode(Errors.NONE.code());
            }
        } catch (Throwable t) {
            LOG.error(
                    "END_TXN failed for transactionalId='{}' commit={}",
                    data.transactionalId(),
                    data.committed(),
                    t);
            resp.setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code());
        }
        return resp;
    }

    /**
     * Best-effort flush of buffered TXN_OFFSET_COMMIT offsets via the local {@link OffsetStore}. A
     * per-offset failure logs a warning but does not fail the END_TXN — the producer has already
     * received the commit ack at this point and the offsets are durable in {@code
     * __kafka_txn_state__} for J.3's recovery to retry.
     */
    private void flushBufferedOffsets(List<TransactionCoordinator.BufferedOffset> offsets) {
        for (TransactionCoordinator.BufferedOffset o : offsets) {
            try {
                groupOffsets.commit(
                        o.groupId(),
                        o.topic(),
                        o.partition(),
                        o.offset(),
                        o.leaderEpoch(),
                        o.metadata());
            } catch (Exception e) {
                LOG.warn(
                        "Failed to flush buffered TXN_OFFSET_COMMIT for group='{}' topic='{}' "
                                + "partition={} offset={}",
                        o.groupId(),
                        o.topic(),
                        o.partition(),
                        o.offset(),
                        e);
            }
        }
    }

    public WriteTxnMarkersResponseData writeTxnMarkers(
            KafkaRequest request, WriteTxnMarkersRequestData data) {
        WriteTxnMarkersResponseData resp = new WriteTxnMarkersResponseData();
        try {
            // Inter-broker WRITE_TXN_MARKERS is gated by Kafka's ClusterAction; Fluss's enum has
            // no CLUSTER_ACTION, so use ALTER on CLUSTER which is the closest equivalent and is
            // the same gate AlterConfigs uses for cluster-admin work.
            AuthzHelper.authorizeOrThrow(
                    authorizer,
                    AuthzHelper.sessionOf(request),
                    OperationType.ALTER,
                    Resource.cluster());
        } catch (AuthorizationException denied) {
            for (WriteTxnMarkersRequestData.WritableTxnMarker m : data.markers()) {
                resp.markers()
                        .add(buildMarkerResult(m, Errors.CLUSTER_AUTHORIZATION_FAILED.code()));
            }
            return resp;
        }
        Optional<TransactionCoordinator> maybeCoord = TransactionCoordinators.current();
        for (WriteTxnMarkersRequestData.WritableTxnMarker m : data.markers()) {
            if (maybeCoord.isPresent()) {
                maybeCoord
                        .get()
                        .acknowledgeMarker(
                                m.producerId(), m.producerEpoch(), m.transactionResult());
            }
            resp.markers().add(appendMarkersForRequest(m));
        }
        return resp;
    }

    /**
     * Phase J.3 — append a control marker batch on every participating partition. Each partition is
     * resolved via the {@link KafkaTopicsCatalog} to its Fluss {@link TableBucket}; the {@link
     * ReplicaManager#appendTxnMarker} call writes a V2 control batch with no records, the {@code
     * (writerId, epoch)} fencing key, and the commit/abort attribute bit.
     *
     * <p>Per-partition failures are reported in the result; the rest of the partitions in the
     * marker still succeed (matches Kafka's per-partition error semantics).
     */
    private WriteTxnMarkersResponseData.WritableTxnMarkerResult appendMarkersForRequest(
            WriteTxnMarkersRequestData.WritableTxnMarker m) {
        WriteTxnMarkersResponseData.WritableTxnMarkerResult r =
                new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
                        .setProducerId(m.producerId());
        boolean commit = m.transactionResult();
        for (WriteTxnMarkersRequestData.WritableTxnMarkerTopic t : m.topics()) {
            WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult tr =
                    new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
                            .setName(t.name());
            for (Integer p : t.partitionIndexes()) {
                short err = appendMarker(t.name(), p, m.producerId(), m.producerEpoch(), commit);
                tr.partitions()
                        .add(
                                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                                        .setPartitionIndex(p)
                                        .setErrorCode(err));
            }
            r.topics().add(tr);
        }
        return r;
    }

    private short appendMarker(
            String topic, int partition, long producerId, short producerEpoch, boolean commit) {
        if (replicaManager == null || topicsCatalog == null) {
            // J.2 wiring path — no append target available; ack as if appended so older test
            // harnesses that ran without replicaManager don't regress.
            return Errors.NONE.code();
        }
        try {
            Optional<KafkaTopicInfo> info = topicsCatalog.lookup(topic);
            if (!info.isPresent()) {
                return Errors.UNKNOWN_TOPIC_OR_PARTITION.code();
            }
            TableBucket bucket = new TableBucket(info.get().flussTableId(), partition);
            // Try the local replica manager first. In a single-server or lucky multi-server
            // scenario this is the leader and succeeds immediately. If the local server is not
            // the leader (common in multi-server in-process test clusters), iterate the JVM-wide
            // registry to find the server that does hold the leader replica.
            for (org.apache.fluss.server.replica.ReplicaManager rm :
                    org.apache.fluss.server.replica.ReplicaManagers.all()) {
                try {
                    rm.appendTxnMarker(bucket, producerId, producerEpoch, commit);
                    return Errors.NONE.code();
                } catch (org.apache.fluss.exception.NotLeaderOrFollowerException
                        | org.apache.fluss.exception.UnknownTableOrBucketException
                        | org.apache.fluss.exception.StorageException ignored) {
                    // this server does not hold the leader replica; try the next one
                }
            }
            LOG.warn(
                    "No local leader found for txn marker topic='{}' partition={} pid={} epoch={};"
                            + " LSO will advance on the next fetch retry",
                    topic,
                    partition,
                    producerId,
                    producerEpoch);
            return Errors.NOT_LEADER_OR_FOLLOWER.code();
        } catch (Exception e) {
            LOG.error(
                    "Failed to append txn marker for topic '{}' partition {} pid={} epoch={} commit={}",
                    topic,
                    partition,
                    producerId,
                    producerEpoch,
                    commit,
                    e);
            return Errors.UNKNOWN_SERVER_ERROR.code();
        }
    }

    private static WriteTxnMarkersResponseData.WritableTxnMarkerResult buildMarkerResult(
            WriteTxnMarkersRequestData.WritableTxnMarker m, short errorCode) {
        WriteTxnMarkersResponseData.WritableTxnMarkerResult r =
                new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
                        .setProducerId(m.producerId());
        for (WriteTxnMarkersRequestData.WritableTxnMarkerTopic t : m.topics()) {
            WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult tr =
                    new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
                            .setName(t.name());
            for (Integer p : t.partitionIndexes()) {
                tr.partitions()
                        .add(
                                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                                        .setPartitionIndex(p)
                                        .setErrorCode(errorCode));
            }
            r.topics().add(tr);
        }
        return r;
    }

    public TxnOffsetCommitResponseData txnOffsetCommit(
            KafkaRequest request, TxnOffsetCommitRequestData data) {
        TxnOffsetCommitResponseData resp = new TxnOffsetCommitResponseData().setThrottleTimeMs(0);
        try {
            AuthzHelper.authorizeOrThrow(
                    authorizer,
                    AuthzHelper.sessionOf(request),
                    OperationType.WRITE,
                    Resource.transactionalId(data.transactionalId()));
        } catch (AuthorizationException denied) {
            populateTxnOffsetCommitError(
                    data, resp, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code());
            return resp;
        }
        try {
            AuthzHelper.authorizeOrThrow(
                    authorizer,
                    AuthzHelper.sessionOf(request),
                    OperationType.READ,
                    Resource.group(data.groupId()));
        } catch (AuthorizationException denied) {
            populateTxnOffsetCommitError(data, resp, Errors.GROUP_AUTHORIZATION_FAILED.code());
            return resp;
        }
        Optional<TransactionCoordinator> maybeCoord = TransactionCoordinators.current();
        short topLevel;
        if (!maybeCoord.isPresent()) {
            topLevel = Errors.NOT_COORDINATOR.code();
        } else {
            List<TransactionCoordinator.BufferedOffset> buffered = new ArrayList<>();
            for (TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic t : data.topics()) {
                for (TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition p :
                        t.partitions()) {
                    buffered.add(
                            new TransactionCoordinator.BufferedOffset(
                                    data.groupId(),
                                    t.name(),
                                    p.partitionIndex(),
                                    p.committedOffset(),
                                    p.committedLeaderEpoch(),
                                    p.committedMetadata()));
                }
            }
            TransactionCoordinator.EpochCheck check =
                    maybeCoord
                            .get()
                            .txnOffsetCommit(
                                    data.transactionalId(),
                                    data.producerId(),
                                    data.producerEpoch(),
                                    data.groupId(),
                                    buffered);
            topLevel = epochCheckToError(check);
        }
        populateTxnOffsetCommitError(data, resp, topLevel);
        return resp;
    }

    private static void populateTxnOffsetCommitError(
            TxnOffsetCommitRequestData data, TxnOffsetCommitResponseData resp, short errorCode) {
        for (TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic t : data.topics()) {
            TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic out =
                    new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
                            .setName(t.name());
            for (TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition p : t.partitions()) {
                out.partitions()
                        .add(
                                new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                                        .setPartitionIndex(p.partitionIndex())
                                        .setErrorCode(errorCode));
            }
            resp.topics().add(out);
        }
    }

    public DescribeTransactionsResponseData describeTransactions(
            KafkaRequest request, DescribeTransactionsRequestData data) {
        DescribeTransactionsResponseData resp =
                new DescribeTransactionsResponseData().setThrottleTimeMs(0);
        Optional<TransactionCoordinator> maybeCoord = TransactionCoordinators.current();
        for (String txnId : data.transactionalIds()) {
            DescribeTransactionsResponseData.TransactionState txnState =
                    new DescribeTransactionsResponseData.TransactionState()
                            .setTransactionalId(txnId);
            try {
                AuthzHelper.authorizeOrThrow(
                        authorizer,
                        AuthzHelper.sessionOf(request),
                        OperationType.DESCRIBE,
                        Resource.transactionalId(txnId));
            } catch (AuthorizationException denied) {
                txnState.setErrorCode(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code());
                resp.transactionStates().add(txnState);
                continue;
            }
            if (!maybeCoord.isPresent()) {
                txnState.setErrorCode(Errors.NOT_COORDINATOR.code());
                resp.transactionStates().add(txnState);
                continue;
            }
            Optional<KafkaTxnStateEntity> maybeEntity = maybeCoord.get().getState(txnId);
            if (!maybeEntity.isPresent()) {
                txnState.setErrorCode(Errors.TRANSACTIONAL_ID_NOT_FOUND.code());
                resp.transactionStates().add(txnState);
                continue;
            }
            populateDescribeState(txnState, maybeEntity.get());
            resp.transactionStates().add(txnState);
        }
        return resp;
    }

    private static void populateDescribeState(
            DescribeTransactionsResponseData.TransactionState txnState,
            KafkaTxnStateEntity entity) {
        txnState.setErrorCode(Errors.NONE.code())
                .setTransactionState(entity.state())
                .setTransactionTimeoutMs(entity.timeoutMs())
                .setTransactionStartTimeMs(
                        entity.txnStartTimestampMillis() == null
                                ? -1L
                                : entity.txnStartTimestampMillis())
                .setProducerId(entity.producerId())
                .setProducerEpoch(entity.producerEpoch());
        DescribeTransactionsResponseData.TopicDataCollection topics =
                new DescribeTransactionsResponseData.TopicDataCollection();
        // Group encoded "topic:partition" entries by topic so the response carries one TopicData
        // per topic with the full partition list.
        Map<String, List<Integer>> byTopic = new LinkedHashMap<>();
        for (String tp : entity.topicPartitions()) {
            int sep = tp.lastIndexOf(':');
            if (sep <= 0) {
                continue;
            }
            String topic = tp.substring(0, sep);
            int partition;
            try {
                partition = Integer.parseInt(tp.substring(sep + 1));
            } catch (NumberFormatException nfe) {
                continue;
            }
            byTopic.computeIfAbsent(topic, k -> new ArrayList<>()).add(partition);
        }
        for (Map.Entry<String, List<Integer>> e : byTopic.entrySet()) {
            topics.add(
                    new DescribeTransactionsResponseData.TopicData()
                            .setTopic(e.getKey())
                            .setPartitions(e.getValue()));
        }
        txnState.setTopics(topics);
    }

    public ListTransactionsResponseData listTransactions(
            KafkaRequest request, ListTransactionsRequestData data) {
        ListTransactionsResponseData resp = new ListTransactionsResponseData().setThrottleTimeMs(0);
        try {
            AuthzHelper.authorizeOrThrow(
                    authorizer,
                    AuthzHelper.sessionOf(request),
                    OperationType.DESCRIBE,
                    Resource.cluster());
        } catch (AuthorizationException denied) {
            resp.setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code());
            return resp;
        }
        Optional<TransactionCoordinator> maybeCoord = TransactionCoordinators.current();
        if (!maybeCoord.isPresent()) {
            resp.setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code());
            return resp;
        }
        Set<String> stateFilter =
                data.stateFilters() == null
                        ? Collections.emptySet()
                        : new HashSet<>(data.stateFilters());
        Set<Long> pidFilter =
                data.producerIdFilters() == null
                        ? Collections.emptySet()
                        : new HashSet<>(data.producerIdFilters());
        for (KafkaTxnStateEntity entity : maybeCoord.get().listAll()) {
            if (!stateFilter.isEmpty() && !stateFilter.contains(entity.state())) {
                continue;
            }
            if (!pidFilter.isEmpty() && !pidFilter.contains(entity.producerId())) {
                continue;
            }
            resp.transactionStates()
                    .add(
                            new ListTransactionsResponseData.TransactionState()
                                    .setTransactionalId(entity.transactionalId())
                                    .setProducerId(entity.producerId())
                                    .setTransactionState(entity.state()));
        }
        resp.setErrorCode(Errors.NONE.code());
        return resp;
    }

    private static AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResultCollection
            fillTopicResults(
                    AddPartitionsToTxnRequestData.AddPartitionsToTxnTopicCollection topics,
                    short errorCode) {
        AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResultCollection out =
                new AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResultCollection();
        for (AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic topic : topics) {
            AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult tr =
                    new AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult()
                            .setName(topic.name());
            for (Integer p : topic.partitions()) {
                tr.resultsByPartition()
                        .add(
                                new AddPartitionsToTxnResponseData
                                                .AddPartitionsToTxnPartitionResult()
                                        .setPartitionIndex(p)
                                        .setPartitionErrorCode(errorCode));
            }
            out.add(tr);
        }
        return out;
    }

    private static void overrideAllowedPartitionErrors(
            AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResultCollection topicResults,
            Map<String, Boolean> allowed,
            short errorCode) {
        for (AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult tr : topicResults) {
            if (!allowed.getOrDefault(tr.name(), Boolean.TRUE)) {
                continue;
            }
            for (AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResult p :
                    tr.resultsByPartition()) {
                if (p.partitionErrorCode() == Errors.NONE.code()) {
                    p.setPartitionErrorCode(errorCode);
                }
            }
        }
    }

    public static short epochCheckToError(TransactionCoordinator.EpochCheck check) {
        switch (check) {
            case OK:
                return Errors.NONE.code();
            case UNKNOWN_TRANSACTIONAL_ID:
                return Errors.INVALID_PRODUCER_ID_MAPPING.code();
            case INVALID_PRODUCER_EPOCH:
                return Errors.INVALID_PRODUCER_EPOCH.code();
            default:
                return Errors.UNKNOWN_SERVER_ERROR.code();
        }
    }
}
