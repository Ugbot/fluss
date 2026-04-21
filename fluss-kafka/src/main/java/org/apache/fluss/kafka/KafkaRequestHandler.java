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

package org.apache.fluss.kafka;

import org.apache.fluss.kafka.admin.KafkaAdminTranscoder;
import org.apache.fluss.kafka.catalog.CustomPropertiesTopicsCatalog;
import org.apache.fluss.kafka.catalog.KafkaTopicsCatalog;
import org.apache.fluss.kafka.fetch.KafkaFetchTranscoder;
import org.apache.fluss.kafka.fetch.KafkaListOffsetsTranscoder;
import org.apache.fluss.kafka.group.InMemoryOffsetStore;
import org.apache.fluss.kafka.group.KafkaGroupRegistry;
import org.apache.fluss.kafka.group.KafkaGroupTranscoder;
import org.apache.fluss.kafka.group.OffsetStore;
import org.apache.fluss.kafka.group.ZkOffsetStore;
import org.apache.fluss.kafka.metadata.KafkaMetadataBuilder;
import org.apache.fluss.kafka.produce.KafkaProduceTranscoder;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.netty.server.RequestHandler;
import org.apache.fluss.rpc.protocol.RequestType;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.requests.DeleteTopicsResponse;
import org.apache.kafka.common.requests.DescribeClusterResponse;
import org.apache.kafka.common.requests.DescribeGroupsRequest;
import org.apache.kafka.common.requests.DescribeGroupsResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.ListGroupsRequest;
import org.apache.kafka.common.requests.ListGroupsResponse;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;

/** Kafka protocol implementation for request handler. */
public class KafkaRequestHandler implements RequestHandler<KafkaRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaRequestHandler.class);

    /** Cap at v11 because Fluss does not yet implement TopicId-based Metadata (v12+). */
    private static final short MAX_METADATA_VERSION = 11;

    /** Cap at v12 because Fluss does not yet implement TopicId-based Fetch (v13+). */
    private static final short MAX_FETCH_VERSION = 12;

    /**
     * APIs the broker implements end-to-end in this phase. Other advertised APIs dispatch to {@link
     * #handleUnsupportedRequest} at request time and return {@link Errors#UNSUPPORTED_VERSION}.
     */
    private static final EnumSet<ApiKeys> IMPLEMENTED_APIS =
            EnumSet.of(
                    ApiKeys.API_VERSIONS,
                    ApiKeys.METADATA,
                    ApiKeys.DESCRIBE_CLUSTER,
                    ApiKeys.CREATE_TOPICS,
                    ApiKeys.DELETE_TOPICS,
                    ApiKeys.PRODUCE,
                    ApiKeys.INIT_PRODUCER_ID,
                    ApiKeys.FETCH,
                    ApiKeys.LIST_OFFSETS,
                    ApiKeys.FIND_COORDINATOR,
                    ApiKeys.OFFSET_COMMIT,
                    ApiKeys.OFFSET_FETCH,
                    ApiKeys.LIST_GROUPS,
                    ApiKeys.DESCRIBE_GROUPS,
                    ApiKeys.JOIN_GROUP,
                    ApiKeys.SYNC_GROUP,
                    ApiKeys.HEARTBEAT,
                    ApiKeys.LEAVE_GROUP);

    private static final java.util.concurrent.atomic.AtomicLong STUB_PRODUCER_ID =
            new java.util.concurrent.atomic.AtomicLong(1L);

    /**
     * Consumer-group offset store. Backed by ZooKeeper when available (survives tablet-server
     * restarts); falls back to in-memory when the plugin is wired against a testing gateway. Phase
     * 2E+ migrates this to a Fluss PK table {@code kafka.__consumer_offsets__}.
     */
    private final OffsetStore groupOffsets;

    /** In-memory Phase 2D group registry: membership + generation per groupId. */
    private final KafkaGroupRegistry groupRegistry = new KafkaGroupRegistry();

    /**
     * APIs we advertise in ApiVersions. Restricted to the classic set needed for a producer or
     * consumer to complete its handshake and route admin calls through METADATA rather than any
     * newer "describe" APIs (which AdminClient would otherwise prefer and then fail against our
     * stub). Advertising an API does not mean it is implemented; see {@link #IMPLEMENTED_APIS}.
     */
    private static final EnumSet<ApiKeys> ADVERTISED_APIS =
            EnumSet.of(
                    ApiKeys.PRODUCE,
                    ApiKeys.FETCH,
                    ApiKeys.LIST_OFFSETS,
                    ApiKeys.METADATA,
                    ApiKeys.OFFSET_COMMIT,
                    ApiKeys.OFFSET_FETCH,
                    ApiKeys.FIND_COORDINATOR,
                    ApiKeys.JOIN_GROUP,
                    ApiKeys.HEARTBEAT,
                    ApiKeys.LEAVE_GROUP,
                    ApiKeys.SYNC_GROUP,
                    ApiKeys.DESCRIBE_GROUPS,
                    ApiKeys.LIST_GROUPS,
                    ApiKeys.SASL_HANDSHAKE,
                    ApiKeys.API_VERSIONS,
                    ApiKeys.CREATE_TOPICS,
                    ApiKeys.DELETE_TOPICS,
                    ApiKeys.DELETE_RECORDS,
                    ApiKeys.INIT_PRODUCER_ID,
                    ApiKeys.OFFSET_FOR_LEADER_EPOCH,
                    ApiKeys.ADD_PARTITIONS_TO_TXN,
                    ApiKeys.ADD_OFFSETS_TO_TXN,
                    ApiKeys.END_TXN,
                    ApiKeys.WRITE_TXN_MARKERS,
                    ApiKeys.TXN_OFFSET_COMMIT,
                    ApiKeys.DESCRIBE_CONFIGS,
                    ApiKeys.ALTER_CONFIGS,
                    ApiKeys.SASL_AUTHENTICATE,
                    ApiKeys.CREATE_PARTITIONS,
                    ApiKeys.DELETE_GROUPS,
                    ApiKeys.OFFSET_DELETE,
                    ApiKeys.DESCRIBE_CLUSTER);

    // TODO: we may need a new abstraction between TabletService and ReplicaManager to avoid
    //  affecting Fluss protocol when supporting compatibility with Kafka.
    private final TabletServerGateway gateway;

    private final KafkaServerContext context;

    public KafkaRequestHandler(TabletServerGateway gateway, KafkaServerContext context) {
        this.gateway = gateway;
        this.context = context;
        this.groupOffsets =
                context.hasZooKeeperClient()
                        ? new ZkOffsetStore(context.zooKeeperClient())
                        : new InMemoryOffsetStore();
    }

    @Override
    public RequestType requestType() {
        return RequestType.KAFKA;
    }

    @Override
    public void processRequest(KafkaRequest request) {
        // See kafka.server.KafkaApis#handle
        switch (request.apiKey()) {
            case API_VERSIONS:
                handleApiVersionsRequest(request);
                break;
            case METADATA:
                handleMetadataRequest(request);
                break;
            case DESCRIBE_CLUSTER:
                handleDescribeClusterRequest(request);
                break;
            case CREATE_TOPICS:
                handleCreateTopicsRequest(request);
                break;
            case DELETE_TOPICS:
                handleDeleteTopicsRequest(request);
                break;
            case PRODUCE:
                handleProduceRequest(request);
                break;
            case INIT_PRODUCER_ID:
                handleInitProducerIdRequest(request);
                break;
            case FETCH:
                handleFetchRequest(request);
                break;
            case LIST_OFFSETS:
                handleListOffsetsRequest(request);
                break;
            case FIND_COORDINATOR:
                handleFindCoordinatorRequest(request);
                break;
            case OFFSET_COMMIT:
                handleOffsetCommitRequest(request);
                break;
            case OFFSET_FETCH:
                handleOffsetFetchRequest(request);
                break;
            case LIST_GROUPS:
                handleListGroupsRequest(request);
                break;
            case DESCRIBE_GROUPS:
                handleDescribeGroupsRequest(request);
                break;
            case JOIN_GROUP:
                handleJoinGroupRequest(request);
                break;
            case SYNC_GROUP:
                handleSyncGroupRequest(request);
                break;
            case HEARTBEAT:
                handleHeartbeatRequest(request);
                break;
            case LEAVE_GROUP:
                handleLeaveGroupRequest(request);
                break;
            default:
                handleUnsupportedRequest(request);
        }
    }

    private void handleUnsupportedRequest(KafkaRequest request) {
        AbstractRequest abstractRequest = request.request();
        AbstractResponse response =
                abstractRequest.getErrorResponse(Errors.UNSUPPORTED_VERSION.exception());
        request.complete(response);
    }

    void handleApiVersionsRequest(KafkaRequest request) {
        short apiVersion = request.apiVersion();
        if (!ApiKeys.API_VERSIONS.isVersionSupported(apiVersion)) {
            request.fail(Errors.UNSUPPORTED_VERSION.exception());
            return;
        }
        ApiVersionsResponseData data = new ApiVersionsResponseData();
        for (ApiKeys apiKey : ADVERTISED_APIS) {
            if (apiKey.minRequiredInterBrokerMagic > RecordBatch.CURRENT_MAGIC_VALUE) {
                continue;
            }
            short maxVersion = apiKey.latestVersion();
            if (apiKey == ApiKeys.METADATA) {
                maxVersion = (short) Math.min(maxVersion, MAX_METADATA_VERSION);
            } else if (apiKey == ApiKeys.FETCH) {
                maxVersion = (short) Math.min(maxVersion, MAX_FETCH_VERSION);
            }
            data.apiKeys()
                    .add(
                            new ApiVersionsResponseData.ApiVersion()
                                    .setApiKey(apiKey.id)
                                    .setMinVersion(apiKey.oldestVersion())
                                    .setMaxVersion(maxVersion));
        }
        request.complete(new ApiVersionsResponse(data));
    }

    /** Returns true iff the handler dispatches this API to a real implementation in Phase 1. */
    static boolean isImplemented(ApiKeys apiKey) {
        return IMPLEMENTED_APIS.contains(apiKey);
    }

    void handleMetadataRequest(KafkaRequest request) {
        if (!context.hasServerState()) {
            request.fail(
                    Errors.BROKER_NOT_AVAILABLE.exception(
                            "Kafka protocol handler not attached to a running TabletServer."));
            return;
        }
        try {
            MetadataRequest metadataRequest = request.request();
            KafkaMetadataBuilder builder =
                    new KafkaMetadataBuilder(context, newCatalog(), request.listenerName());
            MetadataResponseData data = builder.buildMetadataResponse(metadataRequest);
            request.complete(new MetadataResponse(data, request.apiVersion()));
        } catch (Throwable t) {
            LOG.error("Failed to build Kafka Metadata response", t);
            request.fail(t);
        }
    }

    void handleDescribeClusterRequest(KafkaRequest request) {
        if (!context.hasServerState()) {
            request.fail(
                    Errors.BROKER_NOT_AVAILABLE.exception(
                            "Kafka protocol handler not attached to a running TabletServer."));
            return;
        }
        try {
            KafkaMetadataBuilder builder =
                    new KafkaMetadataBuilder(context, newCatalog(), request.listenerName());
            DescribeClusterResponseData data = builder.buildDescribeClusterResponse();
            request.complete(new DescribeClusterResponse(data));
        } catch (Throwable t) {
            LOG.error("Failed to build Kafka DescribeCluster response", t);
            request.fail(t);
        }
    }

    void handleCreateTopicsRequest(KafkaRequest request) {
        if (!context.hasServerState() || !context.hasCoordinatorGateway()) {
            request.fail(
                    Errors.BROKER_NOT_AVAILABLE.exception(
                            "Kafka CreateTopics requires a tablet server with a coordinator"
                                    + " gateway; the plugin is not fully wired."));
            return;
        }
        try {
            CreateTopicsRequest req = request.request();
            KafkaAdminTranscoder transcoder = new KafkaAdminTranscoder(context, newCatalog());
            CreateTopicsResponseData data = transcoder.createTopics(req.data());
            request.complete(new CreateTopicsResponse(data));
        } catch (Throwable t) {
            LOG.error("CreateTopics handler threw", t);
            request.fail(t);
        }
    }

    void handleDeleteTopicsRequest(KafkaRequest request) {
        if (!context.hasServerState() || !context.hasCoordinatorGateway()) {
            request.fail(
                    Errors.BROKER_NOT_AVAILABLE.exception(
                            "Kafka DeleteTopics requires a tablet server with a coordinator"
                                    + " gateway; the plugin is not fully wired."));
            return;
        }
        try {
            DeleteTopicsRequest req = request.request();
            KafkaAdminTranscoder transcoder = new KafkaAdminTranscoder(context, newCatalog());
            DeleteTopicsResponseData data = transcoder.deleteTopics(req.data());
            request.complete(new DeleteTopicsResponse(data));
        } catch (Throwable t) {
            LOG.error("DeleteTopics handler threw", t);
            request.fail(t);
        }
    }

    void handleProduceRequest(KafkaRequest request) {
        if (!context.hasServerState() || !context.hasReplicaManager()) {
            request.fail(
                    Errors.BROKER_NOT_AVAILABLE.exception(
                            "Kafka Produce requires a tablet server; the plugin is not wired."));
            return;
        }
        try {
            ProduceRequest req = request.request();
            KafkaProduceTranscoder transcoder =
                    new KafkaProduceTranscoder(context, newCatalog(), context.replicaManager());
            transcoder
                    .produce(req.data())
                    .whenComplete(
                            (data, err) -> {
                                if (err != null) {
                                    LOG.error("Produce handler failed", err);
                                    request.fail(err);
                                    return;
                                }
                                request.complete(completedProduceResponse(data));
                            });
        } catch (Throwable t) {
            LOG.error("Produce handler threw", t);
            request.fail(t);
        }
    }

    private static ProduceResponse completedProduceResponse(ProduceResponseData data) {
        return new ProduceResponse(data);
    }

    void handleFindCoordinatorRequest(KafkaRequest request) {
        if (!context.hasServerState()) {
            request.fail(
                    Errors.BROKER_NOT_AVAILABLE.exception(
                            "Kafka FindCoordinator requires server state."));
            return;
        }
        try {
            FindCoordinatorRequest req = request.request();
            KafkaGroupTranscoder transcoder =
                    new KafkaGroupTranscoder(
                            context, groupOffsets, groupRegistry, request.listenerName());
            FindCoordinatorResponseData data = transcoder.findCoordinator(req.data());
            request.complete(new FindCoordinatorResponse(data));
        } catch (Throwable t) {
            LOG.error("FindCoordinator handler threw", t);
            request.fail(t);
        }
    }

    void handleOffsetCommitRequest(KafkaRequest request) {
        if (!context.hasServerState()) {
            request.fail(
                    Errors.BROKER_NOT_AVAILABLE.exception(
                            "Kafka OffsetCommit requires server state."));
            return;
        }
        try {
            OffsetCommitRequest req = request.request();
            KafkaGroupTranscoder transcoder =
                    new KafkaGroupTranscoder(
                            context, groupOffsets, groupRegistry, request.listenerName());
            OffsetCommitResponseData data = transcoder.offsetCommit(req.data());
            request.complete(new OffsetCommitResponse(data));
        } catch (Throwable t) {
            LOG.error("OffsetCommit handler threw", t);
            request.fail(t);
        }
    }

    void handleOffsetFetchRequest(KafkaRequest request) {
        if (!context.hasServerState()) {
            request.fail(
                    Errors.BROKER_NOT_AVAILABLE.exception(
                            "Kafka OffsetFetch requires server state."));
            return;
        }
        try {
            OffsetFetchRequest req = request.request();
            KafkaGroupTranscoder transcoder =
                    new KafkaGroupTranscoder(
                            context, groupOffsets, groupRegistry, request.listenerName());
            OffsetFetchResponseData data = transcoder.offsetFetch(req.data());
            request.complete(new OffsetFetchResponse(data, request.apiVersion()));
        } catch (Throwable t) {
            LOG.error("OffsetFetch handler threw", t);
            request.fail(t);
        }
    }

    void handleJoinGroupRequest(KafkaRequest request) {
        if (!context.hasServerState()) {
            request.fail(
                    Errors.BROKER_NOT_AVAILABLE.exception(
                            "Kafka JoinGroup requires server state."));
            return;
        }
        try {
            JoinGroupRequest req = request.request();
            KafkaGroupTranscoder transcoder =
                    new KafkaGroupTranscoder(
                            context, groupOffsets, groupRegistry, request.listenerName());
            JoinGroupResponseData data = transcoder.joinGroup(req.data());
            request.complete(new JoinGroupResponse(data, request.apiVersion()));
        } catch (Throwable t) {
            LOG.error("JoinGroup handler threw", t);
            request.fail(t);
        }
    }

    void handleSyncGroupRequest(KafkaRequest request) {
        if (!context.hasServerState()) {
            request.fail(
                    Errors.BROKER_NOT_AVAILABLE.exception(
                            "Kafka SyncGroup requires server state."));
            return;
        }
        try {
            SyncGroupRequest req = request.request();
            KafkaGroupTranscoder transcoder =
                    new KafkaGroupTranscoder(
                            context, groupOffsets, groupRegistry, request.listenerName());
            SyncGroupResponseData data = transcoder.syncGroup(req.data());
            request.complete(new SyncGroupResponse(data));
        } catch (Throwable t) {
            LOG.error("SyncGroup handler threw", t);
            request.fail(t);
        }
    }

    void handleHeartbeatRequest(KafkaRequest request) {
        if (!context.hasServerState()) {
            request.fail(
                    Errors.BROKER_NOT_AVAILABLE.exception(
                            "Kafka Heartbeat requires server state."));
            return;
        }
        try {
            HeartbeatRequest req = request.request();
            KafkaGroupTranscoder transcoder =
                    new KafkaGroupTranscoder(
                            context, groupOffsets, groupRegistry, request.listenerName());
            HeartbeatResponseData data = transcoder.heartbeat(req.data());
            request.complete(new HeartbeatResponse(data));
        } catch (Throwable t) {
            LOG.error("Heartbeat handler threw", t);
            request.fail(t);
        }
    }

    void handleLeaveGroupRequest(KafkaRequest request) {
        if (!context.hasServerState()) {
            request.fail(
                    Errors.BROKER_NOT_AVAILABLE.exception(
                            "Kafka LeaveGroup requires server state."));
            return;
        }
        try {
            LeaveGroupRequest req = request.request();
            KafkaGroupTranscoder transcoder =
                    new KafkaGroupTranscoder(
                            context, groupOffsets, groupRegistry, request.listenerName());
            LeaveGroupResponseData data = transcoder.leaveGroup(req.data());
            request.complete(new LeaveGroupResponse(data));
        } catch (Throwable t) {
            LOG.error("LeaveGroup handler threw", t);
            request.fail(t);
        }
    }

    void handleListGroupsRequest(KafkaRequest request) {
        if (!context.hasServerState()) {
            request.fail(
                    Errors.BROKER_NOT_AVAILABLE.exception(
                            "Kafka ListGroups requires server state."));
            return;
        }
        try {
            ListGroupsRequest req = request.request();
            KafkaGroupTranscoder transcoder =
                    new KafkaGroupTranscoder(
                            context, groupOffsets, groupRegistry, request.listenerName());
            ListGroupsResponseData data = transcoder.listGroups(req.data());
            request.complete(new ListGroupsResponse(data));
        } catch (Throwable t) {
            LOG.error("ListGroups handler threw", t);
            request.fail(t);
        }
    }

    void handleDescribeGroupsRequest(KafkaRequest request) {
        if (!context.hasServerState()) {
            request.fail(
                    Errors.BROKER_NOT_AVAILABLE.exception(
                            "Kafka DescribeGroups requires server state."));
            return;
        }
        try {
            DescribeGroupsRequest req = request.request();
            KafkaGroupTranscoder transcoder =
                    new KafkaGroupTranscoder(
                            context, groupOffsets, groupRegistry, request.listenerName());
            DescribeGroupsResponseData data = transcoder.describeGroups(req.data());
            request.complete(new DescribeGroupsResponse(data));
        } catch (Throwable t) {
            LOG.error("DescribeGroups handler threw", t);
            request.fail(t);
        }
    }

    void handleListOffsetsRequest(KafkaRequest request) {
        if (!context.hasServerState() || !context.hasReplicaManager()) {
            request.fail(
                    Errors.BROKER_NOT_AVAILABLE.exception(
                            "Kafka ListOffsets requires a tablet server; the plugin is not wired."));
            return;
        }
        try {
            ListOffsetsRequest req = request.request();
            KafkaListOffsetsTranscoder transcoder =
                    new KafkaListOffsetsTranscoder(newCatalog(), context.replicaManager());
            ListOffsetsResponseData data = transcoder.listOffsets(req.data());
            request.complete(new ListOffsetsResponse(data));
        } catch (Throwable t) {
            LOG.error("ListOffsets handler threw", t);
            request.fail(t);
        }
    }

    void handleFetchRequest(KafkaRequest request) {
        if (!context.hasServerState() || !context.hasReplicaManager()) {
            request.fail(
                    Errors.BROKER_NOT_AVAILABLE.exception(
                            "Kafka Fetch requires a tablet server; the plugin is not wired."));
            return;
        }
        try {
            FetchRequest req = request.request();
            KafkaFetchTranscoder transcoder =
                    new KafkaFetchTranscoder(context, newCatalog(), context.replicaManager());
            transcoder
                    .fetch(req.data())
                    .whenComplete(
                            (data, err) -> {
                                if (err != null) {
                                    LOG.error("Fetch handler failed", err);
                                    request.fail(err);
                                    return;
                                }
                                request.complete(new FetchResponse(data));
                            });
        } catch (Throwable t) {
            LOG.error("Fetch handler threw", t);
            request.fail(t);
        }
    }

    /**
     * Minimal INIT_PRODUCER_ID stub: mint a locally-unique producerId so the producer's idempotence
     * state machine can advance. Proper mapping to Fluss's writerId allocator lands with the
     * transactional producer work in a later phase; for Phase 2B idempotent producers simply get a
     * unique id and epoch 0.
     */
    void handleInitProducerIdRequest(KafkaRequest request) {
        long producerId = STUB_PRODUCER_ID.getAndIncrement();
        InitProducerIdResponseData data = new InitProducerIdResponseData();
        data.setErrorCode(Errors.NONE.code())
                .setProducerId(producerId)
                .setProducerEpoch((short) 0)
                .setThrottleTimeMs(0);
        request.complete(new InitProducerIdResponse(data));
    }

    /** Build the catalog for this request. Cheap - just a view over metadataManager. */
    private KafkaTopicsCatalog newCatalog() {
        return new CustomPropertiesTopicsCatalog(
                context.metadataManager(), context.kafkaDatabase());
    }
}
