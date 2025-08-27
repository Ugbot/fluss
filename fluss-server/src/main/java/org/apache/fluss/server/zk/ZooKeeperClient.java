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

package org.apache.fluss.server.zk;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.security.acl.ResourceType;
import org.apache.fluss.server.authorizer.DefaultAuthorizer.VersionedAcls;
import org.apache.fluss.server.entity.RegisterTableBucketLeadAndIsrInfo;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.PartitionMetadata;
import org.apache.fluss.server.zk.ZkAsyncRequest.ZkGetChildrenRequest;
import org.apache.fluss.server.zk.ZkAsyncRequest.ZkGetDataRequest;
import org.apache.fluss.server.zk.ZkAsyncResponse.ZkGetChildrenResponse;
import org.apache.fluss.server.zk.ZkAsyncResponse.ZkGetDataResponse;
import org.apache.fluss.server.zk.ZooKeeperMetadataUtils.AssignmentInfo;
import org.apache.fluss.server.zk.ZooKeeperMetadataUtils.PartitionPathInfo;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.DatabaseRegistration;
import org.apache.fluss.server.zk.data.LakeTableSnapshot;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.RemoteLogManifestHandle;
import org.apache.fluss.server.zk.data.ResourceAcl;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.ZkData.AclChangeNotificationNode;
import org.apache.fluss.server.zk.data.ZkData.BucketIdsZNode;
import org.apache.fluss.server.zk.data.ZkData.BucketRemoteLogsZNode;
import org.apache.fluss.server.zk.data.ZkData.BucketSnapshotIdZNode;
import org.apache.fluss.server.zk.data.ZkData.BucketSnapshotsZNode;
import org.apache.fluss.server.zk.data.ZkData.CoordinatorZNode;
import org.apache.fluss.server.zk.data.ZkData.DatabaseZNode;
import org.apache.fluss.server.zk.data.ZkData.DatabasesZNode;
import org.apache.fluss.server.zk.data.ZkData.LakeTableZNode;
import org.apache.fluss.server.zk.data.ZkData.LeaderAndIsrZNode;
import org.apache.fluss.server.zk.data.ZkData.PartitionIdZNode;
import org.apache.fluss.server.zk.data.ZkData.PartitionSequenceIdZNode;
import org.apache.fluss.server.zk.data.ZkData.PartitionZNode;
import org.apache.fluss.server.zk.data.ZkData.PartitionsZNode;
import org.apache.fluss.server.zk.data.ZkData.ResourceAclNode;
import org.apache.fluss.server.zk.data.ZkData.SchemaZNode;
import org.apache.fluss.server.zk.data.ZkData.SchemasZNode;
import org.apache.fluss.server.zk.data.ZkData.ServerIdZNode;
import org.apache.fluss.server.zk.data.ZkData.ServerIdsZNode;
import org.apache.fluss.server.zk.data.ZkData.TableIdZNode;
import org.apache.fluss.server.zk.data.ZkData.TableSequenceIdZNode;
import org.apache.fluss.server.zk.data.ZkData.TableZNode;
import org.apache.fluss.server.zk.data.ZkData.TablesZNode;
import org.apache.fluss.server.zk.data.ZkData.WriterIdZNode;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.api.BackgroundCallback;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.api.CuratorEvent;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.CreateMode;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.data.Stat;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.concurrent.FutureUtils;
import org.apache.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.fluss.metadata.ResolvedPartitionSpec.fromPartitionName;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * This class includes methods for write/read various metadata (leader address, tablet server
 * registration, table assignment, table, schema) in Zookeeper.
 */
@Internal
public class ZooKeeperClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperClient.class);
    public static final int UNKNOWN_VERSION = -2;
    private static final int MAX_BATCH_SIZE = 1024;

    private final CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper;

    private final CuratorFramework zkClient;
    private final ZkSequenceIDCounter tableIdCounter;
    private final ZkSequenceIDCounter partitionIdCounter;
    private final ZkSequenceIDCounter writerIdCounter;

    private final Semaphore inFlightRequests;

    public ZooKeeperClient(
            CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper,
            Configuration configuration) {
        this.curatorFrameworkWrapper = curatorFrameworkWrapper;
        this.zkClient = curatorFrameworkWrapper.asCuratorFramework();
        this.tableIdCounter = new ZkSequenceIDCounter(zkClient, TableSequenceIdZNode.path());
        this.partitionIdCounter =
                new ZkSequenceIDCounter(zkClient, PartitionSequenceIdZNode.path());
        this.writerIdCounter = new ZkSequenceIDCounter(zkClient, WriterIdZNode.path());

        int maxInFlightRequests =
                configuration.getInt(ConfigOptions.ZOOKEEPER_MAX_INFLIGHT_REQUESTS);
        this.inFlightRequests = new Semaphore(maxInFlightRequests);
    }

    public Optional<byte[]> getOrEmpty(String path) throws Exception {
        try {
            return Optional.of(zkClient.getData().forPath(path));
        } catch (KeeperException.NoNodeException e) {
            return Optional.empty();
        }
    }

    /**
     * Send a pipelined sequence of requests and return a CompletableFuture for all their responses.
     *
     * <p>The watch flag on each outgoing request will be set if we've already registered a handler
     * for the path associated with the request.
     *
     * @param requests a sequence of requests to send
     * @param respCreator function to create response objects from curator events
     * @return CompletableFuture containing the responses for the requests
     */
    private <Resp extends ZkAsyncResponse, Req extends ZkAsyncRequest>
            CompletableFuture<List<Resp>> handleRequestInBackgroundAsync(
                    List<Req> requests, Function<CuratorEvent, Resp> respCreator) {
        if (requests == null || requests.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        CompletableFuture<List<Resp>> future = new CompletableFuture<>();
        BackgroundCallback callback = getZkBackgroundCallback(requests, respCreator, future);

        try {
            for (Req request : requests) {
                try {
                    inFlightRequests.acquire();
                    if (request instanceof ZkGetDataRequest) {
                        zkClient.getData().inBackground(callback).forPath(request.getPath());

                    } else if (request instanceof ZkGetChildrenRequest) {
                        zkClient.getChildren().inBackground(callback).forPath(request.getPath());

                    } else {
                        inFlightRequests.release();
                        throw new IllegalArgumentException(
                                "Unsupported request type: " + request.getClass());
                    }
                } catch (Exception e) {
                    inFlightRequests.release();
                    throw e;
                }
            }
        } catch (Exception e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    @Nonnull
    private <Resp extends ZkAsyncResponse, Req extends ZkAsyncRequest>
            BackgroundCallback getZkBackgroundCallback(
                    List<Req> requests,
                    Function<CuratorEvent, Resp> respCreator,
                    CompletableFuture<List<Resp>> future) {
        ArrayBlockingQueue<Resp> responseQueue = new ArrayBlockingQueue<>(requests.size());
        CountDownLatch countDownLatch = new CountDownLatch(requests.size());

        // Complete future when all responses received
        return (client, event) -> {
            try {
                Resp response = respCreator.apply(event);
                responseQueue.add(response);
                inFlightRequests.release();
                countDownLatch.countDown();

                // Complete future when all responses received
                if (countDownLatch.getCount() == 0) {
                    future.complete(new ArrayList<>(responseQueue));
                }
            } catch (Exception e) {
                inFlightRequests.release();
                future.completeExceptionally(e);
            }
        };
    }

    /**
     * Send a pipelined sequence of requests and wait for all of their responses synchronously in
     * background.
     *
     * <p>The watch flag on each outgoing request will be set if we've already registered a handler
     * for the path associated with the request.
     *
     * @param requests a sequence of requests to send and wait on.
     * @return the responses for the requests. If all requests have the same type, the responses
     *     will have the respective response type.
     */
    private <Resp extends ZkAsyncResponse, Req extends ZkAsyncRequest>
            List<Resp> handleRequestInBackground(
                    List<Req> requests, Function<CuratorEvent, Resp> respCreator) throws Exception {
        try {
            return handleRequestInBackgroundAsync(requests, respCreator).get();
        } catch (ExecutionException e) {
            Throwable cause = ExceptionUtils.stripExecutionException(e);
            if (cause instanceof Exception) {
                throw (Exception) cause;
            } else {
                throw new Exception("Async request handling failed", cause);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new Exception("Request handling was interrupted", e);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Coordinator server
    // --------------------------------------------------------------------------------------------

    /** Register a coordinator leader server to ZK. */
    public void registerCoordinatorLeader(CoordinatorAddress coordinatorAddress) throws Exception {
        String path = CoordinatorZNode.path();
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(path, CoordinatorZNode.encode(coordinatorAddress));
        LOG.info("Registered leader {} at path {}.", coordinatorAddress, path);
    }

    /** Get the leader address registered in ZK. */
    public Optional<CoordinatorAddress> getCoordinatorAddress() throws Exception {
        Optional<byte[]> bytes = getOrEmpty(CoordinatorZNode.path());
        return bytes.map(CoordinatorZNode::decode);
    }

    // --------------------------------------------------------------------------------------------
    // Tablet server
    // --------------------------------------------------------------------------------------------

    /** Register a tablet server to ZK. */
    public void registerTabletServer(
            int tabletServerId, TabletServerRegistration tabletServerRegistration)
            throws Exception {
        String path = ServerIdZNode.path(tabletServerId);
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(path, ServerIdZNode.encode(tabletServerRegistration));
        LOG.info(
                "Registered tablet server {} at path {} with registration {}.",
                tabletServerId,
                path,
                tabletServerRegistration);
    }

    /** Get the tablet server registered in ZK. */
    public Optional<TabletServerRegistration> getTabletServer(int tabletServerId) throws Exception {
        Optional<byte[]> bytes = getOrEmpty(ServerIdZNode.path(tabletServerId));
        return bytes.map(ServerIdZNode::decode);
    }

    /** Get the tablet servers registered in ZK. */
    public Map<Integer, TabletServerRegistration> getTabletServers(int[] tabletServerIds)
            throws Exception {
        Map<String, Integer> path2IdMap =
                Arrays.stream(tabletServerIds)
                        .boxed()
                        .collect(Collectors.toMap(ServerIdZNode::path, id -> id));

        List<ZkGetDataResponse> responses = getDataInBackground(path2IdMap.keySet());
        return ZooKeeperMetadataUtils.processMultipleDataResponses(
                responses,
                response -> path2IdMap.get(response.getPath()),
                ServerIdZNode::decode,
                "tablet server registration");
    }

    /** Gets the list of sorted server Ids. */
    public int[] getSortedTabletServerList() throws Exception {
        List<String> tabletServers = getChildren(ServerIdsZNode.path());
        return tabletServers.stream().mapToInt(Integer::parseInt).sorted().toArray();
    }

    // --------------------------------------------------------------------------------------------
    // Tablet assignments
    // --------------------------------------------------------------------------------------------

    /** Register table assignment to ZK. */
    public void registerTableAssignment(long tableId, TableAssignment tableAssignment)
            throws Exception {
        String path = TableIdZNode.path(tableId);
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(path, TableIdZNode.encode(tableAssignment));
        LOG.info("Registered table assignment {} for table id {}.", tableAssignment, tableId);
    }

    /** Get the table assignment in ZK. */
    public Optional<TableAssignment> getTableAssignment(long tableId) throws Exception {
        return FutureUtils.asyncToSync(() -> getTableAssignmentAsync(tableId), "table assignment");
    }

    /** Get the table assignment in ZK asynchronously. */
    public CompletableFuture<TableAssignment> getTableAssignmentAsync(long tableId) {
        String path = TableIdZNode.path(tableId);

        return getDataInBackgroundAsync(Collections.singleton(path))
                .thenApply(
                        responses ->
                                ZooKeeperMetadataUtils.processSingleDataResponse(
                                        responses,
                                        data -> data.length == 0 ? null : TableIdZNode.decode(data),
                                        path));
    }

    /** Get the tables assignments in ZK. */
    public Map<Long, TableAssignment> getTablesAssignments(List<Long> tableIds) throws Exception {
        Map<String, Long> path2TableIdMap =
                tableIds.stream().collect(Collectors.toMap(TableIdZNode::path, id -> id));

        List<ZkGetDataResponse> responses = getDataInBackground(path2TableIdMap.keySet());
        return ZooKeeperMetadataUtils.processMultipleDataResponsesWithCondition(
                responses,
                response -> path2TableIdMap.get(response.getPath()),
                TableIdZNode::decode,
                response -> response.getData().length > 0,
                "table assignment");
    }

    /** Get the partition assignment in ZK. */
    public Optional<PartitionAssignment> getPartitionAssignment(long partitionId) throws Exception {
        return FutureUtils.asyncToSync(
                () -> getPartitionAssignmentAsync(partitionId),
                assignment ->
                        (PartitionAssignment)
                                assignment, // PartitionAssignment extends TableAssignment
                "partition assignment");
    }

    /** Get the partition assignment in ZK asynchronously. */
    private CompletableFuture<TableAssignment> getPartitionAssignmentAsync(long partitionId) {
        String path = PartitionIdZNode.path(partitionId);

        return getDataInBackgroundAsync(Collections.singleton(path))
                .thenApply(
                        responses ->
                                ZooKeeperMetadataUtils.processSingleDataResponse(
                                        responses,
                                        PartitionIdZNode::decode, // PartitionAssignment extends
                                        // TableAssignment
                                        path));
    }

    /** Get the partitions assignments in ZK. */
    public Map<Long, PartitionAssignment> getPartitionsAssignments(List<Long> partitionIds)
            throws Exception {
        Map<String, Long> path2PartitionIdMap =
                partitionIds.stream().collect(Collectors.toMap(PartitionIdZNode::path, id -> id));

        List<ZkGetDataResponse> responses = getDataInBackground(path2PartitionIdMap.keySet());
        return ZooKeeperMetadataUtils.processMultipleDataResponses(
                responses,
                response -> path2PartitionIdMap.get(response.getPath()),
                PartitionIdZNode::decode,
                "partition assignment");
    }

    public void updateTableAssignment(long tableId, TableAssignment tableAssignment)
            throws Exception {
        String path = TableIdZNode.path(tableId);
        zkClient.setData().forPath(path, TableIdZNode.encode(tableAssignment));
        LOG.info("Updated table assignment {} for table id {}.", tableAssignment, tableId);
    }

    public void deleteTableAssignment(long tableId) throws Exception {
        String path = TableIdZNode.path(tableId);
        zkClient.delete().deletingChildrenIfNeeded().forPath(path);
        LOG.info("Deleted table assignment for table id {}.", tableId);
    }

    public void deletePartitionAssignment(long partitionId) throws Exception {
        String path = PartitionIdZNode.path(partitionId);
        zkClient.delete().deletingChildrenIfNeeded().forPath(path);
        LOG.info("Deleted table assignment for partition id {}.", partitionId);
    }

    // --------------------------------------------------------------------------------------------
    // Table state
    // --------------------------------------------------------------------------------------------

    /** Register bucket LeaderAndIsr to ZK. */
    public void registerLeaderAndIsr(TableBucket tableBucket, LeaderAndIsr leaderAndIsr)
            throws Exception {
        String path = LeaderAndIsrZNode.path(tableBucket);
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(path, LeaderAndIsrZNode.encode(leaderAndIsr));
        LOG.info("Registered {} for bucket {} in Zookeeper.", leaderAndIsr, tableBucket);
    }

    public void batchRegisterLeaderAndIsrForTablePartition(
            List<RegisterTableBucketLeadAndIsrInfo> registerList) throws Exception {
        if (registerList.isEmpty()) {
            return;
        }

        List<CuratorOp> ops = new ArrayList<>(registerList.size());
        // In transaction API, it is not allowed to use "creatingParentsIfNeeded()"
        // So we have to create parent dictionary in advance.
        RegisterTableBucketLeadAndIsrInfo firstInfo = registerList.get(0);
        String bucketsParentPath = BucketIdsZNode.path(firstInfo.getTableBucket());
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(bucketsParentPath);

        for (RegisterTableBucketLeadAndIsrInfo info : registerList) {
            LOG.info(
                    "Batch Register {} for bucket {} in Zookeeper.",
                    info.getLeaderAndIsr(),
                    info.getTableBucket());
            byte[] data = LeaderAndIsrZNode.encode(info.getLeaderAndIsr());
            // create direct parent node
            CuratorOp parentNodeCreate =
                    zkClient.transactionOp()
                            .create()
                            .withMode(CreateMode.PERSISTENT)
                            .forPath(ZkData.BucketIdZNode.path(info.getTableBucket()));
            // create current node
            CuratorOp currentNodeCreate =
                    zkClient.transactionOp()
                            .create()
                            .withMode(CreateMode.PERSISTENT)
                            .forPath(LeaderAndIsrZNode.path(info.getTableBucket()), data);
            ops.add(parentNodeCreate);
            ops.add(currentNodeCreate);
            if (ops.size() == MAX_BATCH_SIZE) {
                zkClient.transaction().forOperations(ops);
                ops.clear();
            }
        }
        if (!ops.isEmpty()) {
            zkClient.transaction().forOperations(ops);
        }
        LOG.info(
                "Batch registered leadAndIsr for tableId: {}, partitionId: {}, partitionName: {}  in Zookeeper.",
                firstInfo.getTableBucket().getTableId(),
                firstInfo.getTableBucket().getPartitionId(),
                firstInfo.getPartitionName());
    }

    /** Get the bucket LeaderAndIsr in ZK. */
    public Optional<LeaderAndIsr> getLeaderAndIsr(TableBucket tableBucket) throws Exception {
        Optional<byte[]> bytes = getOrEmpty(LeaderAndIsrZNode.path(tableBucket));
        return bytes.map(LeaderAndIsrZNode::decode);
    }

    /** Get the buckets LeaderAndIsr in ZK asynchronously. */
    private CompletableFuture<Map<TableBucket, Optional<LeaderAndIsr>>> getLeaderAndIsrsAsync(
            Collection<TableBucket> tableBuckets) {
        if (tableBuckets.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }

        Map<String, TableBucket> path2TableBucketMap =
                tableBuckets.stream()
                        .collect(Collectors.toMap(LeaderAndIsrZNode::path, bucket -> bucket));

        return getDataInBackgroundAsync(path2TableBucketMap.keySet())
                .thenApply(
                        responses ->
                                ZooKeeperMetadataUtils.processMultipleDataResponsesWithOptional(
                                        responses,
                                        response -> path2TableBucketMap.get(response.getPath()),
                                        LeaderAndIsrZNode::decode,
                                        "leader and isr"));
    }

    /** Get the buckets LeaderAndIsr in ZK. */
    public Map<TableBucket, LeaderAndIsr> getLeaderAndIsrs(Collection<TableBucket> tableBuckets)
            throws Exception {
        Map<String, TableBucket> path2TableBucketMap =
                tableBuckets.stream()
                        .collect(Collectors.toMap(LeaderAndIsrZNode::path, bucket -> bucket));

        List<ZkGetDataResponse> responses = getDataInBackground(path2TableBucketMap.keySet());
        return ZooKeeperMetadataUtils.processMultipleDataResponses(
                responses,
                response -> path2TableBucketMap.get(response.getPath()),
                LeaderAndIsrZNode::decode,
                "leader and isr");
    }

    public void updateLeaderAndIsr(TableBucket tableBucket, LeaderAndIsr leaderAndIsr)
            throws Exception {
        String path = LeaderAndIsrZNode.path(tableBucket);
        zkClient.setData().forPath(path, LeaderAndIsrZNode.encode(leaderAndIsr));
        LOG.info("Updated {} for bucket {} in Zookeeper.", leaderAndIsr, tableBucket);
    }

    public void batchUpdateLeaderAndIsr(Map<TableBucket, LeaderAndIsr> leaderAndIsrList)
            throws Exception {
        if (leaderAndIsrList.isEmpty()) {
            return;
        }

        List<CuratorOp> ops = new ArrayList<>(leaderAndIsrList.size());
        for (Map.Entry<TableBucket, LeaderAndIsr> entry : leaderAndIsrList.entrySet()) {
            TableBucket tableBucket = entry.getKey();
            LeaderAndIsr leaderAndIsr = entry.getValue();

            LOG.info("Batch Update {} for bucket {} in Zookeeper.", leaderAndIsr, tableBucket);
            String path = LeaderAndIsrZNode.path(tableBucket);
            byte[] data = LeaderAndIsrZNode.encode(leaderAndIsr);
            CuratorOp updateOp = zkClient.transactionOp().setData().forPath(path, data);
            ops.add(updateOp);
            if (ops.size() == MAX_BATCH_SIZE) {
                zkClient.transaction().forOperations(ops);
                ops.clear();
            }
        }
        if (!ops.isEmpty()) {
            zkClient.transaction().forOperations(ops);
        }
    }

    public void deleteLeaderAndIsr(TableBucket tableBucket) throws Exception {
        String path = LeaderAndIsrZNode.path(tableBucket);
        zkClient.delete().forPath(path);
        LOG.info("Deleted LeaderAndIsr for bucket {} in Zookeeper.", tableBucket);
    }

    // --------------------------------------------------------------------------------------------
    // Database
    // --------------------------------------------------------------------------------------------
    public void registerDatabase(String database, DatabaseRegistration databaseRegistration)
            throws Exception {
        String path = DatabaseZNode.path(database);
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(path, DatabaseZNode.encode(databaseRegistration));
        LOG.info("Registered database {}", database);
    }

    /** Get the database in ZK. */
    public Optional<DatabaseRegistration> getDatabase(String database) throws Exception {
        String path = DatabaseZNode.path(database);
        Optional<byte[]> bytes = getOrEmpty(path);
        return bytes.map(DatabaseZNode::decode);
    }

    public void deleteDatabase(String database) throws Exception {
        String path = DatabaseZNode.path(database);
        zkClient.delete().deletingChildrenIfNeeded().forPath(path);
    }

    public boolean databaseExists(String database) throws Exception {
        String path = DatabaseZNode.path(database);
        return zkClient.checkExists().forPath(path) != null;
    }

    public List<String> listDatabases() throws Exception {
        return getChildren(DatabasesZNode.path());
    }

    public List<String> listTables(String databaseName) throws Exception {
        return getChildren(TablesZNode.path(databaseName));
    }

    // --------------------------------------------------------------------------------------------
    // Table
    // --------------------------------------------------------------------------------------------

    /** generate a table id . */
    public long getTableIdAndIncrement() throws Exception {
        return tableIdCounter.getAndIncrement();
    }

    public long getPartitionIdAndIncrement() throws Exception {
        return partitionIdCounter.getAndIncrement();
    }

    /** Register table to ZK metadata. */
    public void registerTable(TablePath tablePath, TableRegistration tableRegistration)
            throws Exception {
        registerTable(tablePath, tableRegistration, true);
    }

    /**
     * Register table to ZK metadata.
     *
     * @param needCreateNode when register a table to zk, whether need to create the node of the
     *     path. In the case that we first register the schema to a path which will be the children
     *     path of the path to store the table, then register the table, we won't need to create the
     *     node again.
     */
    public void registerTable(
            TablePath tablePath, TableRegistration tableRegistration, boolean needCreateNode)
            throws Exception {
        String path = TableZNode.path(tablePath);
        byte[] tableBytes = TableZNode.encode(tableRegistration);
        if (needCreateNode) {
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path, tableBytes);
        } else {
            zkClient.setData().forPath(path, tableBytes);
        }
        LOG.info(
                "Registered table {} for database {}",
                tablePath.getTableName(),
                tablePath.getDatabaseName());
    }

    /** Get the table in ZK. */
    public Optional<TableRegistration> getTable(TablePath tablePath) throws Exception {
        Optional<byte[]> bytes = getOrEmpty(TableZNode.path(tablePath));
        return bytes.map(TableZNode::decode);
    }

    /** Get the tables in ZK. */
    public Map<TablePath, TableRegistration> getTables(Collection<TablePath> tablePaths)
            throws Exception {
        return FutureUtils.asyncToSyncDirect(() -> getTablesAsync(tablePaths), "tables");
    }

    /** Get the tables in ZK asynchronously. */
    public CompletableFuture<Map<TablePath, TableRegistration>> getTablesAsync(
            Collection<TablePath> tablePaths) {
        Map<String, TablePath> path2TablePathMap =
                tablePaths.stream().collect(Collectors.toMap(TableZNode::path, path -> path));

        return getDataInBackgroundAsync(path2TablePathMap.keySet())
                .thenApply(
                        responses ->
                                ZooKeeperMetadataUtils.processMultipleDataResponses(
                                        responses,
                                        response -> path2TablePathMap.get(response.getPath()),
                                        TableZNode::decode,
                                        "table registration"));
    }

    /** Update the table in ZK. */
    public void updateTable(TablePath tablePath, TableRegistration tableRegistration)
            throws Exception {
        String path = TableZNode.path(tablePath);
        zkClient.setData().forPath(path, TableZNode.encode(tableRegistration));
        LOG.info(
                "Updated table {} for database {}",
                tablePath.getTableName(),
                tablePath.getDatabaseName());
    }

    /** Delete the table in ZK. */
    public void deleteTable(TablePath tablePath) throws Exception {
        String path = TableZNode.path(tablePath);
        zkClient.delete().deletingChildrenIfNeeded().forPath(path);
        LOG.info("Deleted table {}.", tablePath);
    }

    public boolean tableExist(TablePath tablePath) throws Exception {
        String path = TableZNode.path(tablePath);
        Stat stat = zkClient.checkExists().forPath(path);
        // when we create a table, we will first create a node with
        // path 'table_path/schemas/schema_id' to store the schema, so we can't use the path of
        // table 'table_path' exist or not to check the table exist or not.
        return stat != null && stat.getDataLength() > 0;
    }

    /** Get the partitions of a table in ZK. */
    public Set<String> getPartitions(TablePath tablePath) throws Exception {
        String path = PartitionsZNode.path(tablePath);
        return new HashSet<>(getChildren(path));
    }

    /** Get the partitions of tables in ZK. */
    public Map<TablePath, List<String>> getPartitionsForTables(List<TablePath> tablePaths)
            throws Exception {
        Map<String, TablePath> path2TablePathMap =
                tablePaths.stream().collect(Collectors.toMap(PartitionsZNode::path, path -> path));

        List<ZkGetChildrenResponse> responses = getChildrenInBackground(path2TablePathMap.keySet());
        return ZooKeeperMetadataUtils.processMultipleChildrenResponses(
                responses,
                response -> path2TablePathMap.get(response.getPath()),
                "partitions for tables");
    }

    /** Get the partition and the id for the partitions of a table in ZK. */
    public Map<String, Long> getPartitionNameAndIds(TablePath tablePath) throws Exception {
        Map<String, Long> partitions = new HashMap<>();
        for (String partitionName : getPartitions(tablePath)) {
            Optional<TablePartition> optPartition = getPartition(tablePath, partitionName);
            optPartition.ifPresent(
                    partition -> partitions.put(partitionName, partition.getPartitionId()));
        }
        return partitions;
    }

    /** Get the partition and the id for the partitions of tables in ZK. */
    public Map<TablePath, Map<String, Long>> getPartitionNameAndIdsForTables(
            List<TablePath> tablePaths) throws Exception {
        Map<TablePath, Map<String, Long>> result = new HashMap<>();

        Map<TablePath, List<String>> tablePath2Partitions = getPartitionsForTables(tablePaths);

        // each TablePath has a list of partitions
        Map<String, TablePath> zkPath2TablePath = new HashMap<>();
        Map<String, String> zkPath2PartitionName = new HashMap<>();
        for (Map.Entry<TablePath, List<String>> entry : tablePath2Partitions.entrySet()) {
            TablePath tablePath = entry.getKey();
            List<String> partitions = entry.getValue();
            for (String partitionName : partitions) {
                zkPath2TablePath.put(PartitionZNode.path(tablePath, partitionName), tablePath);
                zkPath2PartitionName.put(
                        PartitionZNode.path(tablePath, partitionName), partitionName);
            }
        }

        List<ZkGetDataResponse> responses = getDataInBackground(zkPath2TablePath.keySet());
        for (ZkGetDataResponse response : responses) {
            if (response.getResultCode() == KeeperException.Code.OK) {
                String zkPath = response.getPath();
                TablePath tablePath = zkPath2TablePath.get(zkPath);
                String partitionName = zkPath2PartitionName.get(zkPath);
                long partitionId = PartitionZNode.decode(response.getData()).getPartitionId();
                result.computeIfAbsent(tablePath, k -> new HashMap<>())
                        .put(partitionName, partitionId);
            } else {
                LOG.warn(
                        "Failed to get data for path {}: {}",
                        response.getPath(),
                        response.getResultCode());
            }
        }
        return result;
    }

    /** Get the partition and the id for the partitions of a table in ZK by partition spec. */
    public Map<String, Long> getPartitionNameAndIds(
            TablePath tablePath,
            List<String> partitionKeys,
            ResolvedPartitionSpec partialPartitionSpec)
            throws Exception {
        Map<String, Long> partitions = new HashMap<>();

        for (String partitionName : getPartitions(tablePath)) {
            ResolvedPartitionSpec resolvedPartitionSpec =
                    fromPartitionName(partitionKeys, partitionName);
            boolean contains = resolvedPartitionSpec.contains(partialPartitionSpec);
            if (contains) {
                Optional<TablePartition> optPartition = getPartition(tablePath, partitionName);
                optPartition.ifPresent(
                        partition -> partitions.put(partitionName, partition.getPartitionId()));
            }
        }

        return partitions;
    }

    /** Get the id and name for the partitions of a table in ZK. */
    public Map<Long, String> getPartitionIdAndNames(TablePath tablePath) throws Exception {
        Map<Long, String> partitionIdAndNames = new HashMap<>();
        for (String partitionName : getPartitions(tablePath)) {
            Optional<TablePartition> optPartition = getPartition(tablePath, partitionName);
            optPartition.ifPresent(
                    partition ->
                            partitionIdAndNames.put(partition.getPartitionId(), partitionName));
        }
        return partitionIdAndNames;
    }

    /** Get a partition of a table in ZK. */
    public Optional<TablePartition> getPartition(TablePath tablePath, String partitionName)
            throws Exception {
        return FutureUtils.asyncToSync(
                () -> getPartitionAsync(tablePath, partitionName), "partition");
    }

    /** Get a partition of a table in ZK asynchronously. */
    public CompletableFuture<TablePartition> getPartitionAsync(
            TablePath tablePath, String partitionName) {
        String path = PartitionZNode.path(tablePath, partitionName);

        return getDataInBackgroundAsync(Collections.singleton(path))
                .thenApply(
                        responses ->
                                ZooKeeperMetadataUtils.processSingleDataResponse(
                                        responses, PartitionZNode::decode, path));
    }

    /** Get partition num of a table in ZK. */
    public int getPartitionNumber(TablePath tablePath) throws Exception {
        String path = PartitionsZNode.path(tablePath);
        Stat stat = zkClient.checkExists().forPath(path);
        if (stat == null) {
            return 0;
        }
        return stat.getNumChildren();
    }

    /** Delete a partition for a table in ZK. */
    public void deletePartition(TablePath tablePath, String partitionName) throws Exception {
        String path = PartitionZNode.path(tablePath, partitionName);
        zkClient.delete().forPath(path);
    }

    /** Register partition assignment and metadata in transaction. */
    public void registerPartitionAssignmentAndMetadata(
            long partitionId,
            String partitionName,
            PartitionAssignment partitionAssignment,
            TablePath tablePath,
            long tableId)
            throws Exception {
        // Merge "registerPartitionAssignment()" and "registerPartition()"
        // into one transaction. This is to avoid the case that the partition assignment is
        // registered
        // but the partition metadata is not registered.

        // Create parent dictionary in advance.
        try {
            String tabletServerPartitionParentPath = ZkData.PartitionIdsZNode.path();
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(tabletServerPartitionParentPath);
        } catch (KeeperException.NodeExistsException e) {
            // ignore
        }
        try {
            String metadataPartitionParentPath = PartitionsZNode.path(tablePath);
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(metadataPartitionParentPath);
        } catch (KeeperException.NodeExistsException e) {
            // ignore
        }

        List<CuratorOp> ops = new ArrayList<>(2);
        String tabletServerPartitionPath = PartitionIdZNode.path(partitionId);
        CuratorOp tabletServerPartitionNode =
                zkClient.transactionOp()
                        .create()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(
                                tabletServerPartitionPath,
                                PartitionIdZNode.encode(partitionAssignment));

        String metadataPath = PartitionZNode.path(tablePath, partitionName);
        CuratorOp metadataPartitionNode =
                zkClient.transactionOp()
                        .create()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(
                                metadataPath,
                                PartitionZNode.encode(new TablePartition(tableId, partitionId)));

        ops.add(tabletServerPartitionNode);
        ops.add(metadataPartitionNode);
        zkClient.transaction().forOperations(ops);
    }
    // --------------------------------------------------------------------------------------------
    // Schema
    // --------------------------------------------------------------------------------------------

    /** Register schema to ZK metadata and return the schema id. */
    public int registerSchema(TablePath tablePath, Schema schema) throws Exception {
        int currentSchemaId = getCurrentSchemaId(tablePath);
        // increase schema id.
        currentSchemaId++;
        String path = SchemaZNode.path(tablePath, currentSchemaId);
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(path, SchemaZNode.encode(schema));
        LOG.info("Registered new schema version {} for table {}.", currentSchemaId, tablePath);
        return currentSchemaId;
    }

    /** Get the specific schema by schema id in ZK metadata. */
    public Optional<SchemaInfo> getSchemaById(TablePath tablePath, int schemaId) throws Exception {
        Optional<byte[]> bytes = getOrEmpty(SchemaZNode.path(tablePath, schemaId));
        return bytes.map(b -> new SchemaInfo(SchemaZNode.decode(b), schemaId));
    }

    /** Gets the current schema id of the given table in ZK metadata. */
    public int getCurrentSchemaId(TablePath tablePath) throws Exception {
        Optional<Integer> currentSchemaId =
                getChildren(SchemasZNode.path(tablePath)).stream()
                        .map(Integer::parseInt)
                        .reduce(Math::max);
        return currentSchemaId.orElse(0);
    }

    // --------------------------------------------------------------------------------------------
    // Table Bucket snapshot
    // --------------------------------------------------------------------------------------------
    public void registerTableBucketSnapshot(TableBucket tableBucket, BucketSnapshot snapshot)
            throws Exception {
        String path = BucketSnapshotIdZNode.path(tableBucket, snapshot.getSnapshotId());
        zkClient.create()
                .creatingParentsIfNeeded()
                .forPath(path, BucketSnapshotIdZNode.encode(snapshot));
    }

    public void deleteTableBucketSnapshot(TableBucket tableBucket, long snapshotId)
            throws Exception {
        String path = BucketSnapshotIdZNode.path(tableBucket, snapshotId);
        zkClient.delete().forPath(path);
    }

    public OptionalLong getTableBucketLatestSnapshotId(TableBucket tableBucket) throws Exception {
        String path = BucketSnapshotsZNode.path(tableBucket);
        return getChildren(path).stream().mapToLong(Long::parseLong).max();
    }

    public Optional<BucketSnapshot> getTableBucketSnapshot(TableBucket tableBucket, long snapshotId)
            throws Exception {
        String path = BucketSnapshotIdZNode.path(tableBucket, snapshotId);
        return getOrEmpty(path).map(BucketSnapshotIdZNode::decode);
    }

    /** Get the latest snapshot of the table bucket. */
    public Optional<BucketSnapshot> getTableBucketLatestSnapshot(TableBucket tableBucket)
            throws Exception {
        OptionalLong latestSnapshotId = getTableBucketLatestSnapshotId(tableBucket);
        if (latestSnapshotId.isPresent()) {
            return getTableBucketSnapshot(tableBucket, latestSnapshotId.getAsLong());
        } else {
            return Optional.empty();
        }
    }

    public List<Tuple2<BucketSnapshot, Long>> getTableBucketAllSnapshotAndIds(
            TableBucket tableBucket) throws Exception {
        String path = BucketSnapshotsZNode.path(tableBucket);
        List<Tuple2<BucketSnapshot, Long>> snapshotAndIds = new ArrayList<>();
        for (String snapshotId : getChildren(path)) {
            long snapshotIdLong = Long.parseLong(snapshotId);
            Optional<BucketSnapshot> optionalTableBucketSnapshot =
                    getTableBucketSnapshot(tableBucket, snapshotIdLong);
            optionalTableBucketSnapshot.ifPresent(
                    snapshot -> snapshotAndIds.add(Tuple2.of(snapshot, snapshotIdLong)));
        }
        return snapshotAndIds;
    }

    /**
     * Get all the latest snapshot for the buckets of the table. If no any buckets found for the
     * table in zk, return empty. The key of the map is the bucket id, the value is the optional
     * latest snapshot, empty if there is no snapshot for the kv bucket.
     */
    public Map<Integer, Optional<BucketSnapshot>> getTableLatestBucketSnapshot(long tableId)
            throws Exception {
        Optional<TableAssignment> optTableAssignment = getTableAssignment(tableId);
        if (!optTableAssignment.isPresent()) {
            return Collections.emptyMap();
        } else {
            TableAssignment tableAssignment = optTableAssignment.get();
            return getBucketSnapshots(tableId, null, tableAssignment);
        }
    }

    public Map<Integer, Optional<BucketSnapshot>> getPartitionLatestBucketSnapshot(long partitionId)
            throws Exception {
        Optional<PartitionAssignment> optPartitionAssignment = getPartitionAssignment(partitionId);
        if (!optPartitionAssignment.isPresent()) {
            return Collections.emptyMap();
        } else {
            return getBucketSnapshots(
                    optPartitionAssignment.get().getTableId(),
                    partitionId,
                    optPartitionAssignment.get());
        }
    }

    private Map<Integer, Optional<BucketSnapshot>> getBucketSnapshots(
            long tableId, @Nullable Long partitionId, TableAssignment tableAssignment)
            throws Exception {
        Map<Integer, Optional<BucketSnapshot>> snapshots = new HashMap<>();
        // first, put as empty for all buckets
        for (Integer bucket : tableAssignment.getBuckets()) {
            snapshots.put(bucket, Optional.empty());
        }

        // get the bucket ids
        String bucketIdsPath =
                partitionId == null
                        ? BucketIdsZNode.pathOfTable(tableId)
                        : BucketIdsZNode.pathOfPartition(partitionId);
        // iterate all buckets
        for (String bucketIdStr : getChildren(bucketIdsPath)) {
            // get the bucket id
            int bucketId = Integer.parseInt(bucketIdStr);
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
            // get the snapshot node for the bucket
            String bucketSnapshotPath = BucketSnapshotsZNode.path(tableBucket);
            // get all the snapshots for the bucket
            List<String> bucketSnapshots = getChildren(bucketSnapshotPath);

            Optional<Long> optLatestSnapshotId =
                    bucketSnapshots.stream().map(Long::parseLong).reduce(Math::max);
            Optional<BucketSnapshot> optTableBucketSnapshot = Optional.empty();
            if (optLatestSnapshotId.isPresent()) {
                optTableBucketSnapshot =
                        getTableBucketSnapshot(tableBucket, optLatestSnapshotId.get());
            }
            snapshots.put(bucketId, optTableBucketSnapshot);
        }
        return snapshots;
    }

    // --------------------------------------------------------------------------------------------
    // Writer
    // --------------------------------------------------------------------------------------------

    /** generate an unique id for writer. */
    public long getWriterIdAndIncrement() throws Exception {
        return writerIdCounter.getAndIncrement();
    }

    // --------------------------------------------------------------------------------------------
    // Remote log manifest handler
    // --------------------------------------------------------------------------------------------

    /**
     * Register or update the remote log manifest handle to zookeeper.
     *
     * <p>Note: If there is already a remote log manifest for the given table bucket, it will be
     * overwritten.
     */
    public void upsertRemoteLogManifestHandle(
            TableBucket tableBucket, RemoteLogManifestHandle remoteLogManifestHandle)
            throws Exception {
        String path = BucketRemoteLogsZNode.path(tableBucket);
        if (getRemoteLogManifestHandle(tableBucket).isPresent()) {
            zkClient.setData().forPath(path, BucketRemoteLogsZNode.encode(remoteLogManifestHandle));
        } else {
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .forPath(path, BucketRemoteLogsZNode.encode(remoteLogManifestHandle));
        }
    }

    public Optional<RemoteLogManifestHandle> getRemoteLogManifestHandle(TableBucket tableBucket)
            throws Exception {
        String path = BucketRemoteLogsZNode.path(tableBucket);
        return getOrEmpty(path).map(BucketRemoteLogsZNode::decode);
    }

    public void upsertLakeTableSnapshot(long tableId, LakeTableSnapshot lakeTableSnapshot)
            throws Exception {
        String path = LakeTableZNode.path(tableId);
        Optional<LakeTableSnapshot> optLakeTableSnapshot = getLakeTableSnapshot(tableId);
        if (optLakeTableSnapshot.isPresent()) {
            // we need to merge current lake table snapshot with previous
            // since the current lake table snapshot request won't carry all
            // the bucket for the table. It will only carry the bucket that is written
            // after the previous commit
            LakeTableSnapshot previous = optLakeTableSnapshot.get();

            // merge log startup offset, current will override the previous
            Map<TableBucket, Long> bucketLogStartOffset =
                    new HashMap<>(previous.getBucketLogStartOffset());
            bucketLogStartOffset.putAll(lakeTableSnapshot.getBucketLogStartOffset());

            // merge log end offsets, current will override the previous
            Map<TableBucket, Long> bucketLogEndOffset =
                    new HashMap<>(previous.getBucketLogEndOffset());
            bucketLogEndOffset.putAll(lakeTableSnapshot.getBucketLogEndOffset());

            // merge max timestamp, current will override the previous
            Map<TableBucket, Long> bucketMaxTimestamp =
                    new HashMap<>(previous.getBucketMaxTimestamp());
            bucketMaxTimestamp.putAll(lakeTableSnapshot.getBucketMaxTimestamp());

            Map<Long, String> partitionNameById =
                    new HashMap<>(previous.getPartitionNameIdByPartitionId());
            partitionNameById.putAll(lakeTableSnapshot.getPartitionNameIdByPartitionId());

            lakeTableSnapshot =
                    new LakeTableSnapshot(
                            lakeTableSnapshot.getSnapshotId(),
                            lakeTableSnapshot.getTableId(),
                            bucketLogStartOffset,
                            bucketLogEndOffset,
                            bucketMaxTimestamp,
                            partitionNameById);
            zkClient.setData().forPath(path, LakeTableZNode.encode(lakeTableSnapshot));
        } else {
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .forPath(path, LakeTableZNode.encode(lakeTableSnapshot));
        }
    }

    public Optional<LakeTableSnapshot> getLakeTableSnapshot(long tableId) throws Exception {
        String path = LakeTableZNode.path(tableId);
        return getOrEmpty(path).map(LakeTableZNode::decode);
    }

    /**
     * Register or update the acl registration to zookeeper.
     *
     * <p>Note: If there is already an acl registration for the given resources and the acl version
     * is smaller than new one, it will be overwritten.(we need to compare the version before
     * upsert)
     *
     * @return the version of the acl node after upsert.
     */
    public int updateResourceAcl(
            Resource resource, Set<AccessControlEntry> accessControlEntries, int expectedVersion)
            throws Exception {
        String path = ResourceAclNode.path(resource);
        LOG.info("update acl node {} with value {}", resource, accessControlEntries);
        return zkClient.setData()
                .withVersion(expectedVersion)
                .forPath(path, ResourceAclNode.encode(new ResourceAcl(accessControlEntries)))
                .getVersion();
    }

    public void createResourceAcl(Resource resource, Set<AccessControlEntry> accessControlEntries)
            throws Exception {
        String path = ResourceAclNode.path(resource);
        LOG.info("insert acl node {} with value {}", resource, accessControlEntries);
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(path, ResourceAclNode.encode(new ResourceAcl(accessControlEntries)));
    }

    /**
     * Retrieves the ACL (Access Control List) for a specific resource from ZooKeeper.
     *
     * @param resource the resource to query
     * @return an Optional containing the ResourceAcl if it exists, or empty if not found
     * @throws Exception if there is an error accessing ZooKeeper
     */
    public VersionedAcls getResourceAclWithVersion(Resource resource) throws Exception {
        String path = ResourceAclNode.path(resource);
        try {
            Stat stat = new Stat();
            byte[] bytes = zkClient.getData().storingStatIn(stat).forPath(path);
            int zkVersion = stat.getVersion();
            Optional<ResourceAcl> resourceAcl =
                    Optional.ofNullable(bytes).map(ResourceAclNode::decode);

            return new VersionedAcls(
                    zkVersion,
                    resourceAcl.isPresent()
                            ? resourceAcl.get().getEntries()
                            : Collections.emptySet());
        } catch (KeeperException.NoNodeException e) {
            return new VersionedAcls(UNKNOWN_VERSION, Collections.emptySet());
        }
    }

    /**
     * Retrieves all resources of a specific type and their corresponding ACLs from ZooKeeper.
     *
     * @param resourceType the type of resource to query
     * @return a list of child node names representing resources of the given type
     * @throws Exception if there is an error accessing ZooKeeper
     */
    public List<String> listResourcesByType(ResourceType resourceType) throws Exception {
        String path = ResourceAclNode.path(resourceType);
        return getChildren(path);
    }

    /**
     * Deletes the ACL (Access Control List) for a specific resource from ZooKeeper.
     *
     * @param resource the resource whose ACL should be deleted
     * @throws Exception if there is an error accessing ZooKeeper
     */
    public void conditionalDeleteResourceAcl(Resource resource, int zkVersion) throws Exception {
        String path = ResourceAclNode.path(resource);
        zkClient.delete().withVersion(zkVersion).forPath(path);
    }

    public void insertAclChangeNotification(Resource resource) throws Exception {
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
                .forPath(
                        AclChangeNotificationNode.pathPrefix(),
                        AclChangeNotificationNode.encode(resource));
        LOG.info("add acl change notification for resource {}  ", resource);
    }

    // --------------------------------------------------------------------------------------------
    // Utils
    // --------------------------------------------------------------------------------------------

    /**
     * Gets all the child nodes at a given zk node path.
     *
     * @param path the path to list children
     * @return list of child node names
     */
    public List<String> getChildren(String path) throws Exception {
        try {
            return zkClient.getChildren().forPath(path);
        } catch (KeeperException.NoNodeException e) {
            return Collections.emptyList();
        }
    }

    /**
     * Gets the child nodes at given zk node paths in background asynchronously.
     *
     * @param paths the paths to list children
     * @return CompletableFuture containing list of async responses for each path
     */
    public CompletableFuture<List<ZkGetChildrenResponse>> getChildrenInBackgroundAsync(
            Collection<String> paths) {
        List<ZkGetChildrenRequest> requests =
                paths.stream().map(ZkGetChildrenRequest::new).collect(Collectors.toList());
        return handleRequestInBackgroundAsync(requests, ZkGetChildrenResponse::create);
    }

    /**
     * Gets the child nodes at given zk node paths in background.
     *
     * @param paths the paths to list children
     * @return list of async responses for each path
     * @throws Exception if there is an error during the operation
     */
    public List<ZkGetChildrenResponse> getChildrenInBackground(Collection<String> paths)
            throws Exception {
        List<ZkGetChildrenRequest> requests =
                paths.stream().map(ZkGetChildrenRequest::new).collect(Collectors.toList());
        return handleRequestInBackground(requests, ZkGetChildrenResponse::create);
    }

    /**
     * Gets the data of given zk node paths in background asynchronously.
     *
     * @param paths the paths to fetch data
     * @return CompletableFuture containing list of async responses for each path
     */
    public CompletableFuture<List<ZkGetDataResponse>> getDataInBackgroundAsync(
            Collection<String> paths) {
        List<ZkGetDataRequest> requests =
                paths.stream().map(ZkGetDataRequest::new).collect(Collectors.toList());
        return handleRequestInBackgroundAsync(requests, ZkGetDataResponse::create);
    }

    /**
     * Gets the data of given zk node paths in background.
     *
     * @param paths the paths to fetch data
     * @return list of async responses for each path
     * @throws Exception if there is an error during the operation
     */
    public List<ZkGetDataResponse> getDataInBackground(Collection<String> paths) throws Exception {
        List<ZkGetDataRequest> requests =
                paths.stream().map(ZkGetDataRequest::new).collect(Collectors.toList());
        return handleRequestInBackground(requests, ZkGetDataResponse::create);
    }

    /** Gets the data and stat of a given zk node path. */
    public Optional<Stat> getStat(String path) throws Exception {
        try {
            Stat stat = zkClient.checkExists().forPath(path);
            return Optional.ofNullable(stat);
        } catch (KeeperException.NoNodeException e) {
            return Optional.empty();
        }
    }

    /** delete a path. */
    public void deletePath(String path) throws Exception {
        try {
            zkClient.delete().forPath(path);
        } catch (KeeperException.NoNodeException ignored) {
        }
    }

    public CuratorFramework getCuratorClient() {
        return zkClient;
    }

    // --------------------------------------------------------------------------------------------
    // Table and Partition Metadata
    // --------------------------------------------------------------------------------------------

    /**
     * Asynchronously retrieves table metadata from ZooKeeper for the specified table.
     *
     * <p>This method fetches the table assignment information from ZooKeeper and converts it into a
     * list of bucket metadata. The operation is performed asynchronously to avoid blocking the
     * calling thread.
     *
     * @param tablePath the path of the table to retrieve metadata for
     * @param tableId the unique identifier of the table
     * @param isPartitioned whether the table is partitioned (affects warning behavior for missing
     *     assignments)
     * @return a CompletableFuture containing a list of BucketMetadata objects representing all
     *     buckets for this table. Returns an empty list if no table assignment is found.
     * @throws TableNotExistException if the table does not exist in ZooKeeper
     * @throws FlussRuntimeException if there's an error retrieving or processing the metadata
     */
    public CompletableFuture<List<BucketMetadata>> getTableMetadataFromZkAsync(
            TablePath tablePath, long tableId, boolean isPartitioned) {

        PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tablePath);

        return getAssignmentInfoAsync(tableId, physicalTablePath)
                .thenCompose(
                        assignmentInfo -> {
                            if (assignmentInfo.tableAssignment == null) {
                                if (isPartitioned) {
                                    LOG.warn(
                                            "No table assignment node found for table {}", tableId);
                                }
                                return CompletableFuture.completedFuture(
                                        new ArrayList<BucketMetadata>());
                            }

                            TableAssignment tableAssignment = assignmentInfo.tableAssignment;
                            if (tableAssignment == null) {
                                return CompletableFuture.completedFuture(
                                        new ArrayList<BucketMetadata>());
                            }
                            tableAssignment = checkNotNull(tableAssignment);

                            Map<Integer, BucketAssignment> bucketAssignments =
                                    tableAssignment.getBucketAssignments();
                            if (bucketAssignments.isEmpty()) {
                                return CompletableFuture.completedFuture(
                                        new ArrayList<BucketMetadata>());
                            }

                            // Create TableBucket collection for batch processing
                            List<TableBucket> tableBuckets = new ArrayList<>();
                            Map<TableBucket, Map.Entry<Integer, BucketAssignment>>
                                    bucketToAssignmentMap = new HashMap<>();

                            for (Map.Entry<Integer, BucketAssignment> assignment :
                                    bucketAssignments.entrySet()) {
                                int bucketId = assignment.getKey();
                                TableBucket tableBucket = new TableBucket(tableId, null, bucketId);
                                tableBuckets.add(tableBucket);
                                bucketToAssignmentMap.put(tableBucket, assignment);
                            }

                            // Use batch async method to get all LeaderAndIsr information in one ZK
                            // call
                            return getLeaderAndIsrsAsync(tableBuckets)
                                    .thenApply(
                                            leaderAndIsrMap ->
                                                    ZooKeeperMetadataUtils
                                                            .assembleBucketMetadataList(
                                                                    tableBuckets,
                                                                    bucketToAssignmentMap,
                                                                    leaderAndIsrMap,
                                                                    null));
                        })
                .exceptionally(
                        throwable ->
                                ZooKeeperMetadataUtils.handleTableMetadataException(
                                        throwable, tablePath, tableId));
    }

    public static CompletableFuture<List<BucketMetadata>> getTableMetadataFromZkAsync(
            ZooKeeperClient zkClient, TablePath tablePath, long tableId, boolean isPartitioned) {
        return zkClient.getTableMetadataFromZkAsync(tablePath, tableId, isPartitioned);
    }

    /**
     * Asynchronously retrieves partition metadata from ZooKeeper for the specified partition.
     *
     * <p>This method fetches the partition assignment information from ZooKeeper and converts it
     * into partition metadata including all bucket metadata. The operation is performed
     * asynchronously to avoid blocking the calling thread.
     *
     * @param partitionPath the physical path of the partition to retrieve metadata for
     * @return a CompletableFuture containing a PartitionMetadata object representing the partition
     *     and all its buckets. Returns a PartitionMetadata with empty bucket list if no partition
     *     assignment is found.
     * @throws PartitionNotExistException if the partition does not exist in ZooKeeper
     * @throws FlussRuntimeException if there's an error retrieving or processing the metadata
     * @throws IllegalArgumentException if the partition ID is null (should never happen for valid
     *     partitions)
     */
    public CompletableFuture<PartitionMetadata> getPartitionMetadataFromZkAsync(
            PhysicalTablePath partitionPath) {

        return getAssignmentInfoAsync(null, partitionPath)
                .thenCompose(
                        assignmentInfo -> {
                            checkNotNull(
                                    assignmentInfo.partitionId,
                                    "partition id must be not null for " + partitionPath);

                            if (assignmentInfo.tableAssignment == null) {
                                LOG.warn(
                                        "No partition assignment node found for partition {}",
                                        partitionPath);

                                PartitionMetadata emptyMetadata =
                                        new PartitionMetadata(
                                                assignmentInfo.tableId,
                                                partitionPath.getPartitionName(),
                                                assignmentInfo.partitionId != null
                                                        ? assignmentInfo.partitionId
                                                        : -1L,
                                                new ArrayList<>());

                                return CompletableFuture.completedFuture(emptyMetadata);
                            }

                            TableAssignment tableAssignment =
                                    checkNotNull(assignmentInfo.tableAssignment);
                            Map<Integer, BucketAssignment> bucketAssignments =
                                    tableAssignment.getBucketAssignments();
                            if (bucketAssignments.isEmpty()) {
                                return CompletableFuture.completedFuture(
                                        new PartitionMetadata(
                                                assignmentInfo.tableId,
                                                partitionPath.getPartitionName(),
                                                assignmentInfo.partitionId != null
                                                        ? assignmentInfo.partitionId
                                                        : -1L,
                                                new ArrayList<>()));
                            }

                            // Create TableBucket collection for batch processing
                            List<TableBucket> tableBuckets = new ArrayList<>();
                            Map<TableBucket, Map.Entry<Integer, BucketAssignment>>
                                    bucketToAssignmentMap = new HashMap<>();

                            for (Map.Entry<Integer, BucketAssignment> assignment :
                                    bucketAssignments.entrySet()) {
                                int bucketId = assignment.getKey();
                                TableBucket tableBucket =
                                        new TableBucket(
                                                assignmentInfo.tableId,
                                                assignmentInfo.partitionId,
                                                bucketId);
                                tableBuckets.add(tableBucket);
                                bucketToAssignmentMap.put(tableBucket, assignment);
                            }

                            // Use batch async method to get all LeaderAndIsr information in one ZK
                            // call
                            return getLeaderAndIsrsAsync(tableBuckets)
                                    .thenApply(
                                            leaderAndIsrMap -> {
                                                List<BucketMetadata> bucketMetadataList =
                                                        ZooKeeperMetadataUtils
                                                                .assembleBucketMetadataList(
                                                                        tableBuckets,
                                                                        bucketToAssignmentMap,
                                                                        leaderAndIsrMap,
                                                                        null);
                                                return new PartitionMetadata(
                                                        assignmentInfo.tableId,
                                                        partitionPath.getPartitionName(),
                                                        assignmentInfo.partitionId != null
                                                                ? assignmentInfo.partitionId
                                                                : -1L,
                                                        bucketMetadataList);
                                            });
                        })
                .exceptionally(
                        throwable ->
                                ZooKeeperMetadataUtils.handlePartitionException(
                                        throwable,
                                        "Failed to get metadata for partition {}",
                                        partitionPath,
                                        String.format(
                                                "Failed to get metadata for partition %s",
                                                partitionPath)));
    }

    public static CompletableFuture<PartitionMetadata> getPartitionMetadataFromZkAsync(
            PhysicalTablePath partitionPath, ZooKeeperClient zkClient) {
        return zkClient.getPartitionMetadataFromZkAsync(partitionPath);
    }

    /**
     * Asynchronously retrieves partition metadata from ZooKeeper for multiple partitions in batch.
     *
     * <p>This method efficiently fetches partition metadata for multiple partitions across multiple
     * tables in a single batched operation. It performs the following steps:
     *
     * <ul>
     *   <li>Maps table paths to their partition information (name and ID mappings)
     *   <li>Validates that all requested partition IDs exist
     *   <li>Builds partition path information with resolved table IDs
     *   <li>Batch retrieves partition assignments and bucket metadata
     * </ul>
     *
     * <p>The batch operation is optimized to minimize ZooKeeper calls by grouping related
     * operations together, making it much more efficient than calling {@link
     * #getPartitionMetadataFromZkAsync(PhysicalTablePath)} individually for each partition.
     *
     * @param tablePaths the collection of table paths that may contain the requested partitions.
     *     This is used to scope the search and improve efficiency. Cannot be empty if
     *     partitionIdSet is not empty.
     * @param partitionIdSet the set of partition IDs to retrieve metadata for. Cannot be empty if
     *     tablePaths is not empty.
     * @return a CompletableFuture containing a list of PartitionMetadata objects for all found
     *     partitions. The list may be smaller than the input partition ID set if some partitions
     *     have no assignment data. Returns an empty list if either input parameter is empty.
     * @throws PartitionNotExistException if any of the requested partition IDs do not exist in any
     *     of the provided tables
     * @throws FlussRuntimeException if there's an error during ZooKeeper operations or metadata
     *     processing
     */
    public CompletableFuture<List<PartitionMetadata>> batchGetPartitionMetadataFromZkAsync(
            Collection<TablePath> tablePaths, Set<Long> partitionIdSet) {
        if (tablePaths.isEmpty() || partitionIdSet.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.<PartitionMetadata>emptyList());
        }

        return CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return getPartitionNameAndIdsForTables(new ArrayList<>(tablePaths));
                            } catch (Exception e) {
                                throw new RuntimeException(
                                        "Failed to get partition name and ids for tables", e);
                            }
                        })
                .thenCompose(
                        tableToPartitionInfo -> {
                            // Inline collectValidPartitionsAndTables logic
                            Set<Long> remainingPartitionIds = new HashSet<>(partitionIdSet);
                            Set<TablePath> tablesNeedingIds = new HashSet<>();
                            Map<TablePath, List<Tuple2<String, Long>>> tablePartitionMapping =
                                    new HashMap<>();

                            for (Map.Entry<TablePath, Map<String, Long>> entry :
                                    tableToPartitionInfo.entrySet()) {
                                TablePath tablePath = entry.getKey();
                                Map<String, Long> partitionInfo = entry.getValue();

                                List<Tuple2<String, Long>> validPartitions =
                                        ZooKeeperMetadataUtils.findValidPartitionsForTable(
                                                partitionInfo,
                                                partitionIdSet,
                                                remainingPartitionIds);

                                if (!validPartitions.isEmpty()) {
                                    tablesNeedingIds.add(tablePath);
                                    tablePartitionMapping.put(tablePath, validPartitions);
                                }
                            }

                            if (!remainingPartitionIds.isEmpty()) {
                                throw new PartitionNotExistException(
                                        "Partition not exist for partition ids: "
                                                + remainingPartitionIds);
                            }

                            if (tablePartitionMapping.isEmpty()) {
                                return CompletableFuture.completedFuture(
                                        Collections.<PartitionPathInfo>emptyList());
                            }

                            return getTablesAsync(tablesNeedingIds)
                                    .thenApply(
                                            tableRegistrations ->
                                                    ZooKeeperMetadataUtils
                                                            .buildPartitionPathInfoList(
                                                                    tablePartitionMapping,
                                                                    tableRegistrations));
                        })
                .thenCompose(
                        partitionPathInfos -> {
                            if (partitionPathInfos.isEmpty()) {
                                return CompletableFuture.completedFuture(
                                        Collections.<PartitionMetadata>emptyList());
                            }

                            List<Long> partitionIds =
                                    partitionPathInfos.stream()
                                            .map(info -> info.partitionId)
                                            .collect(Collectors.toList());

                            CompletableFuture<Map<Long, PartitionAssignment>>
                                    partitionAssignmentsFuture;
                            if (partitionIds.isEmpty()) {
                                partitionAssignmentsFuture =
                                        CompletableFuture.completedFuture(Collections.emptyMap());
                            } else {
                                Map<String, Long> path2PartitionIdMap =
                                        partitionIds.stream()
                                                .collect(
                                                        Collectors.toMap(
                                                                PartitionIdZNode::path, id -> id));

                                partitionAssignmentsFuture =
                                        getDataInBackgroundAsync(path2PartitionIdMap.keySet())
                                                .thenApply(
                                                        responses ->
                                                                ZooKeeperMetadataUtils
                                                                        .processMultipleDataResponses(
                                                                                responses,
                                                                                response ->
                                                                                        path2PartitionIdMap
                                                                                                .get(
                                                                                                        response
                                                                                                                .getPath()),
                                                                                PartitionIdZNode
                                                                                        ::decode,
                                                                                "partition assignment"));
                            }

                            return partitionAssignmentsFuture.thenCompose(
                                    partitionAssignments -> {
                                        List<AssignmentInfo> assignmentInfos = new ArrayList<>();
                                        for (PartitionPathInfo pathInfo : partitionPathInfos) {
                                            PartitionAssignment partitionAssignment =
                                                    partitionAssignments.get(pathInfo.partitionId);
                                            AssignmentInfo assignmentInfo =
                                                    new AssignmentInfo(
                                                            pathInfo.tableId,
                                                            partitionAssignment,
                                                            pathInfo.partitionId);
                                            assignmentInfos.add(assignmentInfo);
                                        }

                                        List<TableBucket> allTableBuckets = new ArrayList<>();
                                        Map<TableBucket, Integer> bucketToIndexMap =
                                                new HashMap<>();

                                        for (int i = 0; i < assignmentInfos.size(); i++) {
                                            AssignmentInfo assignmentInfo = assignmentInfos.get(i);
                                            PartitionPathInfo pathInfo = partitionPathInfos.get(i);

                                            if (assignmentInfo.tableAssignment != null) {
                                                for (Integer bucketId :
                                                        assignmentInfo.tableAssignment
                                                                .getBuckets()) {
                                                    TableBucket tableBucket =
                                                            new TableBucket(
                                                                    pathInfo.tableId,
                                                                    pathInfo.partitionId,
                                                                    bucketId);
                                                    allTableBuckets.add(tableBucket);
                                                    bucketToIndexMap.put(tableBucket, i);
                                                }
                                            }
                                        }

                                        return getLeaderAndIsrsAsync(allTableBuckets)
                                                .thenApply(
                                                        leaderAndIsrMap ->
                                                                ZooKeeperMetadataUtils
                                                                        .assemblePartitionMetadata(
                                                                                partitionPathInfos,
                                                                                assignmentInfos,
                                                                                bucketToIndexMap,
                                                                                leaderAndIsrMap));
                                    });
                        })
                .exceptionally(
                        throwable ->
                                ZooKeeperMetadataUtils.handlePartitionException(
                                        throwable,
                                        "Failed to batch get partition metadata for partition ids: {}",
                                        partitionIdSet,
                                        "Failed to batch get partition metadata for partition ids: "
                                                + partitionIdSet));
    }

    private CompletableFuture<AssignmentInfo> getAssignmentInfoAsync(
            @Nullable Long tableId, PhysicalTablePath physicalTablePath) {

        if (physicalTablePath.getPartitionName() != null) {
            return getPartitionAsync(
                            physicalTablePath.getTablePath(), physicalTablePath.getPartitionName())
                    .thenApply(
                            tablePartition -> {
                                if (tablePartition == null) {
                                    throw new PartitionNotExistException(
                                            "Table partition '"
                                                    + physicalTablePath
                                                    + "' does not exist.");
                                }
                                return tablePartition;
                            })
                    .thenCompose(
                            tablePartition -> {
                                long partitionId = tablePartition.getPartitionId();
                                long partitionTableId = tablePartition.getTableId();

                                return getPartitionAssignmentAsync(partitionId)
                                        .thenApply(
                                                tableAssignment ->
                                                        new AssignmentInfo(
                                                                partitionTableId,
                                                                tableAssignment,
                                                                partitionId));
                            });
        } else {
            checkNotNull(tableId, "tableId must be not null");
            long realTableId = tableId != null ? tableId : -1L;

            return getTableAssignmentAsync(realTableId)
                    .thenApply(
                            tableAssignment ->
                                    new AssignmentInfo(realTableId, tableAssignment, null));
        }
    }

    /** Close the underlying ZooKeeperClient. */
    @Override
    public void close() {
        LOG.info("Closing...");
        if (curatorFrameworkWrapper != null) {
            curatorFrameworkWrapper.close();
        }
    }
}
