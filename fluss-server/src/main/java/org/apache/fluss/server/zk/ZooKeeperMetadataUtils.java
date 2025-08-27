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

import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.PartitionMetadata;
import org.apache.fluss.server.zk.ZkAsyncResponse.ZkGetChildrenResponse;
import org.apache.fluss.server.zk.ZkAsyncResponse.ZkGetDataResponse;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/** Utility class containing helper methods for ZooKeeper metadata operations. */
public class ZooKeeperMetadataUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperMetadataUtils.class);

    /**
     * Template method to process multiple ZooKeeper data responses with decoder.
     *
     * @param responses list of ZkGetDataResponse from ZooKeeper
     * @param keyExtractor function to extract key from response
     * @param decoder function to decode byte array to target type
     * @param operationName name of the operation for error messages
     * @param <K> the type of the result map key
     * @param <V> the type of the result map value
     * @return Map containing decoded results for successful responses
     */
    public static <K, V> Map<K, V> processMultipleDataResponses(
            List<ZkGetDataResponse> responses,
            Function<ZkGetDataResponse, K> keyExtractor,
            Function<byte[], V> decoder,
            String operationName) {
        Map<K, V> result = new HashMap<>();
        for (ZkGetDataResponse response : responses) {
            if (response.getResultCode() == KeeperException.Code.OK) {
                K key = keyExtractor.apply(response);
                V value = decoder.apply(response.getData());
                result.put(key, value);
            } else {
                LOG.warn(
                        "Failed to get {} for path {}: {}",
                        operationName,
                        response.getPath(),
                        response.getResultCode());
            }
        }
        return result;
    }

    /**
     * Template method to process multiple ZooKeeper data responses with decoder and condition.
     *
     * @param responses list of ZkGetDataResponse from ZooKeeper
     * @param keyExtractor function to extract key from response
     * @param decoder function to decode byte array to target type
     * @param condition additional condition to check before processing (beyond OK status)
     * @param operationName name of the operation for error messages
     * @param <K> the type of the result map key
     * @param <V> the type of the result map value
     * @return Map containing decoded results for successful responses
     */
    public static <K, V> Map<K, V> processMultipleDataResponsesWithCondition(
            List<ZkGetDataResponse> responses,
            Function<ZkGetDataResponse, K> keyExtractor,
            Function<byte[], V> decoder,
            Function<ZkGetDataResponse, Boolean> condition,
            String operationName) {
        Map<K, V> result = new HashMap<>();
        for (ZkGetDataResponse response : responses) {
            if (response.getResultCode() == KeeperException.Code.OK && condition.apply(response)) {
                K key = keyExtractor.apply(response);
                V value = decoder.apply(response.getData());
                result.put(key, value);
            } else {
                String reason =
                        response.getResultCode() != KeeperException.Code.OK
                                ? response.getResultCode().toString()
                                : "condition check failed";
                LOG.warn(
                        "Failed to get {} for path {}: {}",
                        operationName,
                        response.getPath(),
                        reason);
            }
        }
        return result;
    }

    /**
     * Template method to process multiple ZooKeeper data responses with decoder and Optional
     * result. This method handles NONODE as a valid case returning Optional.empty().
     *
     * @param responses list of ZkGetDataResponse from ZooKeeper
     * @param keyExtractor function to extract key from response
     * @param decoder function to decode byte array to target type
     * @param operationName name of the operation for error messages
     * @param <K> the type of the result map key
     * @param <V> the type of the result map value
     * @return Map containing Optional results for all responses (including NONODE cases)
     */
    public static <K, V> Map<K, Optional<V>> processMultipleDataResponsesWithOptional(
            List<ZkGetDataResponse> responses,
            Function<ZkGetDataResponse, K> keyExtractor,
            Function<byte[], V> decoder,
            String operationName) {
        Map<K, Optional<V>> result = new HashMap<>();
        for (ZkGetDataResponse response : responses) {
            K key = keyExtractor.apply(response);
            if (response.getResultCode() == KeeperException.Code.OK) {
                V value = decoder.apply(response.getData());
                result.put(key, Optional.of(value));
            } else if (response.getResultCode() == KeeperException.Code.NONODE) {
                result.put(key, Optional.empty());
            } else {
                LOG.warn(
                        "Failed to get {} for path {}: {}",
                        operationName,
                        response.getPath(),
                        response.getResultCode());
                result.put(key, Optional.empty());
            }
        }
        return result;
    }

    /**
     * Template method to process multiple ZooKeeper children responses with decoder.
     *
     * @param responses list of ZkGetChildrenResponse from ZooKeeper
     * @param keyExtractor function to extract key from response
     * @param operationName name of the operation for error messages
     * @param <K> the type of the result map key
     * @return Map containing children lists for successful responses
     */
    public static <K> Map<K, List<String>> processMultipleChildrenResponses(
            List<ZkGetChildrenResponse> responses,
            Function<ZkGetChildrenResponse, K> keyExtractor,
            String operationName) {
        Map<K, List<String>> result = new HashMap<>();
        for (ZkGetChildrenResponse response : responses) {
            if (response.getResultCode() == KeeperException.Code.OK) {
                K key = keyExtractor.apply(response);
                result.put(key, response.getChildren());
            } else {
                LOG.warn(
                        "Failed to get {} for path {}: {}",
                        operationName,
                        response.getPath(),
                        response.getResultCode());
            }
        }
        return result;
    }

    /**
     * Template method to process single ZooKeeper data response with decoder.
     *
     * @param responses list of ZkGetDataResponse from ZooKeeper
     * @param decoder function to decode byte array to target type
     * @param path ZooKeeper path for error messages
     * @param <T> the type of the decoded result
     * @return decoded result, or null if no data or NONODE
     */
    public static <T> T processSingleDataResponse(
            List<ZkGetDataResponse> responses, Function<byte[], T> decoder, String path) {
        if (responses.isEmpty()) {
            return null;
        }

        ZkGetDataResponse response = responses.get(0);
        if (response.getResultCode() == KeeperException.Code.OK) {
            return decoder.apply(response.getData());
        } else if (response.getResultCode() == KeeperException.Code.NONODE) {
            return null;
        } else {
            throw new RuntimeException(KeeperException.create(response.getResultCode(), path));
        }
    }

    /**
     * Handle exceptions that occur during table metadata retrieval.
     *
     * @param throwable the exception that occurred
     * @param tablePath the table path for error context
     * @param tableId the table ID for error context
     * @return never returns normally, always throws an exception
     * @throws TableNotExistException if the original cause was table not existing
     * @throws FlussRuntimeException for other errors
     */
    public static List<BucketMetadata> handleTableMetadataException(
            Throwable throwable, TablePath tablePath, long tableId) {

        Throwable cause = ExceptionUtils.stripCompletionException(throwable);
        cause = ExceptionUtils.stripException(cause, RuntimeException.class);

        if (cause instanceof TableNotExistException) {
            throw (TableNotExistException) cause;
        }

        LOG.error("Failed to get metadata for table {}, id {}", tablePath, tableId, cause);
        throw new FlussRuntimeException(
                String.format("Failed to get metadata for table %s, id %d", tablePath, tableId),
                cause);
    }

    /**
     * Assemble a list of BucketMetadata from table buckets, assignments, and leader information.
     *
     * @param tableBuckets the list of table buckets to process
     * @param bucketToAssignmentMap mapping from table buckets to their assignment entries
     * @param leaderAndIsrMap mapping from table buckets to their leader and ISR information
     * @param leaderEpoch the leader epoch to use for all buckets (can be null)
     * @return a list of BucketMetadata objects
     */
    public static List<BucketMetadata> assembleBucketMetadataList(
            List<TableBucket> tableBuckets,
            Map<TableBucket, Map.Entry<Integer, BucketAssignment>> bucketToAssignmentMap,
            Map<TableBucket, Optional<LeaderAndIsr>> leaderAndIsrMap,
            @Nullable Integer leaderEpoch) {

        List<BucketMetadata> result = new ArrayList<>();

        for (TableBucket tableBucket : tableBuckets) {
            Map.Entry<Integer, BucketAssignment> assignment =
                    bucketToAssignmentMap.get(tableBucket);
            int bucketId = assignment.getKey();
            List<Integer> replicas = assignment.getValue().getReplicas();

            Optional<LeaderAndIsr> optLeaderAndIsr = leaderAndIsrMap.get(tableBucket);
            Integer leader = optLeaderAndIsr.map(LeaderAndIsr::leader).orElse(null);

            result.add(new BucketMetadata(bucketId, leader, leaderEpoch, replicas));
        }

        return result;
    }

    /**
     * Common exception handler for partition-related operations.
     *
     * @param throwable the exception that occurred
     * @param logMessageTemplate the template for log message (with {} placeholder)
     * @param logContext the context object to be logged
     * @param errorMessage the error message for FlussRuntimeException
     * @param <T> the return type (never actually returns)
     * @return never returns normally, always throws an exception
     * @throws PartitionNotExistException if the original cause was partition not existing
     * @throws FlussRuntimeException for other errors
     */
    public static <T> T handlePartitionException(
            Throwable throwable,
            String logMessageTemplate,
            Object logContext,
            String errorMessage) {

        Throwable cause = ExceptionUtils.stripCompletionException(throwable);
        cause = ExceptionUtils.stripException(cause, RuntimeException.class);

        if (cause instanceof PartitionNotExistException) {
            throw (PartitionNotExistException) cause;
        }

        LOG.error(logMessageTemplate, logContext, cause);
        throw new FlussRuntimeException(errorMessage, cause);
    }

    /**
     * Find valid partitions for a specific table.
     *
     * @param partitionInfo partition information for the table
     * @param partitionIdSet the set of partition IDs to look for
     * @param remainingPartitionIds mutable set to track unmatched partition IDs
     * @return list of valid partitions (name, id) tuples
     */
    public static List<Tuple2<String, Long>> findValidPartitionsForTable(
            Map<String, Long> partitionInfo,
            Set<Long> partitionIdSet,
            Set<Long> remainingPartitionIds) {

        List<Tuple2<String, Long>> validPartitions = new ArrayList<>();

        for (Map.Entry<String, Long> partitionEntry : partitionInfo.entrySet()) {
            Long partitionId = partitionEntry.getValue();
            if (partitionIdSet.contains(partitionId)) {
                String partitionName = partitionEntry.getKey();
                validPartitions.add(Tuple2.of(partitionName, partitionId));
                remainingPartitionIds.remove(partitionId);
            }
        }

        return validPartitions;
    }

    /**
     * Build the list of PartitionPathInfo from table registrations and partition mappings.
     *
     * @param tablePartitionMapping mapping of tables to their valid partitions
     * @param tableRegistrations table registration information containing table IDs
     * @return list of PartitionPathInfo objects
     */
    public static List<PartitionPathInfo> buildPartitionPathInfoList(
            Map<TablePath, List<Tuple2<String, Long>>> tablePartitionMapping,
            Map<TablePath, TableRegistration> tableRegistrations) {

        List<PartitionPathInfo> targetPartitionInfos = new ArrayList<>();

        for (Map.Entry<TablePath, List<Tuple2<String, Long>>> entry :
                tablePartitionMapping.entrySet()) {
            TablePath tablePath = entry.getKey();
            TableRegistration tableRegistration = tableRegistrations.get(tablePath);

            if (tableRegistration != null) {
                long tableId = tableRegistration.tableId;
                addPartitionPathInfosForTable(
                        targetPartitionInfos, tablePath, tableId, entry.getValue());
            }
        }

        return targetPartitionInfos;
    }

    /**
     * Add PartitionPathInfo objects for all partitions of a specific table.
     *
     * @param targetPartitionInfos target list to add to
     * @param tablePath the table path
     * @param tableId the table ID
     * @param partitionInfos list of partition (name, id) tuples
     */
    public static void addPartitionPathInfosForTable(
            List<PartitionPathInfo> targetPartitionInfos,
            TablePath tablePath,
            long tableId,
            List<Tuple2<String, Long>> partitionInfos) {

        for (Tuple2<String, Long> partitionInfo : partitionInfos) {
            String partitionName = partitionInfo.f0;
            Long partitionId = partitionInfo.f1;
            PhysicalTablePath partitionPath = PhysicalTablePath.of(tablePath, partitionName);
            targetPartitionInfos.add(new PartitionPathInfo(partitionPath, partitionId, tableId));
        }
    }

    /**
     * Assemble partition metadata from the collected bucket information.
     *
     * @param partitionPathInfos the partition path information
     * @param assignmentInfos the assignment information for each partition
     * @param bucketToIndexMap mapping from table buckets to partition index
     * @param leaderAndIsrMap mapping from table buckets to leader information
     * @return list of assembled partition metadata
     */
    public static List<PartitionMetadata> assemblePartitionMetadata(
            List<PartitionPathInfo> partitionPathInfos,
            List<AssignmentInfo> assignmentInfos,
            Map<TableBucket, Integer> bucketToIndexMap,
            Map<TableBucket, Optional<LeaderAndIsr>> leaderAndIsrMap) {

        Map<Integer, List<BucketMetadata>> indexToBuckets = new HashMap<>();

        for (Map.Entry<TableBucket, Optional<LeaderAndIsr>> entry : leaderAndIsrMap.entrySet()) {
            TableBucket tableBucket = entry.getKey();
            Optional<LeaderAndIsr> optLeaderAndIsr = entry.getValue();
            Integer assignmentIndex = bucketToIndexMap.get(tableBucket);

            if (assignmentIndex != null) {
                AssignmentInfo assignmentInfo = assignmentInfos.get(assignmentIndex);
                if (assignmentInfo.tableAssignment != null) {
                    BucketAssignment bucketAssignment =
                            assignmentInfo
                                    .tableAssignment
                                    .getBucketAssignments()
                                    .get(tableBucket.getBucket());

                    if (bucketAssignment != null) {
                        Integer leader = optLeaderAndIsr.map(LeaderAndIsr::leader).orElse(null);
                        BucketMetadata bucketMetadata =
                                new BucketMetadata(
                                        tableBucket.getBucket(),
                                        leader,
                                        // leaderEpoch
                                        null,
                                        bucketAssignment.getReplicas());

                        indexToBuckets
                                .computeIfAbsent(assignmentIndex, k -> new ArrayList<>())
                                .add(bucketMetadata);
                    }
                }
            }
        }

        List<PartitionMetadata> result = new ArrayList<>();
        for (int i = 0; i < partitionPathInfos.size(); i++) {
            PartitionPathInfo pathInfo = partitionPathInfos.get(i);
            List<BucketMetadata> bucketMetadataList =
                    indexToBuckets.getOrDefault(i, new ArrayList<>());

            result.add(
                    new PartitionMetadata(
                            pathInfo.tableId,
                            pathInfo.partitionPath.getPartitionName(),
                            pathInfo.partitionId,
                            bucketMetadataList));
        }

        return result;
    }

    /** Data structure to hold partition path and its corresponding partition ID. */
    public static class PartitionPathInfo {
        public final PhysicalTablePath partitionPath;
        public final long partitionId;
        public final long tableId;

        public PartitionPathInfo(PhysicalTablePath partitionPath, long partitionId, long tableId) {
            this.partitionPath = partitionPath;
            this.partitionId = partitionId;
            this.tableId = tableId;
        }
    }

    /** Data structure to hold assignment information. */
    public static class AssignmentInfo {
        public final long tableId;
        // null then the bucket doesn't belong to a partition. Otherwise, not null
        public final @Nullable Long partitionId;
        public final @Nullable TableAssignment tableAssignment;

        public AssignmentInfo(
                long tableId,
                @Nullable TableAssignment tableAssignment,
                @Nullable Long partitionId) {
            this.tableId = tableId;
            this.tableAssignment = tableAssignment;
            this.partitionId = partitionId;
        }
    }
}
