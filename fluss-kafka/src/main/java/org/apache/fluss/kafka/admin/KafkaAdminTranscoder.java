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

package org.apache.fluss.kafka.admin;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.DatabaseNotExistException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.kafka.KafkaServerContext;
import org.apache.fluss.kafka.catalog.KafkaTableFactory;
import org.apache.fluss.kafka.catalog.KafkaTopicInfo;
import org.apache.fluss.kafka.catalog.KafkaTopicsCatalog;
import org.apache.fluss.kafka.metadata.KafkaTopicMapping;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.CreateTableRequest;
import org.apache.fluss.rpc.messages.DropTableRequest;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfig;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsRequestData.DeleteTopicState;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResult;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Translates Kafka admin requests into Fluss coordinator RPCs and catalog mutations. */
@Internal
public final class KafkaAdminTranscoder {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaAdminTranscoder.class);

    private final KafkaServerContext context;
    private final KafkaTopicsCatalog catalog;

    public KafkaAdminTranscoder(KafkaServerContext context, KafkaTopicsCatalog catalog) {
        this.context = context;
        this.catalog = catalog;
    }

    public CreateTopicsResponseData createTopics(CreateTopicsRequestData request) {
        CreateTopicsResponseData response = new CreateTopicsResponseData();
        response.setThrottleTimeMs(0);

        boolean validateOnly = request.validateOnly();
        for (CreatableTopic topic : request.topics()) {
            response.topics().add(createOne(topic, validateOnly));
        }
        return response;
    }

    public DeleteTopicsResponseData deleteTopics(DeleteTopicsRequestData request) {
        DeleteTopicsResponseData response = new DeleteTopicsResponseData();
        response.setThrottleTimeMs(0);

        if (request.topicNames() != null && !request.topicNames().isEmpty()) {
            for (String name : request.topicNames()) {
                response.responses().add(deleteOne(name));
            }
        }
        if (request.topics() != null) {
            for (DeleteTopicState topic : request.topics()) {
                if (topic.name() != null) {
                    response.responses().add(deleteOne(topic.name()));
                } else {
                    response.responses()
                            .add(
                                    new DeletableTopicResult()
                                            .setName(null)
                                            .setTopicId(
                                                    topic.topicId() != null
                                                            ? topic.topicId()
                                                            : Uuid.ZERO_UUID)
                                            .setErrorCode(Errors.UNSUPPORTED_VERSION.code())
                                            .setErrorMessage(
                                                    "Delete-by-topic-id is not supported in this"
                                                            + " Fluss Kafka protocol phase."));
                }
            }
        }
        return response;
    }

    private CreatableTopicResult createOne(CreatableTopic topic, boolean validateOnly) {
        CreatableTopicResult result =
                new CreatableTopicResult().setName(topic.name()).setTopicId(Uuid.ZERO_UUID);

        if (!KafkaTopicMapping.isValidAutoCreateTopic(topic.name())) {
            return result.setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code())
                    .setErrorMessage(
                            "Topic name '"
                                    + topic.name()
                                    + "' is not a valid auto-created Kafka topic "
                                    + "(alphanumerics, '-' and '_' only; no '.' allowed).");
        }

        int numPartitions = resolvePartitionCount(topic);
        if (numPartitions <= 0) {
            return result.setErrorCode(Errors.INVALID_PARTITIONS.code())
                    .setErrorMessage("numPartitions must be greater than 0.");
        }

        TablePath tablePath = new TablePath(context.kafkaDatabase(), topic.name());

        // If a Kafka binding already exists, reject cleanly. If a *non-Kafka* Fluss table exists at
        // this path, also reject - we don't silently adopt tables created by other clients.
        Optional<KafkaTopicInfo> existing;
        try {
            existing = catalog.lookup(topic.name());
        } catch (Exception e) {
            LOG.error("Catalog lookup failed for '{}'", topic.name(), e);
            return result.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                    .setErrorMessage("Catalog lookup failed: " + e.getMessage());
        }
        if (existing.isPresent()) {
            return result.setErrorCode(Errors.TOPIC_ALREADY_EXISTS.code())
                    .setErrorMessage("Topic '" + topic.name() + "' already exists.")
                    .setTopicId(existing.get().topicId());
        }
        if (clashesWithNonKafkaTable(tablePath)) {
            return result.setErrorCode(Errors.TOPIC_ALREADY_EXISTS.code())
                    .setErrorMessage(
                            "Fluss table "
                                    + tablePath
                                    + " exists but was not created via Kafka CreateTopics; "
                                    + "refusing to adopt it.");
        }

        KafkaTopicInfo.TimestampType timestampType =
                KafkaTopicInfo.TimestampType.fromWireName(
                        configOr(topic.configs(), "message.timestamp.type", "CreateTime"));
        KafkaTopicInfo.Compression compression =
                KafkaTopicInfo.Compression.fromWireName(
                        configOr(topic.configs(), "compression.type", "NONE"));
        String cleanupPolicy = configOr(topic.configs(), "cleanup.policy", "delete");
        boolean compacted = "compact".equalsIgnoreCase(cleanupPolicy);
        Uuid topicId = Uuid.randomUuid();

        TableDescriptor descriptor =
                KafkaTableFactory.buildDescriptor(
                        topic.name(),
                        numPartitions,
                        timestampType,
                        compression,
                        topicId,
                        compacted);

        if (validateOnly) {
            return result.setErrorCode(Errors.NONE.code())
                    .setNumPartitions(numPartitions)
                    .setReplicationFactor(replicationFactor(topic))
                    .setTopicId(topicId);
        }

        CreateTableRequest rpc = new CreateTableRequest();
        rpc.setTableJson(descriptor.toJsonBytes()).setIgnoreIfExists(false);
        rpc.setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());

        try {
            invoke(context.coordinatorGateway().createTable(rpc));
            long tableId = resolveTableId(tablePath);
            catalog.register(
                    new KafkaTopicInfo(
                            topic.name(), tablePath, tableId, timestampType, compression, topicId));
            return result.setErrorCode(Errors.NONE.code())
                    .setNumPartitions(numPartitions)
                    .setReplicationFactor(replicationFactor(topic))
                    .setTopicId(topicId);
        } catch (TableAlreadyExistException already) {
            return result.setErrorCode(Errors.TOPIC_ALREADY_EXISTS.code())
                    .setErrorMessage(already.getMessage());
        } catch (DatabaseNotExistException noDb) {
            return result.setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code())
                    .setErrorMessage(
                            "Fluss database '"
                                    + context.kafkaDatabase()
                                    + "' does not exist; coordinator must have been started with"
                                    + " kafka.enabled=true.");
        } catch (InvalidTableException invalid) {
            return result.setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage(invalid.getMessage());
        } catch (Exception e) {
            LOG.error("CreateTopics failed for '{}'", topic.name(), e);
            return result.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                    .setErrorMessage(e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private DeletableTopicResult deleteOne(String topicName) {
        DeletableTopicResult result =
                new DeletableTopicResult().setName(topicName).setTopicId(Uuid.ZERO_UUID);
        if (topicName == null || topicName.isEmpty()) {
            return result.setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code())
                    .setErrorMessage("Topic name is null or empty.");
        }

        Optional<KafkaTopicInfo> existing;
        try {
            existing = catalog.lookup(topicName);
        } catch (Exception e) {
            LOG.error("Catalog lookup failed for '{}'", topicName, e);
            return result.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                    .setErrorMessage(e.getMessage());
        }
        if (!existing.isPresent()) {
            return result.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                    .setErrorMessage("Topic '" + topicName + "' does not exist.");
        }
        KafkaTopicInfo info = existing.get();
        result.setTopicId(info.topicId());

        try {
            catalog.deregister(topicName);
        } catch (Exception e) {
            LOG.error("Catalog deregister failed for '{}'", topicName, e);
            return result.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                    .setErrorMessage(e.getMessage());
        }

        DropTableRequest rpc = new DropTableRequest().setIgnoreIfNotExists(false);
        rpc.setTablePath()
                .setDatabaseName(info.dataTablePath().getDatabaseName())
                .setTableName(info.dataTablePath().getTableName());
        try {
            invoke(context.coordinatorGateway().dropTable(rpc));
            return result.setErrorCode(Errors.NONE.code());
        } catch (Exception e) {
            LOG.error("DeleteTopics failed for '{}'", topicName, e);
            return result.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                    .setErrorMessage(e.getMessage());
        }
    }

    private boolean clashesWithNonKafkaTable(TablePath tablePath) {
        try {
            context.metadataManager().getTable(tablePath);
            // Table exists but catalog.lookup() already returned empty -> not Kafka-managed.
            return true;
        } catch (org.apache.fluss.exception.TableNotExistException notFound) {
            return false;
        } catch (Exception e) {
            LOG.warn("Failed to probe existing table {} before CreateTopics", tablePath, e);
            return false;
        }
    }

    private long resolveTableId(TablePath tablePath) {
        try {
            return context.metadataManager().getTable(tablePath).getTableId();
        } catch (Exception e) {
            LOG.warn("Could not resolve Fluss tableId for {}", tablePath, e);
            return -1L;
        }
    }

    private static int resolvePartitionCount(CreatableTopic topic) {
        if (topic.numPartitions() > 0) {
            return topic.numPartitions();
        }
        return 1;
    }

    private static short replicationFactor(CreatableTopic topic) {
        return topic.replicationFactor() > 0 ? topic.replicationFactor() : (short) 1;
    }

    private static String configOr(Iterable<CreatableTopicConfig> configs, String key, String def) {
        if (configs == null) {
            return def;
        }
        for (CreatableTopicConfig cfg : configs) {
            if (Objects.equals(cfg.name(), key) && cfg.value() != null) {
                return cfg.value();
            }
        }
        return def;
    }

    private static <T> T invoke(java.util.concurrent.CompletableFuture<T> future) throws Exception {
        try {
            return future.get(30, TimeUnit.SECONDS);
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw new CompletionException(cause);
        } catch (TimeoutException timeout) {
            throw new RuntimeException("Coordinator RPC timed out", timeout);
        }
    }
}
