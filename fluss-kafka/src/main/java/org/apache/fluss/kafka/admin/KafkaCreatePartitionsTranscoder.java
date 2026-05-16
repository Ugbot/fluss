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
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.kafka.catalog.KafkaTopicInfo;
import org.apache.fluss.kafka.catalog.KafkaTopicsCatalog;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.server.coordinator.MetadataManager;

import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.protocol.Errors;

import java.util.Optional;

/**
 * Handles Kafka {@code CREATE_PARTITIONS} (API key 37).
 *
 * <p>Fluss tables have a fixed bucket count decided at {@code CREATE_TOPIC} time — there is no
 * runtime primitive to grow them. The honest Kafka-compat answer is therefore:
 *
 * <ul>
 *   <li>{@code count == currentBuckets} → {@link Errors#NONE}. No-op, which matches Kafka's
 *       treatment of a redundant expansion request.
 *   <li>{@code count &gt; currentBuckets} → {@link Errors#INVALID_PARTITIONS} with a message
 *       explaining the constraint. The client gets a clear, actionable error instead of a raw
 *       {@code UNSUPPORTED_VERSION}.
 *   <li>{@code count &lt; currentBuckets} → {@link Errors#INVALID_PARTITIONS}. Kafka itself never
 *       allows shrinking, so the error is correct for free.
 *   <li>Unknown topic → {@link Errors#UNKNOWN_TOPIC_OR_PARTITION}.
 * </ul>
 *
 * <p>{@code validateOnly=true} requests take the same code path; we never perform the mutation.
 */
@Internal
public final class KafkaCreatePartitionsTranscoder {

    private final KafkaTopicsCatalog catalog;
    private final MetadataManager metadataManager;

    public KafkaCreatePartitionsTranscoder(
            KafkaTopicsCatalog catalog, MetadataManager metadataManager) {
        this.catalog = catalog;
        this.metadataManager = metadataManager;
    }

    public CreatePartitionsResponseData createPartitions(CreatePartitionsRequestData request) {
        CreatePartitionsResponseData response = new CreatePartitionsResponseData();
        response.setThrottleTimeMs(0);

        for (CreatePartitionsTopic topic : request.topics()) {
            response.results().add(resolve(topic));
        }
        return response;
    }

    private CreatePartitionsTopicResult resolve(CreatePartitionsTopic topic) {
        CreatePartitionsTopicResult result =
                new CreatePartitionsTopicResult().setName(topic.name());

        Optional<KafkaTopicInfo> info;
        try {
            info = catalog.lookup(topic.name());
        } catch (Exception e) {
            return result.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                    .setErrorMessage(e.getMessage());
        }
        if (!info.isPresent()) {
            return result.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                    .setErrorMessage("Unknown topic: " + topic.name());
        }

        TableInfo tableInfo;
        try {
            tableInfo = metadataManager.getTable(info.get().dataTablePath());
        } catch (TableNotExistException gone) {
            return result.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                    .setErrorMessage("Unknown topic: " + topic.name());
        } catch (Exception e) {
            return result.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                    .setErrorMessage(e.getMessage());
        }

        int currentBuckets = tableInfo.getNumBuckets();
        int requested = topic.count();
        if (requested == currentBuckets) {
            return result.setErrorCode(Errors.NONE.code());
        }
        String direction = requested > currentBuckets ? "grow" : "shrink";
        return result.setErrorCode(Errors.INVALID_PARTITIONS.code())
                .setErrorMessage(
                        "Cannot "
                                + direction
                                + " topic '"
                                + topic.name()
                                + "' from "
                                + currentBuckets
                                + " to "
                                + requested
                                + " partitions: Fluss tables have a fixed bucket count set at "
                                + "CreateTopic time; runtime expansion is not supported.");
    }
}
