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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.rpc.protocol.Errors;

import java.util.EnumMap;
import java.util.Map;

/**
 * Centralised translation between Fluss and Kafka error codes. Used by the Produce and Fetch
 * transcoders to preserve actionable error information on the wire. See {@code
 * dev-docs/design/notes-kafka-error-mapping.md} for the full rationale per mapping.
 */
@Internal
public final class KafkaErrors {

    private KafkaErrors() {}

    private static final Map<Errors, org.apache.kafka.common.protocol.Errors> TABLE;

    static {
        TABLE = new EnumMap<>(Errors.class);
        TABLE.put(Errors.NONE, org.apache.kafka.common.protocol.Errors.NONE);
        TABLE.put(
                Errors.UNKNOWN_SERVER_ERROR,
                org.apache.kafka.common.protocol.Errors.UNKNOWN_SERVER_ERROR);

        // Fluss codes with a direct Kafka analogue.
        TABLE.put(
                Errors.NETWORK_EXCEPTION,
                org.apache.kafka.common.protocol.Errors.NETWORK_EXCEPTION);
        TABLE.put(
                Errors.UNSUPPORTED_VERSION,
                org.apache.kafka.common.protocol.Errors.UNSUPPORTED_VERSION);
        TABLE.put(Errors.CORRUPT_MESSAGE, org.apache.kafka.common.protocol.Errors.CORRUPT_MESSAGE);
        TABLE.put(
                Errors.NOT_LEADER_OR_FOLLOWER,
                org.apache.kafka.common.protocol.Errors.NOT_LEADER_OR_FOLLOWER);
        TABLE.put(
                Errors.RECORD_TOO_LARGE_EXCEPTION,
                org.apache.kafka.common.protocol.Errors.MESSAGE_TOO_LARGE);
        TABLE.put(
                Errors.CORRUPT_RECORD_EXCEPTION,
                org.apache.kafka.common.protocol.Errors.CORRUPT_MESSAGE);
        TABLE.put(
                Errors.INVALID_REQUIRED_ACKS,
                org.apache.kafka.common.protocol.Errors.INVALID_REQUIRED_ACKS);
        TABLE.put(
                Errors.LOG_OFFSET_OUT_OF_RANGE_EXCEPTION,
                org.apache.kafka.common.protocol.Errors.OFFSET_OUT_OF_RANGE);
        TABLE.put(
                Errors.REQUEST_TIME_OUT, org.apache.kafka.common.protocol.Errors.REQUEST_TIMED_OUT);
        TABLE.put(
                Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND_EXCEPTION,
                org.apache.kafka.common.protocol.Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND);
        TABLE.put(
                Errors.NOT_ENOUGH_REPLICAS_EXCEPTION,
                org.apache.kafka.common.protocol.Errors.NOT_ENOUGH_REPLICAS);
        TABLE.put(
                Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION,
                org.apache.kafka.common.protocol.Errors.OUT_OF_ORDER_SEQUENCE_NUMBER);
        TABLE.put(
                Errors.DUPLICATE_SEQUENCE_EXCEPTION,
                org.apache.kafka.common.protocol.Errors.DUPLICATE_SEQUENCE_NUMBER);
        TABLE.put(
                Errors.UNKNOWN_WRITER_ID_EXCEPTION,
                org.apache.kafka.common.protocol.Errors.UNKNOWN_PRODUCER_ID);
        TABLE.put(
                Errors.INVALID_PRODUCER_ID_EXCEPTION,
                org.apache.kafka.common.protocol.Errors.INVALID_PRODUCER_EPOCH);
        TABLE.put(
                Errors.INVALID_TIMESTAMP_EXCEPTION,
                org.apache.kafka.common.protocol.Errors.INVALID_TIMESTAMP);
        TABLE.put(
                Errors.LEADER_NOT_AVAILABLE_EXCEPTION,
                org.apache.kafka.common.protocol.Errors.LEADER_NOT_AVAILABLE);
        TABLE.put(
                Errors.FENCED_LEADER_EPOCH_EXCEPTION,
                org.apache.kafka.common.protocol.Errors.FENCED_LEADER_EPOCH);
        TABLE.put(
                Errors.AUTHENTICATE_EXCEPTION,
                org.apache.kafka.common.protocol.Errors.SASL_AUTHENTICATION_FAILED);
        TABLE.put(
                Errors.AUTHORIZATION_EXCEPTION,
                org.apache.kafka.common.protocol.Errors.CLUSTER_AUTHORIZATION_FAILED);
        TABLE.put(
                Errors.TABLE_NOT_EXIST,
                org.apache.kafka.common.protocol.Errors.UNKNOWN_TOPIC_OR_PARTITION);
        TABLE.put(
                Errors.UNKNOWN_TABLE_OR_BUCKET_EXCEPTION,
                org.apache.kafka.common.protocol.Errors.UNKNOWN_TOPIC_OR_PARTITION);
        TABLE.put(
                Errors.PARTITION_NOT_EXISTS,
                org.apache.kafka.common.protocol.Errors.UNKNOWN_TOPIC_OR_PARTITION);
        TABLE.put(
                Errors.INVALID_COLUMN_PROJECTION,
                org.apache.kafka.common.protocol.Errors.INVALID_REQUEST);
        TABLE.put(
                Errors.INVALID_TARGET_COLUMN,
                org.apache.kafka.common.protocol.Errors.INVALID_REQUEST);
        TABLE.put(
                Errors.NOT_COORDINATOR_LEADER_EXCEPTION,
                org.apache.kafka.common.protocol.Errors.NOT_COORDINATOR);
    }

    /**
     * Map a Fluss numeric error code to the closest Kafka {@link
     * org.apache.kafka.common.protocol.Errors}. Unknown codes map to {@code UNKNOWN_SERVER_ERROR}.
     */
    public static org.apache.kafka.common.protocol.Errors toKafka(int flussErrorCode) {
        Errors flussError = Errors.forCode(flussErrorCode);
        return toKafka(flussError);
    }

    /**
     * Map a Fluss error enum to the closest Kafka {@link org.apache.kafka.common.protocol.Errors}.
     */
    public static org.apache.kafka.common.protocol.Errors toKafka(Errors flussError) {
        return TABLE.getOrDefault(
                flussError, org.apache.kafka.common.protocol.Errors.UNKNOWN_SERVER_ERROR);
    }

    /**
     * Map a {@link Throwable} to the closest Kafka error. Follows the same
     * unwrap-and-walk-superclass logic as {@link Errors#forException(Throwable)}.
     */
    public static org.apache.kafka.common.protocol.Errors toKafka(Throwable cause) {
        if (cause == null) {
            return org.apache.kafka.common.protocol.Errors.NONE;
        }
        return toKafka(Errors.forException(cause));
    }
}
