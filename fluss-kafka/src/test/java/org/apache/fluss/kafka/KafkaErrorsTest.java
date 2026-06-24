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

import org.apache.fluss.exception.CorruptMessageException;
import org.apache.fluss.exception.NotLeaderOrFollowerException;
import org.apache.fluss.exception.RecordTooLargeException;
import org.apache.fluss.rpc.protocol.Errors;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link KafkaErrors}. */
class KafkaErrorsTest {

    @Test
    void noneRoundTripsToKafkaNone() {
        assertThat(KafkaErrors.toKafka(Errors.NONE))
                .isEqualTo(org.apache.kafka.common.protocol.Errors.NONE);
    }

    @Test
    void directlyMappedProduceCodes() {
        assertThat(KafkaErrors.toKafka(Errors.NOT_LEADER_OR_FOLLOWER))
                .isEqualTo(org.apache.kafka.common.protocol.Errors.NOT_LEADER_OR_FOLLOWER);
        assertThat(KafkaErrors.toKafka(Errors.RECORD_TOO_LARGE_EXCEPTION))
                .isEqualTo(org.apache.kafka.common.protocol.Errors.MESSAGE_TOO_LARGE);
        assertThat(KafkaErrors.toKafka(Errors.DUPLICATE_SEQUENCE_EXCEPTION))
                .isEqualTo(org.apache.kafka.common.protocol.Errors.DUPLICATE_SEQUENCE_NUMBER);
        assertThat(KafkaErrors.toKafka(Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION))
                .isEqualTo(org.apache.kafka.common.protocol.Errors.OUT_OF_ORDER_SEQUENCE_NUMBER);
    }

    @Test
    void fetchOffsetOutOfRangeMapped() {
        assertThat(KafkaErrors.toKafka(Errors.LOG_OFFSET_OUT_OF_RANGE_EXCEPTION))
                .isEqualTo(org.apache.kafka.common.protocol.Errors.OFFSET_OUT_OF_RANGE);
    }

    @Test
    void unknownTopicFamilyMapped() {
        assertThat(KafkaErrors.toKafka(Errors.TABLE_NOT_EXIST))
                .isEqualTo(org.apache.kafka.common.protocol.Errors.UNKNOWN_TOPIC_OR_PARTITION);
        assertThat(KafkaErrors.toKafka(Errors.UNKNOWN_TABLE_OR_BUCKET_EXCEPTION))
                .isEqualTo(org.apache.kafka.common.protocol.Errors.UNKNOWN_TOPIC_OR_PARTITION);
    }

    @Test
    void unmappedFlussErrorFallsBackToUnknownServerError() {
        // SCHEMA_NOT_EXIST is a Fluss-specific error with no Kafka equivalent.
        assertThat(KafkaErrors.toKafka(Errors.SCHEMA_NOT_EXIST))
                .isEqualTo(org.apache.kafka.common.protocol.Errors.UNKNOWN_SERVER_ERROR);
    }

    @Test
    void throwableMappingUnwrapsCompletionException() {
        Throwable wrapped = new CompletionException(new NotLeaderOrFollowerException("not leader"));
        assertThat(KafkaErrors.toKafka(wrapped))
                .isEqualTo(org.apache.kafka.common.protocol.Errors.NOT_LEADER_OR_FOLLOWER);
    }

    @Test
    void throwableMappingWalksExceptionHierarchy() {
        assertThat(KafkaErrors.toKafka(new CorruptMessageException("bad crc")))
                .isEqualTo(org.apache.kafka.common.protocol.Errors.CORRUPT_MESSAGE);
        assertThat(KafkaErrors.toKafka(new RecordTooLargeException("too big")))
                .isEqualTo(org.apache.kafka.common.protocol.Errors.MESSAGE_TOO_LARGE);
    }

    @Test
    void nullThrowableReturnsNone() {
        assertThat(KafkaErrors.toKafka((Throwable) null))
                .isEqualTo(org.apache.kafka.common.protocol.Errors.NONE);
    }

    @Test
    void numericCodeMapping() {
        assertThat(KafkaErrors.toKafka(Errors.NOT_LEADER_OR_FOLLOWER.code()))
                .isEqualTo(org.apache.kafka.common.protocol.Errors.NOT_LEADER_OR_FOLLOWER);
    }
}
