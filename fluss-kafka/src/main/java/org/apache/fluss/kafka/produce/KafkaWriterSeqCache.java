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

package org.apache.fluss.kafka.produce;

import org.apache.fluss.annotation.Internal;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Translates Kafka per-record producer sequences into Fluss per-batch sequences.
 *
 * <p>Kafka sequences increment by {@code numRecords} per batch; Fluss's {@code WriterAppendInfo}
 * validates {@code nextBatchSeq == lastBatchSeq + 1}. To bridge this gap the transcoder assigns its
 * own monotonic counter (0, 1, 2, …) per {@code (producerId, tableId, partitionIdx)} triple and
 * uses that as the Fluss {@code batchSequence}. When the Kafka batch's {@code baseSequence == 0}
 * the counter is reset, covering both initial produces and producer-epoch resets.
 *
 * <p>This cache is a singleton held by {@code KafkaServerContext} so it survives across individual
 * {@code PRODUCE} requests. On a server restart or leader failover the counter resets to zero;
 * {@code WriterAppendInfo.inSequence} handles that via its {@code nextBatchSeq == 0} branch.
 */
@Internal
public final class KafkaWriterSeqCache {

    // producerId -> (partKey -> counter)
    private final ConcurrentHashMap<Long, ConcurrentHashMap<Long, AtomicInteger>> seqs =
            new ConcurrentHashMap<>();

    /**
     * Returns the next Fluss batch sequence for the given producer / partition triple, then
     * increments it. When {@code kafkaBaseSeq == 0} (fresh producer or epoch reset) the counter is
     * reset to zero before returning 0.
     */
    public int nextSeq(long producerId, long tableId, int partitionIdx, int kafkaBaseSeq) {
        long partKey = tableId * 65536L + partitionIdx;
        ConcurrentHashMap<Long, AtomicInteger> partMap =
                seqs.computeIfAbsent(producerId, k -> new ConcurrentHashMap<>());
        AtomicInteger counter = partMap.computeIfAbsent(partKey, k -> new AtomicInteger(0));
        if (kafkaBaseSeq == 0) {
            counter.set(0);
        }
        return counter.getAndIncrement();
    }
}
