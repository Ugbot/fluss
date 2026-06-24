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

package org.apache.fluss.catalog.entities;

import org.apache.fluss.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * One row in {@code _catalog.__kafka_producer_ids__} (design 0016 §6). Persists each producer-id
 * allocation made by the transaction coordinator's monotonic counter so that on coordinator-leader
 * failover the new leader can rehydrate its high-water mark with one PK scan.
 *
 * <p>Each row records (a) the allocated {@code producerId} (the PK), (b) the {@code
 * transactionalId} the id was bound to, or {@code null} for an idempotent-only allocation, (c) the
 * {@code epoch} carried at allocation time, and (d) the wall-clock allocation timestamp.
 */
@PublicEvolving
public final class KafkaProducerIdEntity {

    private final long producerId;
    private final @Nullable String transactionalId;
    private final short epoch;
    private final long allocatedAtMillis;

    public KafkaProducerIdEntity(
            long producerId,
            @Nullable String transactionalId,
            short epoch,
            long allocatedAtMillis) {
        this.producerId = producerId;
        this.transactionalId = transactionalId;
        this.epoch = epoch;
        this.allocatedAtMillis = allocatedAtMillis;
    }

    public long producerId() {
        return producerId;
    }

    @Nullable
    public String transactionalId() {
        return transactionalId;
    }

    public short epoch() {
        return epoch;
    }

    public long allocatedAtMillis() {
        return allocatedAtMillis;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KafkaProducerIdEntity)) {
            return false;
        }
        KafkaProducerIdEntity that = (KafkaProducerIdEntity) o;
        return producerId == that.producerId
                && epoch == that.epoch
                && allocatedAtMillis == that.allocatedAtMillis
                && Objects.equals(transactionalId, that.transactionalId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(producerId, transactionalId, epoch, allocatedAtMillis);
    }

    @Override
    public String toString() {
        return "KafkaProducerIdEntity{"
                + "producerId="
                + producerId
                + ", transactionalId='"
                + transactionalId
                + '\''
                + ", epoch="
                + epoch
                + ", allocatedAtMillis="
                + allocatedAtMillis
                + '}';
    }
}
