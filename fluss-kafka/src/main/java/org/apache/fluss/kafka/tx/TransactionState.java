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

/**
 * Per-{@code transactional.id} lifecycle state. Mirrors Kafka's own state machine — see design 0016
 * §4.
 *
 * <p>Phase J.1 only writes {@link #EMPTY}; the other transitions ship with the five txn APIs in
 * J.2. The enum is in place now so the J.1 coordinator scaffolding doesn't carry a string-typed
 * placeholder.
 */
@Internal
public enum TransactionState {
    EMPTY(KafkaTxnStateEntity.STATE_EMPTY),
    ONGOING(KafkaTxnStateEntity.STATE_ONGOING),
    PREPARE_COMMIT(KafkaTxnStateEntity.STATE_PREPARE_COMMIT),
    PREPARE_ABORT(KafkaTxnStateEntity.STATE_PREPARE_ABORT),
    COMPLETE_COMMIT(KafkaTxnStateEntity.STATE_COMPLETE_COMMIT),
    COMPLETE_ABORT(KafkaTxnStateEntity.STATE_COMPLETE_ABORT);

    private final String wireName;

    TransactionState(String wireName) {
        this.wireName = wireName;
    }

    /** The exact string persisted to {@code __kafka_txn_state__.state}. */
    public String wireName() {
        return wireName;
    }

    /**
     * Decode a wire-form state name back to the enum. Throws {@link IllegalArgumentException} for
     * an unknown value — guarded against silent corruption since every persisted row is one we
     * wrote.
     */
    public static TransactionState fromWireName(String name) {
        for (TransactionState s : values()) {
            if (s.wireName.equals(name)) {
                return s;
            }
        }
        throw new IllegalArgumentException("Unknown TransactionState wire name: " + name);
    }
}
