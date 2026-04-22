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

package org.apache.fluss.rpc.log;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.metadata.TableBucket;

import java.util.Collections;
import java.util.Map;

/**
 * Stable, immutable fetch descriptor for client-style (non-follower) reads. The Kafka protocol
 * bolt-on builds one of these per {@code Fetch} request and hands it to {@code
 * ReplicaManager#fetchLogRecordsForClient} — the internal {@code FetchParams} / {@code
 * FetchReqInfo} structs stay out of the bolt-on's sight.
 *
 * <p>Follower fetches keep using the lower-level entry point; this shape is deliberately missing
 * replica-id / isolation-level fields because client reads always use {@code READ_COMMITTED} and a
 * negative replica id.
 */
@PublicEvolving
public final class ClientFetchRequest {

    private final int maxFetchBytes;
    private final Map<TableBucket, BucketRead> buckets;

    public ClientFetchRequest(int maxFetchBytes, Map<TableBucket, BucketRead> buckets) {
        this.maxFetchBytes = maxFetchBytes;
        this.buckets = Collections.unmodifiableMap(buckets);
    }

    public int maxFetchBytes() {
        return maxFetchBytes;
    }

    public Map<TableBucket, BucketRead> buckets() {
        return buckets;
    }

    /** Per-bucket fetch parameters. */
    public static final class BucketRead {
        private final long tableId;
        private final long fetchOffset;
        private final int maxBytes;

        public BucketRead(long tableId, long fetchOffset, int maxBytes) {
            this.tableId = tableId;
            this.fetchOffset = fetchOffset;
            this.maxBytes = maxBytes;
        }

        public long tableId() {
            return tableId;
        }

        public long fetchOffset() {
            return fetchOffset;
        }

        public int maxBytes() {
            return maxBytes;
        }
    }
}
