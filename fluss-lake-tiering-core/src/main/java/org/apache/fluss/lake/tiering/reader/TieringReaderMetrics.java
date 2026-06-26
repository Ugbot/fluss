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

package org.apache.fluss.lake.tiering.reader;

import java.util.concurrent.atomic.LongAdder;

/**
 * A minimal, engine-agnostic metrics holder for the tiering reader.
 *
 * <p>In the Flink connector the reader records read-throughput against a {@code
 * SourceReaderMetricGroup}. The engine-agnostic core has no such operator runtime, so this plain
 * counter holder replaces it. The daemon can read the accumulated counter via {@link
 * #getNumBytesRead()} and forward it to whatever metric system it owns.
 */
public class TieringReaderMetrics {

    private final LongAdder numBytesRead = new LongAdder();

    /** Records the number of bytes read while tiering. */
    public void recordBytesRead(long bytes) {
        numBytesRead.add(bytes);
    }

    /** Returns the total number of bytes read so far. */
    public long getNumBytesRead() {
        return numBytesRead.sum();
    }
}
