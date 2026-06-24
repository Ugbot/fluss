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

package org.apache.fluss.kafka.sr;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.security.acl.FlussPrincipal;

/**
 * Per-request principal carrier shared between {@link SchemaRegistryHttpHandler} and {@link
 * SchemaRegistryService}. Implemented as a {@link ThreadLocal} because the Schema Registry runs on
 * a small Netty event-loop group where every request is handled synchronously on one thread
 * (blocking catalog calls and HTTP response serialisation); the handler sets the principal before
 * dispatch and clears it in {@code finally}, so the lifetime of the stored value exactly matches
 * the lifetime of the request on that thread.
 *
 * <p>A {@code ChannelHandlerContext.channel().attr(...)} approach would also work but would require
 * threading the context through {@link SchemaRegistryService}, which otherwise has no Netty
 * dependency. The ThreadLocal is intentional.
 */
@Internal
final class SchemaRegistryCallContext {

    private static final ThreadLocal<FlussPrincipal> CURRENT = new ThreadLocal<>();

    private SchemaRegistryCallContext() {}

    static void set(FlussPrincipal principal) {
        CURRENT.set(principal);
    }

    static void clear() {
        CURRENT.remove();
    }

    /** Current request's principal, or {@code null} if no principal was extracted. */
    static FlussPrincipal current() {
        return CURRENT.get();
    }
}
