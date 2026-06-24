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

package org.apache.fluss.rpc.netty.server;

import org.apache.fluss.rpc.protocol.RequestType;

/**
 * Handles a specific type of RPC request.
 *
 * @param <T> the type of the RPC request that is handled by this handler
 */
public interface RequestHandler<T extends RpcRequest> extends AutoCloseable {

    /** Returns the type of the RPC requests that is handled by this handler. */
    RequestType requestType();

    /** Processes the RPC request. */
    void processRequest(T request);

    /**
     * Release any resources held by this handler. Called by the {@link RequestProcessorPool} during
     * server shutdown, after all in-flight requests have drained. The default implementation is a
     * no-op; protocol plugins holding resources (Fluss client Connections, etc.) must override.
     */
    @Override
    default void close() throws Exception {}
}
