/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.rpc.protocol;

import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.rpc.RpcGateway;
import com.alibaba.fluss.rpc.messages.GetTableRequest;
import com.alibaba.fluss.rpc.messages.GetTableResponse;
import com.alibaba.fluss.rpc.messages.GetTableSchemaRequest;
import com.alibaba.fluss.rpc.messages.GetTableSchemaResponse;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.alibaba.fluss.rpc.protocol.ApiManager.ID_TO_API;
import static com.alibaba.fluss.rpc.protocol.ApiManager.registerApiMethods;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link com.alibaba.fluss.rpc.protocol.ApiManager}. */
public class ApiManagerTest {

    @Test
    void testApiManager() {
        ApiManager coordinatorApi = new ApiManager(ServerType.COORDINATOR);
        ApiManager tabletserverApi = new ApiManager(ServerType.TABLET_SERVER);

        // both exists
        assertThat(coordinatorApi.getApi(ApiKeys.GET_TABLE.id)).isNotNull();
        assertThat(tabletserverApi.getApi(ApiKeys.GET_TABLE.id)).isNotNull();

        // coordinator only
        assertThat(coordinatorApi.getApi(ApiKeys.CREATE_TABLE.id)).isNotNull();
        assertThat(tabletserverApi.getApi(ApiKeys.CREATE_TABLE.id)).isNull();
    }

    @Test
    void testRegisteredRpcMethods() {
        assertThat(ApiManager.ID_TO_API.size()).isEqualTo(ApiKeys.values().length);
        assertThat(ApiManager.NAME_TO_API.size()).isEqualTo(ApiKeys.values().length);
        for (Map.Entry<Short, ApiMethod> entry : ID_TO_API.entrySet()) {
            assertThat(entry.getValue().getApiKey().id).isEqualTo(entry.getKey());
            // should be the same ApiMethod instance
            assertThat(ApiManager.NAME_TO_API.get(entry.getValue().getMethodName()))
                    .isEqualTo(entry.getValue());
        }
    }

    @Test
    void testInvalidRpcMethods() {
        assertThatThrownBy(
                        () ->
                                registerApiMethods(
                                        InvalidRpcGateway1.class,
                                        ServerType.COORDINATOR,
                                        new HashMap<>()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "RPC method [public abstract void "
                                + InvalidRpcGateway1.class.getName()
                                + ".testMethod()] must have exactly one parameter of type ApiMessage");

        assertThatThrownBy(
                        () ->
                                registerApiMethods(
                                        InvalidResponseRpcGateway.class,
                                        ServerType.COORDINATOR,
                                        new HashMap<>()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "RPC method [public abstract java.util.concurrent.CompletableFuture "
                                + InvalidResponseRpcGateway.class.getName()
                                + ".getTable("
                                + GetTableRequest.class.getName()
                                + ")] must have a return type of CompletableFuture<T extends ApiMessage>");

        assertThatThrownBy(
                        () ->
                                registerApiMethods(
                                        InvalidResponseNamingRpcGateway.class,
                                        ServerType.COORDINATOR,
                                        new HashMap<>()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "RPC method [getTable] expects to have a response return class "
                                + "of type GetTableResponse, but is GetTableSchemaResponse");

        assertThatThrownBy(
                        () ->
                                registerApiMethods(
                                        InvalidRequestNamingRpcGateway.class,
                                        ServerType.COORDINATOR,
                                        new HashMap<>()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "RPC method [getTable] expects to have a request parameter "
                                + "of type GetTableRequest, but is GetTableSchemaRequest");

        assertThatThrownBy(
                        () ->
                                registerApiMethods(
                                        InvalidDuplicatedRpcGateway.class,
                                        ServerType.COORDINATOR,
                                        new HashMap<>()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Different RPC methods are registered with the same API key [GET_TABLE(1007)]");
    }

    // --------------------------------------------------------------------------------------------
    interface InvalidRpcGateway1 extends RpcGateway {
        @RPC(api = ApiKeys.GET_TABLE)
        void testMethod();
    }

    interface InvalidResponseRpcGateway extends RpcGateway {
        @RPC(api = ApiKeys.GET_TABLE)
        CompletableFuture<?> getTable(GetTableRequest getTableRequest);
    }

    interface InvalidResponseNamingRpcGateway extends RpcGateway {
        @RPC(api = ApiKeys.GET_TABLE)
        CompletableFuture<GetTableSchemaResponse> getTable(GetTableRequest getTableRequest);
    }

    interface InvalidRequestNamingRpcGateway extends RpcGateway {
        @RPC(api = ApiKeys.GET_TABLE)
        CompletableFuture<GetTableResponse> getTable(GetTableSchemaRequest getTableRequest);
    }

    interface InvalidDuplicatedRpcGateway extends RpcGateway {
        @RPC(api = ApiKeys.GET_TABLE)
        CompletableFuture<GetTableResponse> getTable(GetTableRequest getTableRequest);

        @RPC(api = ApiKeys.GET_TABLE)
        CompletableFuture<GetTableSchemaResponse> getTableSchema(
                GetTableSchemaRequest getSchemaRequest);
    }
}
