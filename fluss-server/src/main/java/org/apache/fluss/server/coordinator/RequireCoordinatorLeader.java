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

package org.apache.fluss.server.coordinator;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to mark RPC methods that require the coordinator server to be the current leader
 * before execution.
 *
 * <p>When a method annotated with {@code @RequireCoordinatorLeader} is invoked, it will first check
 * whether this coordinator server is the current leader via {@link
 * CoordinatorLeaderElection#isLeader()}. If the server is not the leader, a {@link
 * org.apache.fluss.exception.NotCoordinatorLeaderException} will be thrown immediately, and the
 * actual method body will not be executed.
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * @RequireCoordinatorLeader
 * public CompletableFuture<CreateTableResponse> createTable(CreateTableRequest request) {
 *     // method body only executes when this server is the coordinator leader
 * }
 * }</pre>
 */
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RequireCoordinatorLeader {}
