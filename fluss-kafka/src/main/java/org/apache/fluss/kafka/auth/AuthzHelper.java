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

package org.apache.fluss.kafka.auth;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.kafka.KafkaRequest;
import org.apache.fluss.rpc.netty.server.Session;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.server.authorizer.Authorizer;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import javax.annotation.Nullable;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Adapts Fluss's {@link Authorizer} to the Kafka request pipeline.
 *
 * <p>Kafka handlers call {@link #authorizeTopicBatch} for per-topic APIs (Produce / Fetch /
 * DeleteRecords / ListOffsets / OffsetForLeaderEpoch / DescribeConfigs / AlterConfigs /
 * IncrementalAlterConfigs) to get the allowed subset, then zero out responses for denied topics
 * with {@code TOPIC_AUTHORIZATION_FAILED}. Broker-level gates (CreateTopics on a {@code DATABASE},
 * DescribeCluster on {@code CLUSTER}) use {@link #authorizeOrThrow} which raises a top-level error.
 *
 * <p>All methods no-op (return "allow") when the provided {@link Authorizer} is {@code null}. That
 * is the contract for unauthenticated development setups and the testing gateway path; it keeps the
 * existing test suite green while SASL-enabled clusters enforce ACLs.
 */
@Internal
public final class AuthzHelper {

    /**
     * Loopback fallback when a Netty channel's remote address is not an {@link InetSocketAddress}.
     */
    private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();

    private AuthzHelper() {}

    /**
     * Build a {@link Session} from the authenticated principal + remote address attached to the
     * request. Safe to call even on PLAINTEXT listeners: the principal will be {@link
     * FlussPrincipal#ANONYMOUS}, which maps to the same "default" ACL semantics the authorizer uses
     * for unauthenticated RPCs.
     */
    public static Session sessionOf(KafkaRequest request) {
        FlussPrincipal principal = request.principal();
        InetAddress remote = remoteAddressOf(request.ctx());
        return new Session(
                request.apiVersion(),
                request.listenerName(),
                // Kafka listeners are never the internal listener; they're always client-facing.
                false,
                remote,
                principal);
    }

    private static InetAddress remoteAddressOf(ChannelHandlerContext ctx) {
        if (ctx == null || ctx.channel() == null) {
            return LOOPBACK;
        }
        SocketAddress addr = ctx.channel().remoteAddress();
        if (addr instanceof InetSocketAddress) {
            InetAddress ia = ((InetSocketAddress) addr).getAddress();
            return ia != null ? ia : LOOPBACK;
        }
        return LOOPBACK;
    }

    /**
     * Filter a batch of Kafka topic names by {@code operationType} on their resolved {@code
     * TABLE(kafkaDatabase, topic)} resources. Preserves input order so the caller's response list
     * lines up with the request.
     *
     * <p>When {@code authorizer == null} every topic is allowed (see class Javadoc).
     *
     * @return a map from topic name to {@code true} (allowed) or {@code false} (denied).
     */
    public static Map<String, Boolean> authorizeTopicBatch(
            @Nullable Authorizer authorizer,
            Session session,
            OperationType operationType,
            Collection<String> topics,
            String kafkaDatabase) {
        Map<String, Boolean> out = new LinkedHashMap<>(topics.size());
        if (authorizer == null) {
            for (String t : topics) {
                out.put(t, true);
            }
            return out;
        }
        for (String t : topics) {
            try {
                boolean ok =
                        authorizer.isAuthorized(
                                session, operationType, Resource.table(kafkaDatabase, t));
                out.put(t, ok);
            } catch (Throwable ignore) {
                // A broken ACL check must deny, not succeed; surfacing as denied gives the
                // client a clean TOPIC_AUTHORIZATION_FAILED rather than UNKNOWN_SERVER_ERROR.
                out.put(t, false);
            }
        }
        return out;
    }

    /**
     * Strict form for broker-scoped calls (CreateTopics, DescribeCluster, group-level ops). Throws
     * an {@link AuthorizationException} the handler can translate to the appropriate Kafka error
     * (CLUSTER_AUTHORIZATION_FAILED / GROUP_AUTHORIZATION_FAILED etc.).
     *
     * <p>When {@code authorizer == null} this is a no-op.
     */
    public static void authorizeOrThrow(
            @Nullable Authorizer authorizer,
            Session session,
            OperationType operationType,
            Resource resource)
            throws AuthorizationException {
        if (authorizer == null) {
            return;
        }
        authorizer.authorize(session, operationType, resource);
    }
}
