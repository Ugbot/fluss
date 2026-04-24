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

package org.apache.fluss.kafka;

import org.apache.fluss.kafka.auth.KafkaListenerAuthConfig;
import org.apache.fluss.kafka.auth.KafkaSaslTranscoder;
import org.apache.fluss.kafka.metrics.KafkaMetricGroup;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.fluss.shaded.netty4.io.netty.handler.timeout.IdleState;
import org.apache.fluss.shaded.netty4.io.netty.handler.timeout.IdleStateEvent;
import org.apache.fluss.shaded.netty4.io.netty.util.ReferenceCountUtil;
import org.apache.fluss.utils.MathUtils;

import org.apache.kafka.common.errors.IllegalSaslStateException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestAndSize;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;
import static org.apache.kafka.common.protocol.ApiKeys.PRODUCE;
import static org.apache.kafka.common.protocol.ApiKeys.SASL_AUTHENTICATE;
import static org.apache.kafka.common.protocol.ApiKeys.SASL_HANDSHAKE;

/**
 * A decoder that decodes the incoming ByteBuf into Kafka requests and sends them to the
 * corresponding RequestChannel.
 *
 * <p>On a {@link KafkaListenerAuthConfig.Protocol#SASL_PLAINTEXT SASL listener}, the decoder also
 * drives the per-connection SASL state machine via {@link KafkaSaslTranscoder}: {@code
 * SASL_HANDSHAKE} / {@code SASL_AUTHENTICATE} frames are translated onto Fluss's {@link
 * org.apache.fluss.security.auth.ServerAuthenticator} SPI and responded to inline (never dispatched
 * to the handler). Any non-SASL, non-ApiVersions API received before the handshake completes is
 * answered with {@code ILLEGAL_SASL_STATE}.
 */
public class KafkaCommandDecoder extends SimpleChannelInboundHandler<ByteBuf> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaCommandDecoder.class);

    private final RequestChannel[] requestChannels;
    private final int numChannels;
    private final String listenerName;
    private final KafkaListenerAuthConfig authConfig;
    private final Supplier<KafkaMetricGroup> metricsSupplier;

    // Need to use a Queue to store the inflight responses, because Kafka clients require the
    // responses to be sent in order.
    // See: org.apache.kafka.clients.InFlightRequests#completeNext
    private final ConcurrentLinkedDeque<KafkaRequest> inflightResponses =
            new ConcurrentLinkedDeque<>();
    protected final AtomicBoolean isActive = new AtomicBoolean(true);
    protected volatile ChannelHandlerContext ctx;
    protected SocketAddress remoteAddress;
    private boolean connectionCounted = false;

    /**
     * SASL state machine for this connection. {@code null} on PLAINTEXT listeners; lazily created
     * on the first inbound frame when the listener requires SASL.
     */
    private KafkaSaslTranscoder saslTranscoder;

    public KafkaCommandDecoder(
            RequestChannel[] requestChannels,
            String listenerName,
            KafkaListenerAuthConfig authConfig) {
        this(requestChannels, listenerName, authConfig, () -> null);
    }

    public KafkaCommandDecoder(
            RequestChannel[] requestChannels,
            String listenerName,
            KafkaListenerAuthConfig authConfig,
            Supplier<KafkaMetricGroup> metricsSupplier) {
        super(false);
        this.requestChannels = requestChannels;
        this.numChannels = requestChannels.length;
        this.listenerName = listenerName;
        this.authConfig = checkNotNull(authConfig, "authConfig");
        this.metricsSupplier = checkNotNull(metricsSupplier, "metricsSupplier");
    }

    @javax.annotation.Nullable
    private KafkaMetricGroup metrics() {
        return metricsSupplier.get();
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, ByteBuf buffer) throws Exception {
        CompletableFuture<AbstractResponse> future = new CompletableFuture<>();
        boolean needRelease = false;
        try {
            KafkaRequest request = parseRequest(ctx, future, buffer, listenerName);
            ApiKeys apiKey = request.apiKey();

            if (authConfig.requiresSasl()) {
                if (saslTranscoder == null) {
                    saslTranscoder =
                            new KafkaSaslTranscoder(
                                    authConfig,
                                    listenerName,
                                    remoteAddress == null ? "unknown" : remoteAddress.toString());
                }

                if (apiKey == SASL_HANDSHAKE) {
                    SaslHandshakeRequest hs = request.request();
                    AbstractResponse resp = saslTranscoder.handleSaslHandshake(hs);
                    completeInline(request, resp);
                    needRelease = false;
                    return;
                }
                if (apiKey == SASL_AUTHENTICATE) {
                    SaslAuthenticateRequest ar = request.request();
                    AbstractResponse resp = saslTranscoder.handleSaslAuthenticate(ar);
                    KafkaMetricGroup m = metrics();
                    if (m != null) {
                        boolean ok =
                                resp
                                                instanceof
                                                org.apache.kafka.common.requests
                                                        .SaslAuthenticateResponse
                                        && ((org.apache.kafka.common.requests
                                                                        .SaslAuthenticateResponse)
                                                                resp)
                                                        .data()
                                                        .errorCode()
                                                == org.apache.kafka.common.protocol.Errors.NONE
                                                        .code();
                        m.onSaslOutcome(ok, saslTranscoder.mechanism());
                    }
                    completeInline(request, resp);
                    needRelease = false;
                    return;
                }
                if (apiKey != API_VERSIONS && saslTranscoder.isSaslRequiredButNotDone()) {
                    AbstractRequest aReq = request.request();
                    AbstractResponse err =
                            aReq.getErrorResponse(
                                    new IllegalSaslStateException(
                                            "Expected SASL handshake before API " + apiKey));
                    completeInline(request, err);
                    needRelease = false;
                    return;
                }
                request.setPrincipal(saslTranscoder.principal());
            } else {
                request.setPrincipal(FlussPrincipal.ANONYMOUS);
            }

            inflightResponses.addLast(request);
            future.whenCompleteAsync((r, t) -> sendResponse(ctx), ctx.executor());
            int channelIndex =
                    MathUtils.murmurHash(ctx.channel().id().asLongText().hashCode()) % numChannels;
            requestChannels[channelIndex].putRequest(request);

            if (!isActive.get()) {
                LOG.warn("Received a request on an inactive channel: {}", remoteAddress);
                request.fail(new LeaderNotAvailableException("Channel is inactive"));
                needRelease = true;
            }
        } catch (Throwable t) {
            needRelease = true;
            LOG.error("Error handling request", t);
            future.completeExceptionally(t);
        } finally {
            if (needRelease) {
                ReferenceCountUtil.release(buffer);
            }
        }
    }

    /**
     * Complete {@code request} inline (without dispatching to the handler) and schedule response
     * flushing on the channel executor. Used for SASL requests and for errors raised in the
     * decoder.
     */
    private void completeInline(KafkaRequest request, AbstractResponse response) {
        inflightResponses.addLast(request);
        request.future().whenCompleteAsync((r, t) -> sendResponse(ctx), ctx.executor());
        request.complete(response);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.ctx = ctx;
        this.remoteAddress = ctx.channel().remoteAddress();
        isActive.set(true);
        LOG.info(
                "Kafka listener '{}' accepted connection from {}",
                listenerName,
                ctx.channel().remoteAddress());
        KafkaMetricGroup m = metrics();
        if (m != null) {
            m.onConnectionOpened();
            connectionCounted = true;
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        LOG.info(
                "Kafka listener '{}' connection closed from {}",
                listenerName,
                ctx.channel().remoteAddress());
        if (saslTranscoder != null) {
            saslTranscoder.close();
            saslTranscoder = null;
        }
        if (connectionCounted) {
            KafkaMetricGroup m = metrics();
            if (m != null) {
                m.onConnectionClosed();
            }
            connectionCounted = false;
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent) evt;
            if (event.state().equals(IdleState.ALL_IDLE)) {
                LOG.warn("Connection {} is idle, closing...", ctx.channel().remoteAddress());
                ctx.close();
            }
        }
    }

    private void sendResponse(ChannelHandlerContext ctx) {
        KafkaRequest request;
        while ((request = inflightResponses.peekFirst()) != null) {
            CompletableFuture<AbstractResponse> f = request.future();
            ApiKeys apiKey = request.apiKey();
            boolean isDone = f.isDone();
            boolean cancelled = request.cancelled();

            if (apiKey.equals(PRODUCE)) {
                ProduceRequest produceRequest = request.request();
                if (produceRequest.acks() == 0 && isDone) {
                    // if acks=0, we don't need to wait for the response to be sent
                    inflightResponses.pollFirst();
                    request.releaseBuffer();
                    continue;
                }
            }

            if (!isDone) {
                break;
            }

            if (cancelled) {
                inflightResponses.pollFirst();
                request.releaseBuffer();
                continue;
            }

            inflightResponses.pollFirst();
            if (isActive.get()) {
                ByteBuf buffer = request.responseBuffer();
                ctx.writeAndFlush(buffer);
            } else {
                request.releaseBuffer();
            }
        }
    }

    protected void close() {
        isActive.set(false);
        ctx.close();
        LOG.warn(
                "Close channel {} with {} pending requests.",
                remoteAddress,
                inflightResponses.size());
        for (KafkaRequest request : inflightResponses) {
            request.cancel();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOG.error("Exception caught on channel {}", remoteAddress, cause);
        close();
    }

    private static KafkaRequest parseRequest(
            ChannelHandlerContext ctx,
            CompletableFuture<AbstractResponse> future,
            ByteBuf buffer,
            String listenerName) {
        ByteBuffer nioBuffer = buffer.nioBuffer();
        RequestHeader header = RequestHeader.parse(nioBuffer);
        if (isUnsupportedApiVersionRequest(header)) {
            ApiVersionsRequest request =
                    new ApiVersionsRequest.Builder(header.apiVersion()).build();
            return new KafkaRequest(
                    API_VERSIONS,
                    header.apiVersion(),
                    header,
                    request,
                    buffer,
                    ctx,
                    future,
                    listenerName);
        }
        RequestAndSize request =
                AbstractRequest.parseRequest(header.apiKey(), header.apiVersion(), nioBuffer);
        return new KafkaRequest(
                header.apiKey(),
                header.apiVersion(),
                header,
                request.request,
                buffer,
                ctx,
                future,
                listenerName);
    }

    private static boolean isUnsupportedApiVersionRequest(RequestHeader header) {
        return header.apiKey() == API_VERSIONS
                && !API_VERSIONS.isVersionSupported(header.apiVersion());
    }
}
