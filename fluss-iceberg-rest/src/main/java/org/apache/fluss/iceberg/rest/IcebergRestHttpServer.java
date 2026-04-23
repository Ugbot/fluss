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

package org.apache.fluss.iceberg.rest;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.catalog.CatalogService;
import org.apache.fluss.rpc.netty.NettyUtils;
import org.apache.fluss.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.fluss.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.fluss.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.HttpObjectAggregator;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.HttpServerCodec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Owns the Netty lifecycle for the Iceberg REST Catalog HTTP listener. Mirrors the Schema Registry
 * scaffolding — one accept group + one selector group, HTTP/1.1 pipeline with {@link
 * HttpServerCodec} and a 1 MiB {@link HttpObjectAggregator}. Intentionally independent of {@code
 * NettyServer} / {@code RequestChannel[]}; this is plain REST, not the Fluss binary RPC framing.
 */
@Internal
public final class IcebergRestHttpServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergRestHttpServer.class);
    private static final int MAX_HTTP_BODY_BYTES = 1024 * 1024; // 1 MiB

    private final String host;
    private final int requestedPort;
    private final CatalogService catalog;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private volatile EventLoopGroup acceptorGroup;
    private volatile EventLoopGroup selectorGroup;
    private volatile Channel bindChannel;
    private volatile int boundPort = -1;

    public IcebergRestHttpServer(String host, int port, CatalogService catalog) {
        this.host = host;
        this.requestedPort = port;
        this.catalog = catalog;
    }

    /** Bind and begin serving. Blocks until the port is bound (or fails). */
    public void start() throws Exception {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("IcebergRestHttpServer already started");
        }
        acceptorGroup = NettyUtils.newEventLoopGroup(1, "fluss-iceberg-rest-acceptor");
        selectorGroup = NettyUtils.newEventLoopGroup(2, "fluss-iceberg-rest-selector");
        ServerBootstrap bootstrap =
                new ServerBootstrap()
                        .group(acceptorGroup, selectorGroup)
                        .channel(NettyUtils.getServerSocketChannelClass(acceptorGroup))
                        .option(ChannelOption.SO_BACKLOG, 128)
                        .childHandler(
                                new ChannelInitializer<SocketChannel>() {
                                    @Override
                                    protected void initChannel(SocketChannel ch) {
                                        ch.pipeline()
                                                .addLast("http-codec", new HttpServerCodec())
                                                .addLast(
                                                        "http-aggregator",
                                                        new HttpObjectAggregator(
                                                                MAX_HTTP_BODY_BYTES))
                                                .addLast(
                                                        "iceberg-rest-handler",
                                                        new IcebergRestHttpHandler(catalog));
                                    }
                                });
        bindChannel = bootstrap.bind(new InetSocketAddress(host, requestedPort)).sync().channel();
        boundPort = ((InetSocketAddress) bindChannel.localAddress()).getPort();
        LOG.info(
                "Iceberg REST Catalog HTTP listener bound to {}:{} (requested port {})",
                host,
                boundPort,
                requestedPort);
    }

    /** Actual port bound (resolves {@code 0} → OS-assigned port). */
    public int boundPort() {
        if (boundPort < 0) {
            throw new IllegalStateException("IcebergRestHttpServer has not started yet");
        }
        return boundPort;
    }

    @Override
    public void close() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        CompletableFuture<Void> shutdown = NettyUtils.shutdownChannel(bindChannel);
        shutdown = shutdown.thenCompose(ignored -> NettyUtils.shutdownGroup(acceptorGroup));
        shutdown = shutdown.thenCompose(ignored -> NettyUtils.shutdownGroup(selectorGroup));
        try {
            shutdown.get();
        } catch (Exception e) {
            LOG.warn("Iceberg REST Catalog HTTP listener shutdown threw", e);
        }
    }
}
