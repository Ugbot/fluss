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

package org.apache.fluss.jmh;

import org.apache.fluss.record.send.Send;
import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.messages.PbApiVersion;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.MessageCodec;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.buffer.PooledByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.fluss.shaded.netty4.io.netty.util.ReferenceCountUtil;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Microbenchmark for the RPC wire-encode hot path in {@link MessageCodec}.
 *
 * <p>Every RPC the server emits and every RPC the client sends passes through {@link MessageCodec},
 * which sizes the message via {@link org.apache.fluss.rpc.messages.ApiMessage#totalSize()},
 * allocates a Netty {@link ByteBuf} of exactly that capacity from the pooled allocator, writes the
 * frame header, and serializes the message body by walking its fields. This sits directly on the
 * produce/fetch/metadata critical path, so the per-message allocation + serialization cost matters
 * under high request rates.
 *
 * <p>The benchmark uses {@link ApiVersionsResponse} as the carrier message because it is the
 * smallest real {@code ApiMessage} that still exposes a {@code repeated} field ({@link
 * PbApiVersion} with three {@code int32} fields each). Sweeping the entry count gives a clean
 * field-count axis while keeping construction cheap, isolating the codec/allocator cost from
 * message-construction noise. Entries are filled with fixed-seed random data (project policy: no
 * hardcoded sample data).
 *
 * <p>Two real encode paths are measured, both against the pooled {@link PooledByteBufAllocator}:
 *
 * <ul>
 *   <li>{@code encodeRequest} — {@link MessageCodec#encodeRequest}, which serializes through {@code
 *       ApiMessage.writeTo(ByteBuf)} into a freshly pooled buffer and returns that buffer; the
 *       buffer is released every invocation so the pool reaches steady state.
 *   <li>{@code encodeSuccessResponse} — {@link MessageCodec#encodeSuccessResponse}, the actual
 *       server response path, which serializes through {@code ApiMessage.writeTo(WritableOutput)}
 *       and yields a {@link Send}. The {@link Send} is written to a throwaway {@link
 *       EmbeddedChannel}, then the resulting outbound buffers are drained and released each
 *       invocation.
 * </ul>
 *
 * <p>Run: {@code mvn -pl fluss-jmh test-compile} then execute {@code main}, or {@code java ...
 * org.apache.fluss.jmh.MessageCodecBenchmark}.
 */
@State(Scope.Thread)
@Warmup(iterations = 5)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Measurement(iterations = 5)
@Fork(value = 1)
public class MessageCodecBenchmark {

    /** Seed fixed so the randomized api-version entries are identical across runs and variants. */
    private static final long SEED = 0xB0A710CEL;

    /** Synthetic request id used for the frame header; value is irrelevant to encode cost. */
    private static final int REQUEST_ID = 4711;

    /**
     * Number of {@code repeated PbApiVersion} entries in the carrier message. 1 is the trivial
     * single-field case, 16 a typical {@code ApiVersions} handshake response, and 256 a large
     * repeated-field message that stresses the per-element serialization loop.
     */
    @Param({"1", "16", "256"})
    private int entryCount;

    private ByteBufAllocator allocator;
    private ApiVersionsResponse response;
    private EmbeddedChannel channel;

    @Setup(Level.Trial)
    public void setup() {
        allocator = PooledByteBufAllocator.DEFAULT;

        Random random = new Random(SEED);
        List<PbApiVersion> versions = new ArrayList<>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            PbApiVersion version = new PbApiVersion();
            version.setApiKey(random.nextInt(Short.MAX_VALUE))
                    .setMinVersion(random.nextInt(Short.MAX_VALUE))
                    .setMaxVersion(random.nextInt(Short.MAX_VALUE));
            versions.add(version);
        }
        response = new ApiVersionsResponse();
        response.addAllApiVersions(versions);

        channel = new EmbeddedChannel();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        // Drain any residual outbound buffers, then close the throwaway channel.
        drainOutbound();
        channel.finishAndReleaseAll();
    }

    @Benchmark
    public void encodeRequest(Blackhole bh) {
        ByteBuf buffer =
                MessageCodec.encodeRequest(
                        allocator,
                        ApiKeys.API_VERSIONS.id,
                        ApiKeys.API_VERSIONS.highestSupportedVersion,
                        REQUEST_ID,
                        response);
        bh.consume(buffer.writerIndex());
        buffer.release();
    }

    @Benchmark
    public void encodeSuccessResponse(Blackhole bh) throws Exception {
        Send send = MessageCodec.encodeSuccessResponse(allocator, REQUEST_ID, response);
        send.writeTo(channel);
        channel.flush();
        bh.consume(drainOutbound());
    }

    /**
     * Drains and releases all staged outbound buffers from the embedded channel, returning the
     * total number of bytes drained so the JIT cannot elide the encode work.
     */
    private int drainOutbound() {
        int drainedBytes = 0;
        Object outbound;
        while ((outbound = channel.readOutbound()) != null) {
            if (outbound instanceof ByteBuf) {
                drainedBytes += ((ByteBuf) outbound).readableBytes();
            }
            ReferenceCountUtil.release(outbound);
        }
        return drainedBytes;
    }

    public static void main(String[] args) throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + MessageCodecBenchmark.class.getCanonicalName() + ".*")
                        .build();

        new Runner(opt).run();
    }
}
