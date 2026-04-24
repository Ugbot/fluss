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

package org.apache.fluss.jmh.kafka;

import org.apache.fluss.kafka.sr.typed.AvroCodecCompiler;
import org.apache.fluss.kafka.sr.typed.AvroFormatTranslator;
import org.apache.fluss.kafka.sr.typed.RecordCodec;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.types.RowType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Compares the Janino-compiled {@link RecordCodec} decode path against a hand-written Avro {@link
 * GenericDatumReader} baseline on a 12-field record.
 *
 * <p><b>Gate (plan §25.4):</b> the compiled codec must stay within 20% of the baseline decoder's
 * steady-state throughput. Run locally with {@code mvn -pl fluss-jmh test} + {@code java -jar
 * target/benchmarks.jar AvroDecodeBench}. The JMH result must show compiled/baseline ratio &ge; 0.8
 * (compiled at least as fast as baseline * 0.8) before PR review.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
@State(Scope.Thread)
public class AvroDecodeBench {

    private static final String SCHEMA_JSON =
            "{\"type\":\"record\",\"name\":\"Bench12\",\"fields\":["
                    + "{\"name\":\"f1\",\"type\":\"boolean\"},"
                    + "{\"name\":\"f2\",\"type\":\"int\"},"
                    + "{\"name\":\"f3\",\"type\":\"long\"},"
                    + "{\"name\":\"f4\",\"type\":\"float\"},"
                    + "{\"name\":\"f5\",\"type\":\"double\"},"
                    + "{\"name\":\"f6\",\"type\":\"string\"},"
                    + "{\"name\":\"f7\",\"type\":\"bytes\"},"
                    + "{\"name\":\"f8\",\"type\":\"int\"},"
                    + "{\"name\":\"f9\",\"type\":\"long\"},"
                    + "{\"name\":\"f10\",\"type\":\"string\"},"
                    + "{\"name\":\"f11\",\"type\":\"double\"},"
                    + "{\"name\":\"f12\",\"type\":[\"null\",\"long\"],\"default\":null}"
                    + "]}";

    private Schema avroSchema;
    private RowType rowType;
    private RecordCodec codec;
    private byte[] payload;

    // Reusable decoder objects to avoid one-off allocations.
    private GenericDatumReader<GenericRecord> baselineReader;
    private BinaryDecoder reuseDecoder;
    private GenericRecord reuseRecord;
    private IndexedRowWriter writer;

    @Setup
    public void setUp() throws Exception {
        avroSchema = new Schema.Parser().parse(SCHEMA_JSON);
        rowType = new AvroFormatTranslator().translateTo(SCHEMA_JSON);
        codec = AvroCodecCompiler.compile(avroSchema, rowType, (short) 1);

        Random rnd = new Random(0xDEC0DEL);
        GenericRecord rec = new GenericData.Record(avroSchema);
        rec.put("f1", rnd.nextBoolean());
        rec.put("f2", rnd.nextInt());
        rec.put("f3", rnd.nextLong());
        rec.put("f4", rnd.nextFloat());
        rec.put("f5", rnd.nextDouble());
        rec.put("f6", randomAscii(rnd, 20));
        byte[] raw = new byte[16];
        rnd.nextBytes(raw);
        rec.put("f7", ByteBuffer.wrap(raw));
        rec.put("f8", rnd.nextInt());
        rec.put("f9", rnd.nextLong());
        rec.put("f10", randomAscii(rnd, 24));
        rec.put("f11", rnd.nextDouble());
        rec.put("f12", rnd.nextBoolean() ? null : rnd.nextLong());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder enc = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(avroSchema).write(rec, enc);
        enc.flush();
        payload = out.toByteArray();

        baselineReader = new GenericDatumReader<>(avroSchema);
        reuseDecoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(payload), null);
        reuseRecord = new GenericData.Record(avroSchema);
        writer = new IndexedRowWriter(rowType);
    }

    /** Hand-written Avro baseline — reuses the decoder instance and a target record. */
    @Benchmark
    public GenericRecord baselineAvroDatumReader() throws Exception {
        reuseDecoder =
                DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(payload), reuseDecoder);
        return baselineReader.read(reuseRecord, reuseDecoder);
    }

    /** Janino-compiled codec. Writes straight into an {@link IndexedRowWriter}. */
    @Benchmark
    public void compiledCodec(Blackhole bh) {
        ByteBuf buf = Unpooled.wrappedBuffer(payload);
        writer.reset();
        int n = codec.decodeInto(buf, 0, payload.length, writer);
        bh.consume(n);
        bh.consume(writer.buffer());
    }

    private static String randomAscii(Random rnd, int len) {
        char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            chars[i] = (char) ('A' + rnd.nextInt(26));
        }
        return new String(chars);
    }
}
