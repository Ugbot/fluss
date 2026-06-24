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
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Symmetric encode-direction benchmark for the compiled Avro codec vs a hand-written Avro {@link
 * GenericDatumWriter}.
 *
 * <p><b>Gate (plan §25.4):</b> same 20% envelope as {@link AvroDecodeBench}. Compiled encode must
 * stay within 20% of the baseline writer's throughput at steady state.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
@State(Scope.Thread)
public class AvroEncodeBench {

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
                    + "{\"name\":\"f12\",\"type\":\"long\"}"
                    + "]}";

    private Schema avroSchema;
    private RowType rowType;
    private RecordCodec codec;

    // Encode source: same row as a GenericRecord + as a BinaryRow.
    private GenericRecord sourceRecord;
    private IndexedRow sourceRow;
    private GenericDatumWriter<GenericRecord> baselineWriter;
    private BinaryEncoder reuseEncoder;
    private ByteArrayOutputStream baselineSink;
    private ByteBuf compiledSink;

    @Setup
    public void setUp() {
        avroSchema = new Schema.Parser().parse(SCHEMA_JSON);
        rowType = new AvroFormatTranslator().translateTo(SCHEMA_JSON);
        codec = AvroCodecCompiler.compile(avroSchema, rowType, (short) 1);

        Random rnd = new Random(0xEC0DEL);
        sourceRecord = new GenericData.Record(avroSchema);
        sourceRecord.put("f1", rnd.nextBoolean());
        sourceRecord.put("f2", rnd.nextInt());
        sourceRecord.put("f3", rnd.nextLong());
        sourceRecord.put("f4", rnd.nextFloat());
        sourceRecord.put("f5", rnd.nextDouble());
        sourceRecord.put("f6", randomAscii(rnd, 20));
        byte[] raw = new byte[16];
        rnd.nextBytes(raw);
        sourceRecord.put("f7", ByteBuffer.wrap(raw));
        sourceRecord.put("f8", rnd.nextInt());
        sourceRecord.put("f9", rnd.nextLong());
        sourceRecord.put("f10", randomAscii(rnd, 24));
        sourceRecord.put("f11", rnd.nextDouble());
        sourceRecord.put("f12", rnd.nextLong());

        // Build a parallel IndexedRow with the same values.
        IndexedRowWriter w = new IndexedRowWriter(rowType);
        w.reset();
        w.writeBoolean((Boolean) sourceRecord.get("f1"));
        w.writeInt((Integer) sourceRecord.get("f2"));
        w.writeLong((Long) sourceRecord.get("f3"));
        w.writeFloat((Float) sourceRecord.get("f4"));
        w.writeDouble((Double) sourceRecord.get("f5"));
        w.writeString(
                org.apache.fluss.row.BinaryString.fromString((String) sourceRecord.get("f6")));
        w.writeBytes(((ByteBuffer) sourceRecord.get("f7")).array());
        w.writeInt((Integer) sourceRecord.get("f8"));
        w.writeLong((Long) sourceRecord.get("f9"));
        w.writeString(
                org.apache.fluss.row.BinaryString.fromString((String) sourceRecord.get("f10")));
        w.writeDouble((Double) sourceRecord.get("f11"));
        w.writeLong((Long) sourceRecord.get("f12"));

        byte[] snapshot = new byte[w.position()];
        System.arraycopy(w.buffer(), 0, snapshot, 0, w.position());
        DataType[] types = rowType.getChildren().toArray(new DataType[0]);
        sourceRow = new IndexedRow(types);
        sourceRow.pointTo(MemorySegment.wrap(snapshot), 0, snapshot.length);

        baselineWriter = new GenericDatumWriter<>(avroSchema);
        baselineSink = new ByteArrayOutputStream(256);
        reuseEncoder = EncoderFactory.get().binaryEncoder(baselineSink, null);
        compiledSink = Unpooled.buffer(256);
    }

    @Benchmark
    public int baselineAvroDatumWriter() throws Exception {
        baselineSink.reset();
        reuseEncoder = EncoderFactory.get().binaryEncoder(baselineSink, reuseEncoder);
        baselineWriter.write(sourceRecord, reuseEncoder);
        reuseEncoder.flush();
        return baselineSink.size();
    }

    @Benchmark
    public void compiledCodec(Blackhole bh) {
        compiledSink.clear();
        int wrote = codec.encodeInto(sourceRow, compiledSink);
        bh.consume(wrote);
        bh.consume(compiledSink.readableBytes());
    }

    private static String randomAscii(Random rnd, int len) {
        char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            chars[i] = (char) ('A' + rnd.nextInt(26));
        }
        return new String(chars);
    }
}
