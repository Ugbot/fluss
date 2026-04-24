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

package org.apache.fluss.kafka.sr.typed;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Round-trip tests for {@link AvroCodecCompiler}. Encodes a random record through the compiled
 * codec, decodes via both the compiled codec and a hand-written Avro {@link GenericDatumReader}
 * baseline, and verifies field-equal results either way.
 */
class AvroCodecTest {

    private static final String EIGHT_FIELD_SCHEMA =
            "{\"type\":\"record\",\"name\":\"Bench8\",\"fields\":["
                    + "{\"name\":\"f_bool\",\"type\":\"boolean\"},"
                    + "{\"name\":\"f_int\",\"type\":\"int\"},"
                    + "{\"name\":\"f_long\",\"type\":\"long\"},"
                    + "{\"name\":\"f_float\",\"type\":\"float\"},"
                    + "{\"name\":\"f_double\",\"type\":\"double\"},"
                    + "{\"name\":\"f_string\",\"type\":\"string\"},"
                    + "{\"name\":\"f_bytes\",\"type\":\"bytes\"},"
                    + "{\"name\":\"f_maybe\",\"type\":[\"null\",\"long\"],\"default\":null}"
                    + "]}";

    @Test
    void roundTripEightFieldRecord() {
        Schema avroSchema = new Schema.Parser().parse(EIGHT_FIELD_SCHEMA);
        AvroFormatTranslator xlator = new AvroFormatTranslator();
        RowType rowType = xlator.translateTo(EIGHT_FIELD_SCHEMA);

        RecordCodec codec = AvroCodecCompiler.compile(avroSchema, rowType, (short) 7);
        assertThat(codec.rowType()).isEqualTo(rowType);
        assertThat(codec.schemaVersion()).isEqualTo((short) 7);

        // 1. Encode via Avro baseline, then decode via the compiled codec.
        Random rnd = new Random(0xC0DEC);
        for (int trial = 0; trial < 32; trial++) {
            boolean b = rnd.nextBoolean();
            int i = rnd.nextInt();
            long l = rnd.nextLong();
            float f = rnd.nextFloat();
            double d = rnd.nextDouble();
            String s = randomAsciiString(rnd, 1 + rnd.nextInt(24));
            byte[] bytes = new byte[rnd.nextInt(40)];
            rnd.nextBytes(bytes);
            Long maybe = rnd.nextBoolean() ? null : rnd.nextLong();

            GenericRecord rec = new GenericData.Record(avroSchema);
            rec.put("f_bool", b);
            rec.put("f_int", i);
            rec.put("f_long", l);
            rec.put("f_float", f);
            rec.put("f_double", d);
            rec.put("f_string", s);
            rec.put("f_bytes", java.nio.ByteBuffer.wrap(bytes));
            rec.put("f_maybe", maybe);

            byte[] payload = encodeAvro(avroSchema, rec);

            // Decode path.
            IndexedRowWriter writer = new IndexedRowWriter(rowType);
            ByteBuf buf = Unpooled.wrappedBuffer(payload);
            int consumed = codec.decodeInto(buf, 0, payload.length, writer);
            assertThat(consumed).isEqualTo(payload.length);

            IndexedRow row = writerToRow(writer, rowType);
            assertThat(row.getBoolean(0)).isEqualTo(b);
            assertThat(row.getInt(1)).isEqualTo(i);
            assertThat(row.getLong(2)).isEqualTo(l);
            assertThat(row.getFloat(3)).isEqualTo(f);
            assertThat(row.getDouble(4)).isEqualTo(d);
            assertThat(row.getString(5).toString()).isEqualTo(s);
            assertThat(row.getBytes(6)).containsExactly(bytes);
            assertThat(row.isNullAt(7)).isEqualTo(maybe == null);
            if (maybe != null) {
                assertThat(row.getLong(7)).isEqualTo(maybe.longValue());
            }

            // Encode via compiled codec and cross-check with Avro's decoder.
            ByteBuf out = Unpooled.buffer();
            try {
                int wrote = codec.encodeInto(row, out);
                assertThat(wrote).isPositive();
                byte[] encoded = new byte[out.readableBytes()];
                out.readBytes(encoded);

                GenericRecord decoded = decodeAvro(avroSchema, encoded);
                assertThat(decoded.get("f_bool")).isEqualTo(b);
                assertThat(decoded.get("f_int")).isEqualTo(i);
                assertThat(decoded.get("f_long")).isEqualTo(l);
                assertThat(decoded.get("f_float")).isEqualTo(f);
                assertThat(decoded.get("f_double")).isEqualTo(d);
                assertThat(decoded.get("f_string").toString()).isEqualTo(s);
                assertThat(((java.nio.ByteBuffer) decoded.get("f_bytes")).array())
                        .containsExactly(bytes);
                if (maybe == null) {
                    assertThat(decoded.get("f_maybe")).isNull();
                } else {
                    assertThat(decoded.get("f_maybe")).isEqualTo(maybe);
                }
            } finally {
                out.release();
            }
        }
    }

    @Test
    void compileRejectsSchemaRowTypeMismatch() {
        Schema avroSchema = new Schema.Parser().parse(EIGHT_FIELD_SCHEMA);
        RowType wrong = DataTypes.ROW(DataTypes.FIELD("only", DataTypes.INT()));
        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> AvroCodecCompiler.compile(avroSchema, wrong, (short) 1))
                .isInstanceOf(SchemaTranslationException.class)
                .hasMessageContaining("field");
    }

    // ----------------------------------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------------------------------

    private static byte[] encodeAvro(Schema schema, GenericRecord record) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            org.apache.avro.io.BinaryEncoder encoder =
                    EncoderFactory.get().binaryEncoder(out, null);
            new GenericDatumWriter<GenericRecord>(schema).write(record, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static GenericRecord decodeAvro(Schema schema, byte[] payload) {
        try {
            org.apache.avro.io.BinaryDecoder decoder =
                    DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(payload), null);
            return new GenericDatumReader<GenericRecord>(schema).read(null, decoder);
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static IndexedRow writerToRow(IndexedRowWriter writer, RowType rowType) {
        DataType[] types = rowType.getChildren().toArray(new DataType[0]);
        byte[] snapshot = new byte[writer.position()];
        System.arraycopy(writer.buffer(), 0, snapshot, 0, writer.position());
        IndexedRow row = new IndexedRow(types);
        row.pointTo(MemorySegment.wrap(snapshot), 0, snapshot.length);
        return row;
    }

    private static String randomAsciiString(Random rnd, int len) {
        byte[] buf = new byte[len];
        for (int i = 0; i < len; i++) {
            // ASCII printable range.
            buf[i] = (byte) (0x20 + rnd.nextInt(0x5E));
        }
        return new String(buf, StandardCharsets.UTF_8);
    }

    @SuppressWarnings("unused")
    private static BinaryString bs(String s) {
        return BinaryString.fromString(s);
    }
}
