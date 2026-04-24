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
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Round-trip tests for {@link JsonCodecCompiler}. */
class JsonCodecTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String SIX_FIELD_JSON_SCHEMA =
            "{\"type\":\"object\",\"properties\":{"
                    + "\"f_bool\":{\"type\":\"boolean\"},"
                    + "\"f_int\":{\"type\":\"integer\",\"format\":\"int32\"},"
                    + "\"f_long\":{\"type\":\"integer\"},"
                    + "\"f_float\":{\"type\":\"number\",\"format\":\"float\"},"
                    + "\"f_double\":{\"type\":\"number\"},"
                    + "\"f_string\":{\"type\":\"string\"}"
                    + "},\"required\":[\"f_bool\",\"f_int\",\"f_long\",\"f_float\",\"f_double\",\"f_string\"]}";

    @Test
    void roundTripSixFieldRecord() {
        JsonSchemaFormatTranslator xlator = new JsonSchemaFormatTranslator();
        RowType rowType = xlator.translateTo(SIX_FIELD_JSON_SCHEMA);

        RecordCodec codec = JsonCodecCompiler.compile(rowType, (short) 3);
        assertThat(codec.rowType()).isEqualTo(rowType);
        assertThat(codec.schemaVersion()).isEqualTo((short) 3);

        Random rnd = new Random(0xC0DEC);
        for (int trial = 0; trial < 32; trial++) {
            boolean b = rnd.nextBoolean();
            int i = rnd.nextInt();
            long l = rnd.nextLong();
            float f = rnd.nextFloat();
            double d = rnd.nextDouble();
            String s = randomAsciiString(rnd, 1 + rnd.nextInt(16));

            // Build a JSON payload via Jackson (the "baseline" serializer Confluent would emit).
            byte[] payload =
                    ("{\"f_bool\":"
                                    + b
                                    + ",\"f_int\":"
                                    + i
                                    + ",\"f_long\":"
                                    + l
                                    + ",\"f_float\":"
                                    + f
                                    + ",\"f_double\":"
                                    + d
                                    + ",\"f_string\":"
                                    + MAPPER.valueToTree(s)
                                    + "}")
                            .getBytes(StandardCharsets.UTF_8);

            // Decode path.
            IndexedRowWriter writer = new IndexedRowWriter(rowType);
            ByteBuf srcBuf = Unpooled.wrappedBuffer(payload);
            int consumed = codec.decodeInto(srcBuf, 0, payload.length, writer);
            assertThat(consumed).isEqualTo(payload.length);

            IndexedRow row = writerToRow(writer, rowType);
            assertThat(row.getBoolean(0)).isEqualTo(b);
            assertThat(row.getInt(1)).isEqualTo(i);
            assertThat(row.getLong(2)).isEqualTo(l);
            assertThat(row.getFloat(3)).isEqualTo(f);
            assertThat(row.getDouble(4)).isEqualTo(d);
            assertThat(row.getString(5).toString()).isEqualTo(s);

            // Encode path: round-trip back through the compiled codec's encodeInto, then parse
            // the bytes via Jackson to verify the output is valid JSON with the right values.
            ByteBuf dstBuf = Unpooled.buffer(512);
            int written = codec.encodeInto(row, dstBuf);
            byte[] reencoded = new byte[written];
            dstBuf.getBytes(0, reencoded);
            try {
                JsonNode reparsed = MAPPER.readTree(reencoded);
                assertThat(reparsed.get("f_bool").asBoolean()).isEqualTo(b);
                assertThat(reparsed.get("f_int").asInt()).isEqualTo(i);
                assertThat(reparsed.get("f_long").asLong()).isEqualTo(l);
                assertThat((float) reparsed.get("f_float").asDouble()).isEqualTo(f);
                assertThat(reparsed.get("f_double").asDouble()).isEqualTo(d);
                assertThat(reparsed.get("f_string").asText()).isEqualTo(s);
            } catch (java.io.IOException e) {
                throw new AssertionError("Compiled codec produced invalid JSON: " + e, e);
            }
        }
    }

    @Test
    void bytesFieldBase64RoundTrip() {
        String schema =
                "{\"type\":\"object\",\"properties\":{"
                        + "\"f_bytes\":{\"type\":\"string\",\"format\":\"byte\"}"
                        + "},\"required\":[\"f_bytes\"]}";
        RowType rowType = new JsonSchemaFormatTranslator().translateTo(schema);
        RecordCodec codec = JsonCodecCompiler.compile(rowType, (short) 1);

        byte[] payload = new byte[] {1, 2, 3, 4, 5};
        String encodedBody =
                "{\"f_bytes\":\"" + Base64.getEncoder().encodeToString(payload) + "\"}";
        IndexedRowWriter writer = new IndexedRowWriter(rowType);
        ByteBuf buf = Unpooled.wrappedBuffer(encodedBody.getBytes(StandardCharsets.UTF_8));
        codec.decodeInto(buf, 0, buf.readableBytes(), writer);
        IndexedRow row = writerToRow(writer, rowType);
        assertThat(row.getBytes(0)).isEqualTo(payload);
    }

    @Test
    void nullableFieldNullDecode() {
        String schema =
                "{\"type\":\"object\",\"properties\":{"
                        + "\"f\":{\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\"}]}"
                        + "}}";
        RowType rowType = new JsonSchemaFormatTranslator().translateTo(schema);
        RecordCodec codec = JsonCodecCompiler.compile(rowType, (short) 1);

        byte[] payload = "{\"f\":null}".getBytes(StandardCharsets.UTF_8);
        IndexedRowWriter writer = new IndexedRowWriter(rowType);
        ByteBuf buf = Unpooled.wrappedBuffer(payload);
        codec.decodeInto(buf, 0, payload.length, writer);
        IndexedRow row = writerToRow(writer, rowType);
        assertThat(row.isNullAt(0)).isTrue();
    }

    @Test
    void nestedShapesRejected() {
        RowType nested =
                DataTypes.ROW(
                        DataTypes.FIELD("n", DataTypes.ROW(DataTypes.FIELD("x", DataTypes.INT()))));
        assertThatThrownBy(() -> JsonCodecCompiler.compile(nested, (short) 1))
                .isInstanceOf(SchemaTranslationException.class)
                .hasMessageContaining("nested");
    }

    @Test
    void arraysRejectedForNow() {
        RowType withArr = DataTypes.ROW(DataTypes.FIELD("xs", DataTypes.ARRAY(DataTypes.INT())));
        assertThatThrownBy(() -> JsonCodecCompiler.compile(withArr, (short) 1))
                .isInstanceOf(SchemaTranslationException.class);
    }

    // --- helpers ---------------------------------------------------------

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
            // Avoid quote/backslash/newline which break raw JSON concatenation.
            int code;
            do {
                code = 0x20 + rnd.nextInt(0x5E);
            } while (code == '"' || code == '\\');
            buf[i] = (byte) code;
        }
        return new String(buf, StandardCharsets.UTF_8);
    }
}
