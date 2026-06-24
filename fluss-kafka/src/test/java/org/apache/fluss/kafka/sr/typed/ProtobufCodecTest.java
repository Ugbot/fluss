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
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Round-trip tests for {@link ProtobufCodecCompiler}. */
class ProtobufCodecTest {

    private static final String SIX_FIELD_PROTO =
            "syntax = \"proto3\";\n"
                    + "message M {\n"
                    + "  bool f_bool = 1;\n"
                    + "  int32 f_int = 2;\n"
                    + "  int64 f_long = 3;\n"
                    + "  float f_float = 4;\n"
                    + "  double f_double = 5;\n"
                    + "  string f_string = 6;\n"
                    + "}\n";

    @Test
    void roundTripSixFieldRecord() {
        RowType rowType = new ProtobufFormatTranslator().translateTo(SIX_FIELD_PROTO);
        RecordCodec codec = ProtobufCodecCompiler.compile(rowType, (short) 4);
        assertThat(codec.rowType()).isEqualTo(rowType);
        assertThat(codec.schemaVersion()).isEqualTo((short) 4);

        // Encode via the compiled codec, decode via the compiled codec, assert equality.
        Random rnd = new Random(0xC0DEC);
        for (int trial = 0; trial < 32; trial++) {
            boolean b = rnd.nextBoolean();
            int i = rnd.nextInt(Integer.MAX_VALUE);
            long l = rnd.nextLong() & Long.MAX_VALUE;
            float f = rnd.nextFloat();
            double d = rnd.nextDouble();
            String s = randomAscii(rnd, 1 + rnd.nextInt(16));

            // Build an IndexedRow with those values, encode via the codec.
            IndexedRowWriter srcWriter = new IndexedRowWriter(rowType);
            srcWriter.writeBoolean(b);
            srcWriter.writeInt(i);
            srcWriter.writeLong(l);
            srcWriter.writeFloat(f);
            srcWriter.writeDouble(d);
            srcWriter.writeString(org.apache.fluss.row.BinaryString.fromString(s));
            srcWriter.complete();
            IndexedRow srcRow = writerToRow(srcWriter, rowType);

            ByteBuf encoded = Unpooled.buffer(128);
            int written = codec.encodeInto(srcRow, encoded);
            byte[] wire = new byte[written];
            encoded.getBytes(0, wire);

            // Decode via the codec again, verify field equality.
            IndexedRowWriter dstWriter = new IndexedRowWriter(rowType);
            ByteBuf decodeBuf = Unpooled.wrappedBuffer(wire);
            int consumed = codec.decodeInto(decodeBuf, 0, wire.length, dstWriter);
            assertThat(consumed).isEqualTo(wire.length);
            IndexedRow dstRow = writerToRow(dstWriter, rowType);
            assertThat(dstRow.getBoolean(0)).isEqualTo(b);
            assertThat(dstRow.getInt(1)).isEqualTo(i);
            assertThat(dstRow.getLong(2)).isEqualTo(l);
            assertThat(dstRow.getFloat(3)).isEqualTo(f);
            assertThat(dstRow.getDouble(4)).isEqualTo(d);
            assertThat(dstRow.getString(5).toString()).isEqualTo(s);
        }
    }

    @Test
    void nullableFieldAbsentDecodesAsNull() {
        // Nullable field → proto3 optional; absence in the wire means null.
        String proto =
                "syntax = \"proto3\";\n"
                        + "message M {\n"
                        + "  int32 id = 1;\n"
                        + "  optional string nick = 2;\n"
                        + "}\n";
        RowType rowType = new ProtobufFormatTranslator().translateTo(proto);
        RecordCodec codec = ProtobufCodecCompiler.compile(rowType, (short) 1);

        // Wire contains only field 1 (id = 42).
        ByteBuf buf = Unpooled.buffer(16);
        org.apache.fluss.kafka.sr.typed.ProtobufCodecRuntime.writeTag(
                buf, 1, ProtobufCodecRuntime.WIRE_VARINT);
        ProtobufCodecRuntime.writeVarint(buf, 42);
        int len = buf.writerIndex();
        byte[] wire = new byte[len];
        buf.getBytes(0, wire);

        IndexedRowWriter dst = new IndexedRowWriter(rowType);
        codec.decodeInto(Unpooled.wrappedBuffer(wire), 0, len, dst);
        IndexedRow row = writerToRow(dst, rowType);
        assertThat(row.getInt(0)).isEqualTo(42);
        assertThat(row.isNullAt(1)).isTrue();
    }

    @Test
    void outOfOrderWireFieldsDecodeCorrectly() {
        // Protobuf doesn't mandate field-number order on the wire; ensure our codec handles
        // fields arriving in any order.
        RowType rowType = new ProtobufFormatTranslator().translateTo(SIX_FIELD_PROTO);
        RecordCodec codec = ProtobufCodecCompiler.compile(rowType, (short) 1);

        ByteBuf buf = Unpooled.buffer(64);
        // Tag 3 (long) then tag 1 (bool).
        ProtobufCodecRuntime.writeTag(buf, 3, ProtobufCodecRuntime.WIRE_VARINT);
        ProtobufCodecRuntime.writeVarint(buf, 999L);
        ProtobufCodecRuntime.writeTag(buf, 1, ProtobufCodecRuntime.WIRE_VARINT);
        ProtobufCodecRuntime.writeVarint(buf, 1);
        int len = buf.writerIndex();
        byte[] wire = new byte[len];
        buf.getBytes(0, wire);

        IndexedRowWriter dst = new IndexedRowWriter(rowType);
        codec.decodeInto(Unpooled.wrappedBuffer(wire), 0, len, dst);
        IndexedRow row = writerToRow(dst, rowType);
        // Fields 2, 4, 5 were absent → default to zero; field 6 was absent → empty string. The
        // schema is non-nullable so they stage the type's default.
        assertThat(row.getBoolean(0)).isTrue();
        assertThat(row.getInt(1)).isEqualTo(0);
        assertThat(row.getLong(2)).isEqualTo(999L);
        assertThat(row.getFloat(3)).isEqualTo(0f);
        assertThat(row.getDouble(4)).isEqualTo(0d);
        assertThat(row.getString(5).toString()).isEmpty();
    }

    @Test
    void unknownFieldIsSkipped() {
        RowType rowType = new ProtobufFormatTranslator().translateTo(SIX_FIELD_PROTO);
        RecordCodec codec = ProtobufCodecCompiler.compile(rowType, (short) 1);

        ByteBuf buf = Unpooled.buffer(64);
        // Tag 1 = true.
        ProtobufCodecRuntime.writeTag(buf, 1, ProtobufCodecRuntime.WIRE_VARINT);
        ProtobufCodecRuntime.writeVarint(buf, 1);
        // Tag 99 (unknown) — length-prefixed with 3 bytes of payload.
        ProtobufCodecRuntime.writeTag(buf, 99, ProtobufCodecRuntime.WIRE_LENGTH);
        ProtobufCodecRuntime.writeVarint(buf, 3);
        buf.writeBytes(new byte[] {1, 2, 3});
        // Tag 3 = 7.
        ProtobufCodecRuntime.writeTag(buf, 3, ProtobufCodecRuntime.WIRE_VARINT);
        ProtobufCodecRuntime.writeVarint(buf, 7L);
        int len = buf.writerIndex();
        byte[] wire = new byte[len];
        buf.getBytes(0, wire);

        IndexedRowWriter dst = new IndexedRowWriter(rowType);
        codec.decodeInto(Unpooled.wrappedBuffer(wire), 0, len, dst);
        IndexedRow row = writerToRow(dst, rowType);
        assertThat(row.getBoolean(0)).isTrue();
        assertThat(row.getLong(2)).isEqualTo(7L);
    }

    @Test
    void nestedShapeRejected() {
        RowType nested =
                DataTypes.ROW(
                        DataTypes.FIELD("n", DataTypes.ROW(DataTypes.FIELD("x", DataTypes.INT()))));
        assertThatThrownBy(() -> ProtobufCodecCompiler.compile(nested, (short) 1))
                .isInstanceOf(SchemaTranslationException.class)
                .hasMessageContaining("nested");
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

    private static String randomAscii(Random rnd, int len) {
        byte[] buf = new byte[len];
        for (int i = 0; i < len; i++) {
            buf[i] = (byte) ('a' + rnd.nextInt(26));
        }
        return new String(buf, StandardCharsets.UTF_8);
    }
}
