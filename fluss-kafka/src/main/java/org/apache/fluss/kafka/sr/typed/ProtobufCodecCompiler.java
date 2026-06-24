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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.SimpleCompiler;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates a specialised {@link RecordCodec} for a Fluss {@link RowType} emitted + consumed as
 * Protobuf-encoded payloads. Field tags are assigned by position (1, 2, 3, ...) — matching the
 * convention {@link ProtobufFormatTranslator#translateFrom} emits when rendering a {@code RowType}
 * back out as {@code .proto} text.
 *
 * <p>Phase T-MF.4 scalar-only scope — matches {@link AvroCodecCompiler} and {@link
 * JsonCodecCompiler}. Nested messages, arrays, maps land in the T-MF.4 nested-shape follow-up.
 *
 * @since 0.9
 */
@Internal
public final class ProtobufCodecCompiler {

    private static final AtomicLong CLASS_ID_SEQ = new AtomicLong();

    private static final String GENERATED_PACKAGE = "org.apache.fluss.kafka.sr.typed.generated";
    private static final String RUNTIME_CLASS =
            "org.apache.fluss.kafka.sr.typed.ProtobufCodecRuntime";
    private static final String WRITER_CLASS = "org.apache.fluss.row.indexed.IndexedRowWriter";
    private static final String BYTEBUF_CLASS =
            "org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf";
    private static final String BINARYROW_CLASS = "org.apache.fluss.row.BinaryRow";
    private static final String BINARYSTRING_CLASS = "org.apache.fluss.row.BinaryString";
    private static final String ROWTYPE_CLASS = "org.apache.fluss.types.RowType";

    private ProtobufCodecCompiler() {}

    public static RecordCodec compile(RowType rowType, short schemaVersion) {
        if (rowType == null) {
            throw new SchemaTranslationException(
                    "ProtobufCodecCompiler requires a non-null RowType.");
        }
        for (DataField f : rowType.getFields()) {
            DataTypeRoot r = f.getType().getTypeRoot();
            if (r == DataTypeRoot.ARRAY
                    || r == DataTypeRoot.MAP
                    || r == DataTypeRoot.ROW
                    || r == DataTypeRoot.DECIMAL) {
                throw new SchemaTranslationException(
                        "ProtobufCodecCompiler (Phase T-MF.4 scalar-only) does not yet emit codec"
                                + " code for "
                                + r
                                + " fields (field '"
                                + f.getName()
                                + "'). Nested shapes land with the T-MF.4 nested-shape follow-up.");
            }
        }

        String className = "ProtobufCodec$" + CLASS_ID_SEQ.incrementAndGet();
        String fqcn = GENERATED_PACKAGE + "." + className;
        String source = emitSource(className, rowType, schemaVersion);

        try {
            SimpleCompiler compiler = new SimpleCompiler();
            compiler.setTargetVersion(8);
            compiler.setSourceVersion(8);
            compiler.setParentClassLoader(ProtobufCodecCompiler.class.getClassLoader());
            compiler.cook(fqcn + ".java", new java.io.StringReader(source));
            Class<?> cls = compiler.getClassLoader().loadClass(fqcn);
            Constructor<?> ctor = cls.getDeclaredConstructor(RowType.class, short.class);
            return (RecordCodec) ctor.newInstance(rowType, schemaVersion);
        } catch (CompileException | java.io.IOException e) {
            throw new SchemaTranslationException(
                    "Janino failed to compile generated codec '" + fqcn + "':\n" + source, e);
        } catch (ReflectiveOperationException | LinkageError e) {
            throw new SchemaTranslationException(
                    "Failed to instantiate generated codec '" + fqcn + "'", e);
        }
    }

    // ------------------------------------------------------------------------------------------
    // source emission
    // ------------------------------------------------------------------------------------------

    private static String emitSource(String className, RowType rowType, short schemaVersion) {
        StringBuilder sb = new StringBuilder(8192);
        sb.append("package ").append(GENERATED_PACKAGE).append(";\n");
        sb.append("public final class ").append(className).append(" implements ");
        sb.append("org.apache.fluss.kafka.sr.typed.RecordCodec {\n");
        sb.append("    private final ").append(ROWTYPE_CLASS).append(" rowType;\n");
        sb.append("    private final short schemaVersion;\n");
        sb.append("    public ")
                .append(className)
                .append("(")
                .append(ROWTYPE_CLASS)
                .append(" rowType, short schemaVersion) {\n");
        sb.append("        this.rowType = rowType;\n");
        sb.append("        this.schemaVersion = schemaVersion;\n");
        sb.append("    }\n");
        sb.append("    public ").append(ROWTYPE_CLASS).append(" rowType() { return rowType; }\n");
        sb.append("    public short schemaVersion() { return schemaVersion; }\n");

        emitDecode(sb, rowType);
        emitEncode(sb, rowType);

        sb.append("}\n");
        return sb.toString();
    }

    /**
     * Decode walks the wire reading tags and dispatches by field number (1-indexed, matching
     * position order). Each field's value is staged in a typed local variable; after the wire runs
     * dry the staged values are written to the writer in RowType order (IndexedRowWriter requires
     * in-order writes).
     */
    private static void emitDecode(StringBuilder sb, RowType rowType) {
        sb.append("    public int decodeInto(")
                .append(BYTEBUF_CLASS)
                .append(" src, int offset, int length, ")
                .append(WRITER_CLASS)
                .append(" dst) {\n");
        sb.append("        int startReader = src.readerIndex();\n");
        sb.append("        src.readerIndex(offset);\n");
        sb.append("        int endReader = offset + length;\n");
        List<DataField> fields = rowType.getFields();

        // Stage locals per field — initialised to default; presence bit tracks seen-flag so
        // nullable fields that never appeared end up null in the row.
        for (int i = 0; i < fields.size(); i++) {
            DataField f = fields.get(i);
            emitStageDeclaration(sb, i, f.getType());
            sb.append("        boolean _seen").append(i).append(" = false;\n");
        }

        sb.append("        while (src.readerIndex() < endReader) {\n");
        sb.append("            long tag = ").append(RUNTIME_CLASS).append(".readVarint(src);\n");
        sb.append("            int fieldNo = (int) (tag >>> 3);\n");
        sb.append("            int wireType = (int) (tag & ")
                .append(RUNTIME_CLASS)
                .append(".WIRE_TYPE_MASK);\n");
        sb.append("            switch (fieldNo) {\n");
        for (int i = 0; i < fields.size(); i++) {
            DataField f = fields.get(i);
            int tag = i + 1;
            sb.append("                case ").append(tag).append(":\n");
            emitStageRead(sb, "                    ", i, f.getType());
            sb.append("                    _seen").append(i).append(" = true;\n");
            sb.append("                    break;\n");
        }
        sb.append("                default:\n");
        sb.append("                    ")
                .append(RUNTIME_CLASS)
                .append(".skipValue(src, wireType);\n");
        sb.append("            }\n");
        sb.append("        }\n");

        sb.append("        dst.reset();\n");
        for (int i = 0; i < fields.size(); i++) {
            DataField f = fields.get(i);
            emitEmitStaged(sb, "        ", i, f.getType());
        }
        sb.append("        src.readerIndex(startReader + length);\n");
        sb.append("        dst.complete();\n");
        sb.append("        return length;\n");
        sb.append("    }\n");
    }

    private static void emitStageDeclaration(StringBuilder sb, int idx, DataType type) {
        DataTypeRoot r = type.getTypeRoot();
        String ind = "        ";
        switch (r) {
            case BOOLEAN:
                sb.append(ind).append("boolean _v").append(idx).append(" = false;\n");
                break;
            case INTEGER:
            case DATE:
                sb.append(ind).append("int _v").append(idx).append(" = 0;\n");
                break;
            case BIGINT:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                sb.append(ind).append("long _v").append(idx).append(" = 0L;\n");
                break;
            case FLOAT:
                sb.append(ind).append("float _v").append(idx).append(" = 0f;\n");
                break;
            case DOUBLE:
                sb.append(ind).append("double _v").append(idx).append(" = 0d;\n");
                break;
            case STRING:
                sb.append(ind).append("String _v").append(idx).append(" = \"\";\n");
                break;
            case BYTES:
                sb.append(ind).append("byte[] _v").append(idx).append(" = new byte[0];\n");
                break;
            default:
                throw new SchemaTranslationException(
                        "ProtobufCodecCompiler: unhandled scalar type " + r);
        }
    }

    private static void emitStageRead(StringBuilder sb, String indent, int idx, DataType type) {
        DataTypeRoot r = type.getTypeRoot();
        switch (r) {
            case BOOLEAN:
                sb.append(indent)
                        .append("_v")
                        .append(idx)
                        .append(" = ")
                        .append(RUNTIME_CLASS)
                        .append(".readVarint(src) != 0;\n");
                break;
            case INTEGER:
            case DATE:
                sb.append(indent)
                        .append("_v")
                        .append(idx)
                        .append(" = (int) ")
                        .append(RUNTIME_CLASS)
                        .append(".readVarint(src);\n");
                break;
            case BIGINT:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                sb.append(indent)
                        .append("_v")
                        .append(idx)
                        .append(" = ")
                        .append(RUNTIME_CLASS)
                        .append(".readVarint(src);\n");
                break;
            case FLOAT:
                sb.append(indent)
                        .append("_v")
                        .append(idx)
                        .append(" = Float.intBitsToFloat(")
                        .append(RUNTIME_CLASS)
                        .append(".readFixed32(src));\n");
                break;
            case DOUBLE:
                sb.append(indent)
                        .append("_v")
                        .append(idx)
                        .append(" = Double.longBitsToDouble(")
                        .append(RUNTIME_CLASS)
                        .append(".readFixed64(src));\n");
                break;
            case STRING:
                sb.append(indent)
                        .append("_v")
                        .append(idx)
                        .append(" = ")
                        .append(RUNTIME_CLASS)
                        .append(".readLengthPrefixedString(src);\n");
                break;
            case BYTES:
                sb.append(indent)
                        .append("_v")
                        .append(idx)
                        .append(" = ")
                        .append(RUNTIME_CLASS)
                        .append(".readLengthPrefixed(src);\n");
                break;
            default:
                throw new SchemaTranslationException(
                        "ProtobufCodecCompiler: unhandled scalar type " + r);
        }
    }

    private static void emitEmitStaged(StringBuilder sb, String indent, int idx, DataType type) {
        DataTypeRoot r = type.getTypeRoot();
        boolean nullable = type.isNullable();
        if (nullable) {
            sb.append(indent).append("if (!_seen").append(idx).append(") {\n");
            sb.append(indent).append("    dst.setNullAt(").append(idx).append(");\n");
            sb.append(indent).append("} else {\n");
            emitStagedWrite(sb, indent + "    ", idx, r);
            sb.append(indent).append("}\n");
        } else {
            emitStagedWrite(sb, indent, idx, r);
        }
    }

    private static void emitStagedWrite(StringBuilder sb, String indent, int idx, DataTypeRoot r) {
        switch (r) {
            case BOOLEAN:
                sb.append(indent).append("dst.writeBoolean(_v").append(idx).append(");\n");
                break;
            case INTEGER:
            case DATE:
                sb.append(indent).append("dst.writeInt(_v").append(idx).append(");\n");
                break;
            case BIGINT:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                sb.append(indent).append("dst.writeLong(_v").append(idx).append(");\n");
                break;
            case FLOAT:
                sb.append(indent).append("dst.writeFloat(_v").append(idx).append(");\n");
                break;
            case DOUBLE:
                sb.append(indent).append("dst.writeDouble(_v").append(idx).append(");\n");
                break;
            case STRING:
                sb.append(indent)
                        .append("dst.writeString(")
                        .append(BINARYSTRING_CLASS)
                        .append(".fromString(_v")
                        .append(idx)
                        .append("));\n");
                break;
            case BYTES:
                sb.append(indent).append("dst.writeBytes(_v").append(idx).append(");\n");
                break;
            default:
                throw new SchemaTranslationException(
                        "ProtobufCodecCompiler: unhandled scalar type " + r);
        }
    }

    private static void emitEncode(StringBuilder sb, RowType rowType) {
        sb.append("    public int encodeInto(")
                .append(BINARYROW_CLASS)
                .append(" src, ")
                .append(BYTEBUF_CLASS)
                .append(" dst) {\n");
        sb.append("        int startWriter = dst.writerIndex();\n");
        List<DataField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            DataField f = fields.get(i);
            int tag = i + 1;
            DataTypeRoot r = f.getType().getTypeRoot();
            boolean nullable = f.getType().isNullable();
            if (nullable) {
                sb.append("        if (!src.isNullAt(").append(i).append(")) {\n");
                emitEncodeField(sb, "            ", i, tag, r);
                sb.append("        }\n");
            } else {
                emitEncodeField(sb, "        ", i, tag, r);
            }
        }
        sb.append("        return dst.writerIndex() - startWriter;\n");
        sb.append("    }\n");
    }

    private static void emitEncodeField(
            StringBuilder sb, String indent, int idx, int tag, DataTypeRoot r) {
        switch (r) {
            case BOOLEAN:
                sb.append(indent)
                        .append(RUNTIME_CLASS)
                        .append(".writeTag(dst, ")
                        .append(tag)
                        .append(", ")
                        .append(RUNTIME_CLASS)
                        .append(".WIRE_VARINT);\n");
                sb.append(indent)
                        .append(RUNTIME_CLASS)
                        .append(".writeVarint(dst, src.getBoolean(")
                        .append(idx)
                        .append(") ? 1 : 0);\n");
                break;
            case INTEGER:
            case DATE:
                sb.append(indent)
                        .append(RUNTIME_CLASS)
                        .append(".writeTag(dst, ")
                        .append(tag)
                        .append(", ")
                        .append(RUNTIME_CLASS)
                        .append(".WIRE_VARINT);\n");
                sb.append(indent)
                        .append(RUNTIME_CLASS)
                        .append(".writeVarint(dst, src.getInt(")
                        .append(idx)
                        .append("));\n");
                break;
            case BIGINT:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                sb.append(indent)
                        .append(RUNTIME_CLASS)
                        .append(".writeTag(dst, ")
                        .append(tag)
                        .append(", ")
                        .append(RUNTIME_CLASS)
                        .append(".WIRE_VARINT);\n");
                sb.append(indent)
                        .append(RUNTIME_CLASS)
                        .append(".writeVarint(dst, src.getLong(")
                        .append(idx)
                        .append("));\n");
                break;
            case FLOAT:
                sb.append(indent)
                        .append(RUNTIME_CLASS)
                        .append(".writeTag(dst, ")
                        .append(tag)
                        .append(", ")
                        .append(RUNTIME_CLASS)
                        .append(".WIRE_FIXED32);\n");
                sb.append(indent)
                        .append(RUNTIME_CLASS)
                        .append(".writeFixed32(dst, Float.floatToRawIntBits(src.getFloat(")
                        .append(idx)
                        .append(")));\n");
                break;
            case DOUBLE:
                sb.append(indent)
                        .append(RUNTIME_CLASS)
                        .append(".writeTag(dst, ")
                        .append(tag)
                        .append(", ")
                        .append(RUNTIME_CLASS)
                        .append(".WIRE_FIXED64);\n");
                sb.append(indent)
                        .append(RUNTIME_CLASS)
                        .append(".writeFixed64(dst, Double.doubleToRawLongBits(src.getDouble(")
                        .append(idx)
                        .append(")));\n");
                break;
            case STRING:
                sb.append(indent)
                        .append(RUNTIME_CLASS)
                        .append(".writeTag(dst, ")
                        .append(tag)
                        .append(", ")
                        .append(RUNTIME_CLASS)
                        .append(".WIRE_LENGTH);\n");
                sb.append(indent)
                        .append(RUNTIME_CLASS)
                        .append(".writeLengthPrefixedBinaryString(dst, src.getString(")
                        .append(idx)
                        .append("));\n");
                break;
            case BYTES:
                sb.append(indent)
                        .append(RUNTIME_CLASS)
                        .append(".writeTag(dst, ")
                        .append(tag)
                        .append(", ")
                        .append(RUNTIME_CLASS)
                        .append(".WIRE_LENGTH);\n");
                sb.append(indent)
                        .append(RUNTIME_CLASS)
                        .append(".writeLengthPrefixed(dst, src.getBytes(")
                        .append(idx)
                        .append("));\n");
                break;
            default:
                throw new SchemaTranslationException(
                        "ProtobufCodecCompiler: unhandled scalar type " + r);
        }
    }
}
