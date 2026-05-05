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
 * Generates a specialised {@link RecordCodec} for a given Fluss {@link RowType} emitted + consumed
 * as SR-framed JSON Schema payloads.
 *
 * <p>Phase T-MF.4 scalar-only scope — matches {@link AvroCodecCompiler}'s T.1 shape. For each
 * top-level field the compiler emits:
 *
 * <ul>
 *   <li>decode: parse one JSON object out of the wire bytes, look up the field by name, dispatch
 *       per {@link DataTypeRoot} into the right {@code IndexedRowWriter} write method.
 *   <li>encode: read each field from the {@code BinaryRow}, dispatch into a Jackson {@code
 *       JsonGenerator} per type.
 * </ul>
 *
 * <p>Deferred: nested {@code ROW}, {@code ARRAY}, {@code MAP}, {@code DECIMAL}. Those need the
 * {@code InternalRow} + {@code RowSerializer} plumbing and land with the T-MF.4 nested-shape
 * follow-up.
 *
 * @since 0.9
 */
@Internal
public final class JsonCodecCompiler {

    private static final AtomicLong CLASS_ID_SEQ = new AtomicLong();

    private static final String GENERATED_PACKAGE = "org.apache.fluss.kafka.sr.typed.generated";
    private static final String RUNTIME_CLASS = "org.apache.fluss.kafka.sr.typed.JsonCodecRuntime";
    private static final String WRITER_CLASS = "org.apache.fluss.row.indexed.IndexedRowWriter";
    private static final String BYTEBUF_CLASS =
            "org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf";
    private static final String BINARYROW_CLASS = "org.apache.fluss.row.BinaryRow";
    private static final String BINARYSTRING_CLASS = "org.apache.fluss.row.BinaryString";
    private static final String JSONNODE_CLASS =
            "org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode";
    private static final String JSONGEN_CLASS =
            "org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator";
    private static final String ROWTYPE_CLASS = "org.apache.fluss.types.RowType";

    private JsonCodecCompiler() {}

    public static RecordCodec compile(RowType rowType, short schemaVersion) {
        if (rowType == null) {
            throw new SchemaTranslationException("JsonCodecCompiler requires a non-null RowType.");
        }
        for (DataField f : rowType.getFields()) {
            DataTypeRoot r = f.getType().getTypeRoot();
            if (r == DataTypeRoot.ARRAY
                    || r == DataTypeRoot.MAP
                    || r == DataTypeRoot.ROW
                    || r == DataTypeRoot.DECIMAL) {
                throw new SchemaTranslationException(
                        "JsonCodecCompiler (Phase T-MF.4 scalar-only) does not yet emit codec"
                                + " code for "
                                + r
                                + " fields (field '"
                                + f.getName()
                                + "'). Nested shapes land with the T-MF.4 nested-shape"
                                + " follow-up.");
            }
        }

        String className = "JsonCodec$" + CLASS_ID_SEQ.incrementAndGet();
        String fqcn = GENERATED_PACKAGE + "." + className;
        String source = emitSource(className, rowType, schemaVersion);

        try {
            SimpleCompiler compiler = new SimpleCompiler();
            compiler.setTargetVersion(8);
            compiler.setSourceVersion(8);
            compiler.setParentClassLoader(JsonCodecCompiler.class.getClassLoader());
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
        StringBuilder sb = new StringBuilder(4096);
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

    private static void emitDecode(StringBuilder sb, RowType rowType) {
        sb.append("    public int decodeInto(")
                .append(BYTEBUF_CLASS)
                .append(" src, int offset, int length, ")
                .append(WRITER_CLASS)
                .append(" dst) {\n");
        sb.append("        int startReader = src.readerIndex();\n");
        sb.append("        src.readerIndex(offset);\n");
        sb.append("        ")
                .append(JSONNODE_CLASS)
                .append(" obj = ")
                .append(RUNTIME_CLASS)
                .append(".parseObject(src, length);\n");
        sb.append("        dst.reset();\n");
        List<DataField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            emitDecodeField(sb, i, fields.get(i));
        }
        sb.append("        src.readerIndex(startReader + length);\n");
        sb.append("        dst.complete();\n");
        sb.append("        return length;\n");
        sb.append("    }\n");
    }

    private static void emitDecodeField(StringBuilder sb, int idx, DataField field) {
        DataType type = field.getType();
        String name = field.getName();
        String esc = escape(name);
        DataTypeRoot root = type.getTypeRoot();
        boolean nullable = type.isNullable();
        if (nullable) {
            sb.append("        if (")
                    .append(RUNTIME_CLASS)
                    .append(".isNull(obj, \"")
                    .append(esc)
                    .append("\")) {\n");
            sb.append("            dst.setNullAt(").append(idx).append(");\n");
            sb.append("        } else {\n");
            emitDecodeScalar(sb, "            ", idx, esc, root);
            sb.append("        }\n");
        } else {
            emitDecodeScalar(sb, "        ", idx, esc, root);
        }
    }

    private static void emitDecodeScalar(
            StringBuilder sb, String indent, int idx, String name, DataTypeRoot root) {
        switch (root) {
            case BOOLEAN:
                sb.append(indent)
                        .append("dst.writeBoolean(")
                        .append(RUNTIME_CLASS)
                        .append(".readBoolean(obj, \"")
                        .append(name)
                        .append("\"));\n");
                break;
            case INTEGER:
            case DATE:
                sb.append(indent)
                        .append("dst.writeInt(")
                        .append(RUNTIME_CLASS)
                        .append(".readInt(obj, \"")
                        .append(name)
                        .append("\"));\n");
                break;
            case BIGINT:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                sb.append(indent)
                        .append("dst.writeLong(")
                        .append(RUNTIME_CLASS)
                        .append(".readLong(obj, \"")
                        .append(name)
                        .append("\"));\n");
                break;
            case FLOAT:
                sb.append(indent)
                        .append("dst.writeFloat(")
                        .append(RUNTIME_CLASS)
                        .append(".readFloat(obj, \"")
                        .append(name)
                        .append("\"));\n");
                break;
            case DOUBLE:
                sb.append(indent)
                        .append("dst.writeDouble(")
                        .append(RUNTIME_CLASS)
                        .append(".readDouble(obj, \"")
                        .append(name)
                        .append("\"));\n");
                break;
            case STRING:
                sb.append(indent)
                        .append("dst.writeString(")
                        .append(BINARYSTRING_CLASS)
                        .append(".fromString(")
                        .append(RUNTIME_CLASS)
                        .append(".readString(obj, \"")
                        .append(name)
                        .append("\")));\n");
                break;
            case BYTES:
                sb.append(indent)
                        .append("dst.writeBytes(")
                        .append(RUNTIME_CLASS)
                        .append(".readBytes(obj, \"")
                        .append(name)
                        .append("\"));\n");
                break;
            default:
                throw new SchemaTranslationException(
                        "JsonCodecCompiler: unhandled scalar type " + root);
        }
    }

    private static void emitEncode(StringBuilder sb, RowType rowType) {
        sb.append("    public int encodeInto(")
                .append(BINARYROW_CLASS)
                .append(" src, ")
                .append(BYTEBUF_CLASS)
                .append(" dst) {\n");
        sb.append("        int startWriter = dst.writerIndex();\n");
        sb.append("        ")
                .append(JSONGEN_CLASS)
                .append(" gen = ")
                .append(RUNTIME_CLASS)
                .append(".openObject(dst);\n");
        List<DataField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            emitEncodeField(sb, i, fields.get(i));
        }
        sb.append("        ").append(RUNTIME_CLASS).append(".closeObject(gen);\n");
        sb.append("        return dst.writerIndex() - startWriter;\n");
        sb.append("    }\n");
    }

    private static void emitEncodeField(StringBuilder sb, int idx, DataField field) {
        DataType type = field.getType();
        String esc = escape(field.getName());
        DataTypeRoot root = type.getTypeRoot();
        boolean nullable = type.isNullable();
        if (nullable) {
            sb.append("        if (src.isNullAt(").append(idx).append(")) {\n");
            sb.append("            ")
                    .append(RUNTIME_CLASS)
                    .append(".writeNullField(gen, \"")
                    .append(esc)
                    .append("\");\n");
            sb.append("        } else {\n");
            emitEncodeScalar(sb, "            ", idx, esc, root);
            sb.append("        }\n");
        } else {
            emitEncodeScalar(sb, "        ", idx, esc, root);
        }
    }

    private static void emitEncodeScalar(
            StringBuilder sb, String indent, int idx, String name, DataTypeRoot root) {
        switch (root) {
            case BOOLEAN:
                sb.append(indent)
                        .append(RUNTIME_CLASS)
                        .append(".writeBooleanField(gen, \"")
                        .append(name)
                        .append("\", src.getBoolean(")
                        .append(idx)
                        .append("));\n");
                break;
            case INTEGER:
            case DATE:
                sb.append(indent)
                        .append(RUNTIME_CLASS)
                        .append(".writeIntField(gen, \"")
                        .append(name)
                        .append("\", src.getInt(")
                        .append(idx)
                        .append("));\n");
                break;
            case BIGINT:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                sb.append(indent)
                        .append(RUNTIME_CLASS)
                        .append(".writeLongField(gen, \"")
                        .append(name)
                        .append("\", src.getLong(")
                        .append(idx)
                        .append("));\n");
                break;
            case FLOAT:
                sb.append(indent)
                        .append(RUNTIME_CLASS)
                        .append(".writeFloatField(gen, \"")
                        .append(name)
                        .append("\", src.getFloat(")
                        .append(idx)
                        .append("));\n");
                break;
            case DOUBLE:
                sb.append(indent)
                        .append(RUNTIME_CLASS)
                        .append(".writeDoubleField(gen, \"")
                        .append(name)
                        .append("\", src.getDouble(")
                        .append(idx)
                        .append("));\n");
                break;
            case STRING:
                sb.append(indent)
                        .append(RUNTIME_CLASS)
                        .append(".writeStringField(gen, \"")
                        .append(name)
                        .append("\", src.getString(")
                        .append(idx)
                        .append(").toString());\n");
                break;
            case BYTES:
                sb.append(indent)
                        .append(RUNTIME_CLASS)
                        .append(".writeBytesField(gen, \"")
                        .append(name)
                        .append("\", src.getBytes(")
                        .append(idx)
                        .append("));\n");
                break;
            default:
                throw new SchemaTranslationException(
                        "JsonCodecCompiler: unhandled scalar type " + root);
        }
    }

    private static String escape(String s) {
        // Field names used as Java string literals — escape backslash and double-quote.
        StringBuilder out = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' || c == '"') {
                out.append('\\');
            }
            out.append(c);
        }
        return out.toString();
    }
}
