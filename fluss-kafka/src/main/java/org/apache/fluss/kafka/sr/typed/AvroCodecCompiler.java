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
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;

import org.apache.avro.Schema;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.SimpleCompiler;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates a specialised {@link RecordCodec} for a given {@code (Avro schema, Fluss RowType)} pair
 * at runtime, compiling it with Janino.
 *
 * <p>For each field of the top-level Avro record, the compiler emits:
 *
 * <ul>
 *   <li>a decode op that reads bytes directly from a {@link
 *       org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf} via {@link AvroCodecRuntime} and
 *       writes into the {@link org.apache.fluss.row.indexed.IndexedRowWriter};
 *   <li>an encode op that reads from a {@link org.apache.fluss.row.BinaryRow} and writes to a
 *       {@code ByteBuf}.
 * </ul>
 *
 * <p><b>Supported field types in Phase T.1:</b> boolean, int, long, float, double, bytes, string,
 * and the {@code [null, T]} nullable-union idiom over any of those scalars. Avro {@code
 * decimal(p,s)}, {@code date}, and {@code timestamp-{millis,micros}} logical types are supported at
 * the translator level; the compiled codec handles them as their underlying int / long / bytes.
 *
 * <p><b>Deferred to T.2 / T.4:</b> array, map, nested record, enum, fixed. The compiler throws
 * {@link SchemaTranslationException} for those shapes, with a message pointing to the next phase.
 *
 * @since 0.9
 */
@Internal
public final class AvroCodecCompiler {

    /** Monotonically-increasing id to mint unique class names per compiled codec. */
    private static final AtomicLong CLASS_ID_SEQ = new AtomicLong();

    private static final String GENERATED_PACKAGE = "org.apache.fluss.kafka.sr.typed.generated";
    private static final String RUNTIME_CLASS = "org.apache.fluss.kafka.sr.typed.AvroCodecRuntime";
    private static final String WRITER_CLASS = "org.apache.fluss.row.indexed.IndexedRowWriter";
    private static final String BYTEBUF_CLASS =
            "org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf";
    private static final String BINARYROW_CLASS = "org.apache.fluss.row.BinaryRow";
    private static final String BINARYSTRING_CLASS = "org.apache.fluss.row.BinaryString";
    private static final String ROWTYPE_CLASS = "org.apache.fluss.types.RowType";

    private AvroCodecCompiler() {}

    /**
     * Compile a codec.
     *
     * @param avroSchema the parsed Avro schema (must be a record)
     * @param rowType the Fluss row type the codec will produce; field count must match the Avro
     *     record's field count
     * @param schemaVersion the schema-registry version (short). Pass 0 if not yet assigned.
     * @return a fresh, ready-to-use codec
     */
    public static RecordCodec compile(Schema avroSchema, RowType rowType, short schemaVersion) {
        if (avroSchema == null || avroSchema.getType() != Schema.Type.RECORD) {
            throw new SchemaTranslationException(
                    "AvroCodecCompiler requires a RECORD schema at the top level.");
        }
        List<Schema.Field> avroFields = avroSchema.getFields();
        if (avroFields.size() != rowType.getFieldCount()) {
            throw new SchemaTranslationException(
                    "Avro record has "
                            + avroFields.size()
                            + " fields but RowType has "
                            + rowType.getFieldCount());
        }

        String className = "AvroCodec$" + CLASS_ID_SEQ.incrementAndGet();
        String fqcn = GENERATED_PACKAGE + "." + className;
        String source = emitSource(className, avroFields, rowType, schemaVersion);

        try {
            SimpleCompiler compiler = new SimpleCompiler();
            compiler.setTargetVersion(8);
            compiler.setSourceVersion(8);
            compiler.setParentClassLoader(AvroCodecCompiler.class.getClassLoader());
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
    // Source emission
    // ------------------------------------------------------------------------------------------

    private static String emitSource(
            String className, List<Schema.Field> avroFields, RowType rowType, short schemaVersion) {
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

        emitDecode(sb, avroFields, rowType);
        emitEncode(sb, avroFields, rowType);

        sb.append("}\n");
        return sb.toString();
    }

    private static void emitDecode(
            StringBuilder sb, List<Schema.Field> avroFields, RowType rowType) {
        sb.append("    public int decodeInto(")
                .append(BYTEBUF_CLASS)
                .append(" src, int offset, int length, ")
                .append(WRITER_CLASS)
                .append(" dst) {\n");
        sb.append("        int startReader = src.readerIndex();\n");
        sb.append("        src.readerIndex(offset);\n");
        sb.append("        dst.reset();\n");

        for (int i = 0; i < avroFields.size(); i++) {
            Schema.Field f = avroFields.get(i);
            DataType fluss = rowType.getTypeAt(i);
            emitDecodeField(sb, i, f, fluss);
        }
        sb.append("        int consumed = src.readerIndex() - offset;\n");
        // Restore the original reader index so callers who pass an absolute offset aren't
        // surprised.
        sb.append("        if (consumed != length) {\n");
        sb.append(
                "            throw new IllegalStateException(\"Avro codec consumed \" + consumed + \" bytes but frame declared \" + length);\n");
        sb.append("        }\n");
        sb.append("        src.readerIndex(startReader + length);\n");
        sb.append("        dst.complete();\n");
        sb.append("        return consumed;\n");
        sb.append("    }\n");
    }

    private static void emitDecodeField(
            StringBuilder sb, int idx, Schema.Field f, DataType flussType) {
        Schema s = f.schema();
        boolean nullable = flussType.isNullable();
        if (nullable) {
            // Union [null, T] — read branch idx first. Spec says the order is as declared; we
            // support both [null, T] and [T, null].
            int nullBranch = nullBranchIndex(s);
            if (nullBranch < 0) {
                throw new SchemaTranslationException(
                        "Nullable field '"
                                + f.name()
                                + "' must be a [null, T] or [T, null] Avro union.");
            }
            Schema nonNull = unionNonNullBranch(s);
            sb.append("        {\n");
            sb.append("            int br = ")
                    .append(RUNTIME_CLASS)
                    .append(".readUnionIndex(src);\n");
            sb.append("            if (br == ").append(nullBranch).append(") {\n");
            sb.append("                dst.setNullAt(").append(idx).append(");\n");
            sb.append("            } else {\n");
            emitDecodeScalar(sb, "                ", idx, nonNull, flussType);
            sb.append("            }\n");
            sb.append("        }\n");
        } else {
            sb.append("        ");
            emitDecodeScalar(sb, "", idx, s, flussType);
        }
    }

    private static void emitDecodeScalar(
            StringBuilder sb, String indent, int idx, Schema avro, DataType fluss) {
        Schema.Type at = avro.getType();
        DataTypeRoot fr = fluss.getTypeRoot();
        // Indent block with the base indent on each emitted line.
        if (at == Schema.Type.BOOLEAN && fr == DataTypeRoot.BOOLEAN) {
            sb.append(indent)
                    .append("dst.writeBoolean(")
                    .append(RUNTIME_CLASS)
                    .append(".readBoolean(src));\n");
        } else if (at == Schema.Type.INT
                && (fr == DataTypeRoot.INTEGER || fr == DataTypeRoot.DATE)) {
            sb.append(indent)
                    .append("dst.writeInt(")
                    .append(RUNTIME_CLASS)
                    .append(".readInt(src));\n");
        } else if (at == Schema.Type.LONG
                && (fr == DataTypeRoot.BIGINT
                        || fr == DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
            sb.append(indent)
                    .append("dst.writeLong(")
                    .append(RUNTIME_CLASS)
                    .append(".readLong(src));\n");
        } else if (at == Schema.Type.FLOAT && fr == DataTypeRoot.FLOAT) {
            sb.append(indent)
                    .append("dst.writeFloat(")
                    .append(RUNTIME_CLASS)
                    .append(".readFloat(src));\n");
        } else if (at == Schema.Type.DOUBLE && fr == DataTypeRoot.DOUBLE) {
            sb.append(indent)
                    .append("dst.writeDouble(")
                    .append(RUNTIME_CLASS)
                    .append(".readDouble(src));\n");
        } else if (at == Schema.Type.STRING && fr == DataTypeRoot.STRING) {
            // Read length-prefixed bytes as UTF-8, wrap into a BinaryString.
            sb.append(indent)
                    .append("byte[] _s")
                    .append(idx)
                    .append(" = ")
                    .append(RUNTIME_CLASS)
                    .append(".readBytes(src);\n");
            sb.append(indent)
                    .append("dst.writeString(")
                    .append(BINARYSTRING_CLASS)
                    .append(".fromBytes(_s")
                    .append(idx)
                    .append("));\n");
        } else if (at == Schema.Type.BYTES && fr == DataTypeRoot.BYTES) {
            sb.append(indent)
                    .append("dst.writeBytes(")
                    .append(RUNTIME_CLASS)
                    .append(".readBytes(src));\n");
        } else {
            throw new SchemaTranslationException(
                    "AvroCodecCompiler (Phase T.1) does not yet handle Avro "
                            + at
                            + " -> Fluss "
                            + fr
                            + ". Arrays, maps, nested records, enum, fixed, and decimal are "
                            + "scheduled for later phases.");
        }
    }

    private static void emitEncode(
            StringBuilder sb, List<Schema.Field> avroFields, RowType rowType) {
        sb.append("    public int encodeInto(")
                .append(BINARYROW_CLASS)
                .append(" src, ")
                .append(BYTEBUF_CLASS)
                .append(" dst) {\n");
        sb.append("        int startWriter = dst.writerIndex();\n");
        for (int i = 0; i < avroFields.size(); i++) {
            Schema.Field f = avroFields.get(i);
            DataType fluss = rowType.getTypeAt(i);
            emitEncodeField(sb, i, f, fluss);
        }
        sb.append("        return dst.writerIndex() - startWriter;\n");
        sb.append("    }\n");
    }

    private static void emitEncodeField(
            StringBuilder sb, int idx, Schema.Field f, DataType flussType) {
        Schema s = f.schema();
        boolean nullable = flussType.isNullable();
        if (nullable) {
            int nullBranch = nullBranchIndex(s);
            int tBranch = 1 - nullBranch;
            Schema nonNull = unionNonNullBranch(s);
            sb.append("        if (src.isNullAt(").append(idx).append(")) {\n");
            sb.append("            ")
                    .append(RUNTIME_CLASS)
                    .append(".writeUnionIndex(dst, ")
                    .append(nullBranch)
                    .append(");\n");
            sb.append("        } else {\n");
            sb.append("            ")
                    .append(RUNTIME_CLASS)
                    .append(".writeUnionIndex(dst, ")
                    .append(tBranch)
                    .append(");\n");
            emitEncodeScalar(sb, "            ", idx, nonNull, flussType);
            sb.append("        }\n");
        } else {
            emitEncodeScalar(sb, "        ", idx, s, flussType);
        }
    }

    private static void emitEncodeScalar(
            StringBuilder sb, String indent, int idx, Schema avro, DataType fluss) {
        Schema.Type at = avro.getType();
        DataTypeRoot fr = fluss.getTypeRoot();
        if (at == Schema.Type.BOOLEAN && fr == DataTypeRoot.BOOLEAN) {
            sb.append(indent)
                    .append(RUNTIME_CLASS)
                    .append(".writeBoolean(dst, src.getBoolean(")
                    .append(idx)
                    .append("));\n");
        } else if (at == Schema.Type.INT
                && (fr == DataTypeRoot.INTEGER || fr == DataTypeRoot.DATE)) {
            sb.append(indent)
                    .append(RUNTIME_CLASS)
                    .append(".writeInt(dst, src.getInt(")
                    .append(idx)
                    .append("));\n");
        } else if (at == Schema.Type.LONG
                && (fr == DataTypeRoot.BIGINT
                        || fr == DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
            sb.append(indent)
                    .append(RUNTIME_CLASS)
                    .append(".writeLong(dst, src.getLong(")
                    .append(idx)
                    .append("));\n");
        } else if (at == Schema.Type.FLOAT && fr == DataTypeRoot.FLOAT) {
            sb.append(indent)
                    .append(RUNTIME_CLASS)
                    .append(".writeFloat(dst, src.getFloat(")
                    .append(idx)
                    .append("));\n");
        } else if (at == Schema.Type.DOUBLE && fr == DataTypeRoot.DOUBLE) {
            sb.append(indent)
                    .append(RUNTIME_CLASS)
                    .append(".writeDouble(dst, src.getDouble(")
                    .append(idx)
                    .append("));\n");
        } else if (at == Schema.Type.STRING && fr == DataTypeRoot.STRING) {
            sb.append(indent)
                    .append(RUNTIME_CLASS)
                    .append(".writeBinaryString(dst, src.getString(")
                    .append(idx)
                    .append("));\n");
        } else if (at == Schema.Type.BYTES && fr == DataTypeRoot.BYTES) {
            sb.append(indent)
                    .append(RUNTIME_CLASS)
                    .append(".writeBytes(dst, src.getBytes(")
                    .append(idx)
                    .append("));\n");
        } else {
            throw new SchemaTranslationException(
                    "AvroCodecCompiler (Phase T.1) does not yet handle Avro "
                            + at
                            + " -> Fluss "
                            + fr);
        }
    }

    // ------------------------------------------------------------------------------------------
    // Union helpers
    // ------------------------------------------------------------------------------------------

    /** Index of the {@code null} branch in a 2-way union, or -1 if none. */
    private static int nullBranchIndex(Schema s) {
        if (s.getType() != Schema.Type.UNION || s.getTypes().size() != 2) {
            return -1;
        }
        if (s.getTypes().get(0).getType() == Schema.Type.NULL) {
            return 0;
        }
        if (s.getTypes().get(1).getType() == Schema.Type.NULL) {
            return 1;
        }
        return -1;
    }

    /** The non-null branch of a [null, T] or [T, null] union. */
    private static Schema unionNonNullBranch(Schema s) {
        Schema a = s.getTypes().get(0);
        Schema b = s.getTypes().get(1);
        return a.getType() == Schema.Type.NULL ? b : a;
    }
}
