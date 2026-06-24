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

import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Maps Google Protocol Buffers ({@code .proto} text, proto2 + proto3) to / from Fluss {@link
 * RowType}.
 *
 * <p>Type-mapping (plan §27.5, design doc 0008):
 *
 * <table>
 *   <caption>Protobuf to Fluss</caption>
 *   <tr><th>Protobuf</th><th>Fluss</th></tr>
 *   <tr><td>{@code bool}</td><td>{@code BOOLEAN}</td></tr>
 *   <tr><td>{@code int32 / sint32 / sfixed32}</td><td>{@code INT}</td></tr>
 *   <tr><td>{@code int64 / sint64 / sfixed64}</td><td>{@code BIGINT}</td></tr>
 *   <tr><td>{@code uint32 / uint64 / fixed32 / fixed64}</td><td>{@code BIGINT}</td></tr>
 *   <tr><td>{@code float}</td><td>{@code FLOAT}</td></tr>
 *   <tr><td>{@code double}</td><td>{@code DOUBLE}</td></tr>
 *   <tr><td>{@code string}</td><td>{@code STRING}</td></tr>
 *   <tr><td>{@code bytes}</td><td>{@code BYTES}</td></tr>
 *   <tr><td>{@code enum}</td><td>{@code STRING}</td></tr>
 *   <tr><td>nested {@code message}</td><td>{@code ROW<...>}</td></tr>
 *   <tr><td>{@code repeated T}</td><td>{@code ARRAY<T>}</td></tr>
 *   <tr><td>{@code map<K, V>}</td><td>{@code MAP<K, V>}</td></tr>
 *   <tr><td>{@code optional T} (proto3)</td><td>nullable {@code T}</td></tr>
 *   <tr><td>{@code google.protobuf.Timestamp}</td><td>{@code TIMESTAMP_LTZ(6)}</td></tr>
 *   <tr><td>{@code google.protobuf.Duration}</td><td>{@code BIGINT}</td></tr>
 *   <tr><td>{@code google.protobuf.*Value} wrappers</td><td>nullable primitive</td></tr>
 *   <tr><td>{@code oneof}</td><td><b>rejected</b> (sum types)</td></tr>
 *   <tr><td>{@code google.protobuf.Any}</td><td><b>rejected</b></td></tr>
 *   <tr><td>recursive message</td><td><b>rejected</b></td></tr>
 * </table>
 *
 * <p>The top-level message is taken to be the first (lexically earliest) {@code message}
 * declaration in the file. A subject that needs to register a specific nested message should
 * promote it to the top level in the submitted {@code .proto} text.
 *
 * @since 0.9
 */
@ThreadSafe
public final class ProtobufFormatTranslator implements FormatTranslator {

    /** Stable format id. */
    public static final String FORMAT_ID = "PROTOBUF";

    /** Well-known type FQNs we treat specially in the mapping table. */
    private static final String WKT_TIMESTAMP = "google.protobuf.Timestamp";

    private static final String WKT_DURATION = "google.protobuf.Duration";
    private static final String WKT_STRING_VALUE = "google.protobuf.StringValue";
    private static final String WKT_INT32_VALUE = "google.protobuf.Int32Value";
    private static final String WKT_INT64_VALUE = "google.protobuf.Int64Value";
    private static final String WKT_UINT32_VALUE = "google.protobuf.UInt32Value";
    private static final String WKT_UINT64_VALUE = "google.protobuf.UInt64Value";
    private static final String WKT_FLOAT_VALUE = "google.protobuf.FloatValue";
    private static final String WKT_DOUBLE_VALUE = "google.protobuf.DoubleValue";
    private static final String WKT_BOOL_VALUE = "google.protobuf.BoolValue";
    private static final String WKT_BYTES_VALUE = "google.protobuf.BytesValue";
    private static final String WKT_ANY = "google.protobuf.Any";

    @Override
    public String formatId() {
        return FORMAT_ID;
    }

    @Override
    public RowType translateTo(String schemaText) {
        if (schemaText == null || schemaText.trim().isEmpty()) {
            throw new SchemaTranslationException("Protobuf schema text must not be empty.");
        }
        ProtoParser.ProtoFile file = new ProtoParser(schemaText).parseFile();
        if (file.messages.isEmpty()) {
            throw new SchemaTranslationException(
                    "Protobuf file must declare at least one top-level message.");
        }
        // First message is the root.
        ProtoParser.ProtoMessage root = file.messages.get(0);
        Context ctx = new Context(file);
        RowType rowType = (RowType) translateMessage(root, ctx, new IdentityHashMap<>());
        return rowType;
    }

    @Override
    public String translateFrom(RowType rowType) {
        if (rowType == null) {
            throw new SchemaTranslationException("rowType must not be null.");
        }
        StringBuilder sb = new StringBuilder();
        sb.append("syntax = \"proto3\";\n\n");
        sb.append("package org.apache.fluss.generated;\n\n");
        writeMessage("Record", rowType, sb, 0);
        return sb.toString();
    }

    // ================================================================
    // schema → RowType
    // ================================================================

    private DataType translateMessage(
            ProtoParser.ProtoMessage m,
            Context ctx,
            IdentityHashMap<ProtoParser.ProtoMessage, Boolean> onStack) {
        if (onStack.containsKey(m)) {
            throw new SchemaTranslationException(
                    "Recursive message '" + m.name + "' is not supported.");
        }
        onStack.put(m, Boolean.TRUE);
        try {
            if (!m.oneofs.isEmpty()) {
                // Proto3 synthetic oneofs are generated for `optional` fields and have exactly one
                // member. Recognise and unwrap; any other oneof is a sum type → reject.
                for (ProtoParser.ProtoOneof o : m.oneofs) {
                    if (o.fields.size() != 1 || !o.name.startsWith("_")) {
                        throw new SchemaTranslationException(
                                "oneof '"
                                        + o.name
                                        + "' in message '"
                                        + m.name
                                        + "' is a sum type and not supported.");
                    }
                }
            }
            List<DataField> flussFields = new ArrayList<>();
            for (ProtoParser.ProtoField f : m.fields) {
                flussFields.add(translateField(f, m, ctx, onStack));
            }
            for (ProtoParser.ProtoOneof o : m.oneofs) {
                // Synthetic oneofs (proto3 optional): unwrap the single field as nullable.
                ProtoParser.ProtoField inner = o.fields.get(0);
                DataType fieldType = translateFieldType(inner, m, ctx, onStack);
                flussFields.add(DataTypes.FIELD(inner.name, fieldType.copy(true)));
            }
            return DataTypes.ROW(flussFields.toArray(new DataField[0]));
        } finally {
            onStack.remove(m);
        }
    }

    private DataField translateField(
            ProtoParser.ProtoField f,
            ProtoParser.ProtoMessage parent,
            Context ctx,
            IdentityHashMap<ProtoParser.ProtoMessage, Boolean> onStack) {
        DataType fieldType = translateFieldType(f, parent, ctx, onStack);
        // Wrapper types (Int32Value, StringValue, etc.) arrive pre-marked nullable — preserve
        // that. "optional" modifier marks proto3 presence-tracking scalar; treat as nullable.
        boolean nullable = fieldType.isNullable() || "optional".equals(f.modifier);
        // repeated and map are never nullable on the array/map wrapper.
        if ("repeated".equals(f.modifier) || "map".equals(f.type)) {
            nullable = false;
        }
        return DataTypes.FIELD(f.name, fieldType.copy(nullable));
    }

    private DataType translateFieldType(
            ProtoParser.ProtoField f,
            ProtoParser.ProtoMessage parent,
            Context ctx,
            IdentityHashMap<ProtoParser.ProtoMessage, Boolean> onStack) {
        if ("map".equals(f.type)) {
            DataType keyType = translatePrimitive(f.mapKeyType, f.name + ".<key>");
            DataTypeRoot keyRoot = keyType.getTypeRoot();
            if (keyRoot != DataTypeRoot.STRING
                    && keyRoot != DataTypeRoot.INTEGER
                    && keyRoot != DataTypeRoot.BIGINT) {
                throw new SchemaTranslationException(
                        "map key type '"
                                + f.mapKeyType
                                + "' not supported (need string/int32/int64)");
            }
            DataType valueType = translateTypeName(f.mapValueType, parent, ctx, onStack);
            return DataTypes.MAP(keyType.copy(false), valueType);
        }
        DataType type = translateTypeName(f.type, parent, ctx, onStack);
        if ("repeated".equals(f.modifier)) {
            return DataTypes.ARRAY(type.copy(false));
        }
        return type;
    }

    private DataType translateTypeName(
            String typeName,
            ProtoParser.ProtoMessage parent,
            Context ctx,
            IdentityHashMap<ProtoParser.ProtoMessage, Boolean> onStack) {
        DataType prim = maybePrimitive(typeName);
        if (prim != null) {
            return prim;
        }
        // Well-known types.
        String full = normaliseTypeName(typeName);
        switch (full) {
            case WKT_TIMESTAMP:
                return DataTypes.TIMESTAMP_LTZ(6);
            case WKT_DURATION:
                return DataTypes.BIGINT();
            case WKT_STRING_VALUE:
                return DataTypes.STRING().copy(true);
            case WKT_BYTES_VALUE:
                return DataTypes.BYTES().copy(true);
            case WKT_INT32_VALUE:
                return DataTypes.INT().copy(true);
            case WKT_INT64_VALUE:
                return DataTypes.BIGINT().copy(true);
            case WKT_UINT32_VALUE:
            case WKT_UINT64_VALUE:
                return DataTypes.BIGINT().copy(true);
            case WKT_FLOAT_VALUE:
                return DataTypes.FLOAT().copy(true);
            case WKT_DOUBLE_VALUE:
                return DataTypes.DOUBLE().copy(true);
            case WKT_BOOL_VALUE:
                return DataTypes.BOOLEAN().copy(true);
            case WKT_ANY:
                throw new SchemaTranslationException(
                        "google.protobuf.Any is not mappable (dynamic typing).");
            default:
                break;
        }
        // Nested / sibling / file-level message or enum.
        ProtoParser.ProtoMessage referenced = ctx.findMessage(typeName, parent);
        if (referenced != null) {
            return translateMessage(referenced, ctx, onStack);
        }
        ProtoParser.ProtoEnum referencedEnum = ctx.findEnum(typeName, parent);
        if (referencedEnum != null) {
            return DataTypes.STRING();
        }
        throw new SchemaTranslationException(
                "Unresolved type '" + typeName + "' referenced from message '" + parent.name + "'");
    }

    private static DataType translatePrimitive(String typeName, String where) {
        DataType t = maybePrimitive(typeName);
        if (t == null) {
            throw new SchemaTranslationException(
                    "Expected primitive type at " + where + ", got '" + typeName + "'");
        }
        return t;
    }

    private static DataType maybePrimitive(String typeName) {
        // Primitives return non-nullable by default; translateField restores nullability based on
        // the Protobuf modifier (optional → nullable) or wrapper-type-specific marking.
        switch (typeName) {
            case "bool":
                return DataTypes.BOOLEAN().copy(false);
            case "int32":
            case "sint32":
            case "sfixed32":
                return DataTypes.INT().copy(false);
            case "int64":
            case "sint64":
            case "sfixed64":
                return DataTypes.BIGINT().copy(false);
            case "uint32":
            case "uint64":
            case "fixed32":
            case "fixed64":
                return DataTypes.BIGINT().copy(false);
            case "float":
                return DataTypes.FLOAT().copy(false);
            case "double":
                return DataTypes.DOUBLE().copy(false);
            case "string":
                return DataTypes.STRING().copy(false);
            case "bytes":
                return DataTypes.BYTES().copy(false);
            default:
                return null;
        }
    }

    private static String normaliseTypeName(String name) {
        // Strip a leading '.' from fully-qualified names.
        return name.startsWith(".") ? name.substring(1) : name;
    }

    // ================================================================
    // RowType → .proto text
    // ================================================================

    private static void writeMessage(String name, RowType row, StringBuilder sb, int depth) {
        indent(sb, depth);
        sb.append("message ").append(name).append(" {\n");
        int tag = 1;
        for (DataField field : row.getFields()) {
            writeField(field, tag++, sb, depth + 1);
        }
        indent(sb, depth);
        sb.append("}\n");
    }

    private static void writeField(DataField field, int tag, StringBuilder sb, int depth) {
        String fieldName = field.getName();
        DataType type = field.getType();
        indent(sb, depth);
        // proto3: use `optional` for nullable scalars so presence is tracked; nullable wrappers
        // render as nullable user types.
        if (type.isNullable() && !isCompositeOrRepeated(type)) {
            sb.append("optional ");
        }
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                sb.append("bool ").append(fieldName);
                break;
            case INTEGER:
                sb.append("int32 ").append(fieldName);
                break;
            case BIGINT:
                sb.append("int64 ").append(fieldName);
                break;
            case FLOAT:
                sb.append("float ").append(fieldName);
                break;
            case DOUBLE:
                sb.append("double ").append(fieldName);
                break;
            case STRING:
                sb.append("string ").append(fieldName);
                break;
            case BYTES:
                sb.append("bytes ").append(fieldName);
                break;
            case DATE:
                sb.append("int32 ").append(fieldName);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                sb.append("google.protobuf.Timestamp ").append(fieldName);
                break;
            case ARRAY:
                sb.setLength(
                        sb.length()
                                - (type.isNullable() && !isCompositeOrRepeated(type)
                                        ? "optional ".length()
                                        : 0));
                indent(sb, depth);
                DataType elem = type.getChildren().get(0);
                sb.append("repeated ")
                        .append(primitiveProtoName(elem, fieldName))
                        .append(' ')
                        .append(fieldName);
                break;
            case MAP:
                {
                    MapType m = (MapType) type;
                    sb.append("map<")
                            .append(primitiveProtoName(m.getKeyType(), fieldName))
                            .append(", ")
                            .append(primitiveProtoName(m.getValueType(), fieldName))
                            .append("> ")
                            .append(fieldName);
                    break;
                }
            case ROW:
                {
                    // Inline a nested message declaration.
                    String msgName = capitalise(fieldName);
                    sb.append(msgName).append(' ').append(fieldName);
                    sb.append(" = ").append(tag).append(";\n");
                    writeMessage(msgName, (RowType) type, sb, depth);
                    return;
                }
            default:
                throw new SchemaTranslationException(
                        "DataType " + type + " is not representable in proto3.");
        }
        sb.append(" = ").append(tag).append(";\n");
    }

    private static boolean isCompositeOrRepeated(DataType type) {
        DataTypeRoot r = type.getTypeRoot();
        return r == DataTypeRoot.ARRAY || r == DataTypeRoot.MAP || r == DataTypeRoot.ROW;
    }

    private static String primitiveProtoName(DataType t, String where) {
        switch (t.getTypeRoot()) {
            case BOOLEAN:
                return "bool";
            case INTEGER:
                return "int32";
            case BIGINT:
                return "int64";
            case FLOAT:
                return "float";
            case DOUBLE:
                return "double";
            case STRING:
                return "string";
            case BYTES:
                return "bytes";
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return "google.protobuf.Timestamp";
            default:
                throw new SchemaTranslationException(
                        "Non-primitive inside array/map (" + where + "): " + t);
        }
    }

    private static void indent(StringBuilder sb, int depth) {
        for (int i = 0; i < depth; i++) {
            sb.append("  ");
        }
    }

    private static String capitalise(String s) {
        if (s.isEmpty()) {
            return s;
        }
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }

    // ================================================================
    // Reference-resolution context
    // ================================================================

    private static final class Context {
        private final ProtoParser.ProtoFile file;
        private final Map<String, ProtoParser.ProtoMessage> messagesByFqn = new LinkedHashMap<>();
        private final Map<String, ProtoParser.ProtoEnum> enumsByFqn = new LinkedHashMap<>();

        Context(ProtoParser.ProtoFile file) {
            this.file = file;
            String pkg = file.pkg;
            for (ProtoParser.ProtoMessage m : file.messages) {
                index(pkg, m, "");
            }
            for (ProtoParser.ProtoEnum e : file.enums) {
                indexEnum(pkg, e, "");
            }
        }

        private void index(String pkg, ProtoParser.ProtoMessage m, String parentPath) {
            String local = parentPath.isEmpty() ? m.name : parentPath + "." + m.name;
            String fqn = pkg == null || pkg.isEmpty() ? local : pkg + "." + local;
            messagesByFqn.put(fqn, m);
            messagesByFqn.put(local, m);
            for (ProtoParser.ProtoMessage nested : m.nestedMessages) {
                index(pkg, nested, local);
            }
            for (ProtoParser.ProtoEnum e : m.nestedEnums) {
                indexEnum(pkg, e, local);
            }
        }

        private void indexEnum(String pkg, ProtoParser.ProtoEnum e, String parentPath) {
            String local = parentPath.isEmpty() ? e.name : parentPath + "." + e.name;
            String fqn = pkg == null || pkg.isEmpty() ? local : pkg + "." + local;
            enumsByFqn.put(fqn, e);
            enumsByFqn.put(local, e);
        }

        ProtoParser.ProtoMessage findMessage(String typeName, ProtoParser.ProtoMessage from) {
            String normalised = normaliseTypeName(typeName);
            ProtoParser.ProtoMessage direct = messagesByFqn.get(normalised);
            if (direct != null) {
                return direct;
            }
            // Try nested inside `from`.
            for (ProtoParser.ProtoMessage nested : from.nestedMessages) {
                if (nested.name.equals(normalised)) {
                    return nested;
                }
            }
            return null;
        }

        ProtoParser.ProtoEnum findEnum(String typeName, ProtoParser.ProtoMessage from) {
            String normalised = normaliseTypeName(typeName);
            ProtoParser.ProtoEnum direct = enumsByFqn.get(normalised);
            if (direct != null) {
                return direct;
            }
            for (ProtoParser.ProtoEnum nested : from.nestedEnums) {
                if (nested.name.equals(normalised)) {
                    return nested;
                }
            }
            return null;
        }
    }

    // Avoid unused-import warnings on utility sets kept for future use.
    @SuppressWarnings("unused")
    private static final Set<String> UNUSED = new HashSet<>();
}
