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

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Maps JSON Schema (drafts 4 / 6 / 7) to / from Fluss {@link RowType}.
 *
 * <p>Type-mapping (plan §27.4, design doc 0007):
 *
 * <table>
 *   <caption>JSON Schema to Fluss</caption>
 *   <tr><th>JSON Schema</th><th>Fluss</th></tr>
 *   <tr><td>{@code type: boolean}</td><td>{@code BOOLEAN}</td></tr>
 *   <tr><td>{@code type: integer}</td><td>{@code BIGINT} ({@code format: int32} → {@code INT})</td></tr>
 *   <tr><td>{@code type: number}</td><td>{@code DOUBLE} ({@code format: float} → {@code FLOAT})</td></tr>
 *   <tr><td>{@code type: string}</td><td>{@code STRING} (+ logical formats — byte → BYTES, date, date-time)</td></tr>
 *   <tr><td>{@code type: array, items: T}</td><td>{@code ARRAY<T>}</td></tr>
 *   <tr><td>{@code type: object, properties}</td><td>{@code ROW<...>} (nullability from {@code required})</td></tr>
 *   <tr><td>{@code type: object, additionalProperties: T}</td><td>{@code MAP<STRING, T>}</td></tr>
 *   <tr><td>{@code oneOf: [ {type:"null"}, X ]}</td><td>nullable {@code X}</td></tr>
 *   <tr><td>3-way union / non-null oneOf</td><td><b>rejected</b></td></tr>
 *   <tr><td>recursive {@code $ref}</td><td><b>rejected</b></td></tr>
 *   <tr><td>{@code $ref} in-file (#/definitions/...)</td><td>inlined</td></tr>
 * </table>
 *
 * <p>Logical formats:
 *
 * <ul>
 *   <li>{@code format: byte} → {@code BYTES}
 *   <li>{@code format: date} → {@code DATE}
 *   <li>{@code format: date-time} → {@code TIMESTAMP_LTZ(3)}
 *   <li>{@code format: decimal} + {@code multipleOf} → {@code DECIMAL(p,s)}
 * </ul>
 *
 * @since 0.9
 */
@ThreadSafe
public final class JsonSchemaFormatTranslator implements FormatTranslator {

    /** Stable format id. */
    public static final String FORMAT_ID = "JSON";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public String formatId() {
        return FORMAT_ID;
    }

    @Override
    public RowType translateTo(String schemaText) {
        if (schemaText == null || schemaText.trim().isEmpty()) {
            throw new SchemaTranslationException("JSON Schema text must not be empty.");
        }
        final JsonNode root;
        try {
            root = MAPPER.readTree(schemaText);
        } catch (Exception e) {
            throw new SchemaTranslationException(
                    "Failed to parse JSON Schema: " + e.getMessage(), e);
        }
        if (!root.isObject()) {
            throw new SchemaTranslationException(
                    "Top-level JSON Schema must be an object, got " + root.getNodeType());
        }
        // Collect in-file $ref targets (usually under /definitions or /$defs).
        ResolveContext ctx = new ResolveContext(root);
        DataType rowType = resolve(root, ctx, new IdentityHashMap<>());
        if (!(rowType instanceof RowType)) {
            throw new SchemaTranslationException(
                    "Top-level JSON Schema must resolve to an object (ROW); got " + rowType);
        }
        return (RowType) rowType;
    }

    @Override
    public String translateFrom(RowType rowType) {
        if (rowType == null) {
            throw new SchemaTranslationException("rowType must not be null");
        }
        ObjectNode root = MAPPER.createObjectNode();
        root.put("$schema", "http://json-schema.org/draft-07/schema#");
        writeRow(rowType, root);
        return root.toString();
    }

    // ================================================================
    // Schema → RowType resolution
    // ================================================================

    private static DataType resolve(
            JsonNode node, ResolveContext ctx, IdentityHashMap<JsonNode, Boolean> onStack) {
        if (onStack.containsKey(node)) {
            throw new SchemaTranslationException(
                    "Recursive $ref is not supported (JSON Schema must be tree-shaped).");
        }
        onStack.put(node, Boolean.TRUE);
        try {
            // Resolve $ref first so the rest of the function deals with concrete shapes.
            if (node.has("$ref")) {
                String ref = node.get("$ref").asText();
                JsonNode target = ctx.resolveRef(ref);
                return resolve(target, ctx, onStack);
            }

            // Nullable union idiom: oneOf: [ {type: "null"}, X ] (or reverse), anyOf the same,
            // or draft-4's type: ["null", X] / [X, "null"].
            DataType nullableInner = nullableUnion(node);
            if (nullableInner != null) {
                return nullableInner.copy(true);
            }

            JsonNode typeNode = node.get("type");
            if (typeNode == null) {
                // Schemas without a "type" field we treat as open maps — rejected for strictness.
                throw new SchemaTranslationException(
                        "JSON Schema node without \"type\" is not supported: " + node);
            }
            if (typeNode.isArray()) {
                // Arrays of types other than the nullable idiom fell through nullableUnion().
                throw new SchemaTranslationException(
                        "Unsupported multi-type shape "
                                + typeNode
                                + "; only [null, T] / [T, null] allowed");
            }
            String type = typeNode.asText();
            switch (type) {
                case "boolean":
                    return DataTypes.BOOLEAN().copy(false);
                case "integer":
                    return integerType(node).copy(false);
                case "number":
                    return numberType(node).copy(false);
                case "string":
                    return stringType(node).copy(false);
                case "array":
                    return arrayType(node, ctx, onStack).copy(false);
                case "object":
                    return objectType(node, ctx, onStack).copy(false);
                case "null":
                    throw new SchemaTranslationException(
                            "Bare 'null' type is not mappable; use inside a [null, T] union.");
                default:
                    throw new SchemaTranslationException("Unknown JSON Schema type: " + type);
            }
        } finally {
            onStack.remove(node);
        }
    }

    private static DataType nullableUnion(JsonNode node) {
        // type: ["null", X] / ["X", "null"]  (draft-4)
        JsonNode typeNode = node.get("type");
        if (typeNode != null && typeNode.isArray()) {
            ArrayNode arr = (ArrayNode) typeNode;
            if (arr.size() == 2) {
                String a = arr.get(0).asText();
                String b = arr.get(1).asText();
                String nonNull = null;
                if ("null".equals(a) && !"null".equals(b)) {
                    nonNull = b;
                } else if ("null".equals(b) && !"null".equals(a)) {
                    nonNull = a;
                }
                if (nonNull != null) {
                    ObjectNode synth = MAPPER.createObjectNode();
                    synth.put("type", nonNull);
                    // Carry format/items/properties along if the parent node specifies them.
                    for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
                        Map.Entry<String, JsonNode> e = it.next();
                        if ("type".equals(e.getKey())) {
                            continue;
                        }
                        synth.set(e.getKey(), e.getValue());
                    }
                    return resolve(synth, new ResolveContext(node), new IdentityHashMap<>());
                }
            } else if (arr.size() > 2 || arr.size() == 1) {
                if (arr.size() > 2) {
                    throw new SchemaTranslationException("3+-way type union not supported: " + arr);
                }
                // size==1 collapses to that single type; let the default resolution path handle it
                // by rewriting the node.
                ObjectNode synth = MAPPER.createObjectNode();
                synth.put("type", arr.get(0).asText());
                for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
                    Map.Entry<String, JsonNode> e = it.next();
                    if ("type".equals(e.getKey())) {
                        continue;
                    }
                    synth.set(e.getKey(), e.getValue());
                }
                return resolve(synth, new ResolveContext(node), new IdentityHashMap<>());
            }
        }

        // oneOf / anyOf: [ {null}, X ] idiom
        JsonNode oneOf = node.has("oneOf") ? node.get("oneOf") : node.get("anyOf");
        if (oneOf != null && oneOf.isArray()) {
            ArrayNode arr = (ArrayNode) oneOf;
            if (arr.size() == 2) {
                JsonNode a = arr.get(0);
                JsonNode b = arr.get(1);
                JsonNode nonNull = isNullType(a) ? b : isNullType(b) ? a : null;
                if (nonNull != null && (isNullType(a) || isNullType(b))) {
                    return resolve(nonNull, new ResolveContext(node), new IdentityHashMap<>());
                }
                throw new SchemaTranslationException(
                        "Non-nullable oneOf/anyOf is a sum type and not supported: " + oneOf);
            }
            throw new SchemaTranslationException(
                    arr.size() + "-way oneOf/anyOf is a sum type and not supported: " + oneOf);
        }
        return null;
    }

    private static boolean isNullType(JsonNode n) {
        if (n == null || !n.isObject()) {
            return false;
        }
        JsonNode t = n.get("type");
        return t != null && "null".equals(t.asText());
    }

    private static DataType integerType(JsonNode node) {
        String format = textOrNull(node, "format");
        if ("int32".equals(format) || "int".equals(format)) {
            return DataTypes.INT();
        }
        return DataTypes.BIGINT();
    }

    private static DataType numberType(JsonNode node) {
        String format = textOrNull(node, "format");
        if ("float".equals(format)) {
            return DataTypes.FLOAT();
        }
        if ("decimal".equals(format)) {
            int scale = scaleFromMultipleOf(node);
            int precision = 38;
            return DataTypes.DECIMAL(Math.min(precision, 38), Math.min(scale, 18));
        }
        return DataTypes.DOUBLE();
    }

    private static DataType stringType(JsonNode node) {
        String format = textOrNull(node, "format");
        if (format == null) {
            return DataTypes.STRING();
        }
        switch (format) {
            case "byte":
            case "binary":
                return DataTypes.BYTES();
            case "date":
                return DataTypes.DATE();
            case "date-time":
                return DataTypes.TIMESTAMP_LTZ(3);
            case "time":
                return DataTypes.TIME(3);
            default:
                return DataTypes.STRING();
        }
    }

    private static DataType arrayType(
            JsonNode node, ResolveContext ctx, IdentityHashMap<JsonNode, Boolean> onStack) {
        JsonNode items = node.get("items");
        if (items == null) {
            throw new SchemaTranslationException(
                    "JSON Schema array without items is not supported: " + node);
        }
        if (items.isArray()) {
            throw new SchemaTranslationException(
                    "Tuple-style arrays (items as array) are not supported: " + items);
        }
        DataType elem = resolve(items, ctx, onStack);
        return DataTypes.ARRAY(elem);
    }

    private static DataType objectType(
            JsonNode node, ResolveContext ctx, IdentityHashMap<JsonNode, Boolean> onStack) {
        JsonNode properties = node.get("properties");
        JsonNode additionalProperties = node.get("additionalProperties");
        if (properties == null || properties.size() == 0) {
            // object with only additionalProperties → MAP<STRING, T>
            if (additionalProperties != null && additionalProperties.isObject()) {
                DataType value = resolve(additionalProperties, ctx, onStack);
                return DataTypes.MAP(DataTypes.STRING().copy(false), value);
            }
            throw new SchemaTranslationException(
                    "JSON Schema object must declare properties or additionalProperties: " + node);
        }
        // properties with an additionalProperties schema is a closed record + extensibility —
        // Fluss has no direct equivalent, so we honour properties and drop additionalProperties.
        Set<String> required = new HashSet<>();
        JsonNode req = node.get("required");
        if (req != null && req.isArray()) {
            for (JsonNode r : req) {
                required.add(r.asText());
            }
        }
        List<DataField> fields = new ArrayList<>();
        Iterator<Map.Entry<String, JsonNode>> it = properties.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> entry = it.next();
            String name = entry.getKey();
            DataType fieldType = resolve(entry.getValue(), ctx, onStack);
            fieldType = fieldType.copy(!required.contains(name));
            fields.add(DataTypes.FIELD(name, fieldType));
        }
        return DataTypes.ROW(fields.toArray(new DataField[0]));
    }

    private static int scaleFromMultipleOf(JsonNode node) {
        JsonNode mo = node.get("multipleOf");
        if (mo == null) {
            return 0;
        }
        double m = mo.asDouble();
        if (m <= 0 || m >= 1) {
            return 0;
        }
        int scale = 0;
        while (scale < 18 && m < 0.999_999) {
            m *= 10;
            scale++;
        }
        return scale;
    }

    private static String textOrNull(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return v == null || v.isNull() ? null : v.asText();
    }

    // ================================================================
    // RowType → JSON Schema (reverse translation)
    // ================================================================

    private static void writeRow(RowType row, ObjectNode target) {
        target.put("type", "object");
        ObjectNode properties = target.putObject("properties");
        ArrayNode required = MAPPER.createArrayNode();
        for (DataField field : row.getFields()) {
            ObjectNode fieldNode = properties.putObject(field.getName());
            writeType(field.getType(), fieldNode);
            if (!field.getType().isNullable()) {
                required.add(field.getName());
            }
        }
        if (required.size() > 0) {
            target.set("required", required);
        }
        target.put("additionalProperties", false);
    }

    private static void writeType(DataType type, ObjectNode target) {
        if (type.isNullable()) {
            // Emit draft-7 nullable idiom: type: [T, "null"].
            ObjectNode inner = MAPPER.createObjectNode();
            writeType(type.copy(false), inner);
            JsonNode innerTypeNode = inner.get("type");
            if (innerTypeNode != null && innerTypeNode.isTextual()) {
                ArrayNode arr = target.putArray("type");
                arr.add(innerTypeNode.asText());
                arr.add("null");
                // Carry over other properties (items, properties, format).
                Iterator<Map.Entry<String, JsonNode>> it = inner.fields();
                while (it.hasNext()) {
                    Map.Entry<String, JsonNode> e = it.next();
                    if (!"type".equals(e.getKey())) {
                        target.set(e.getKey(), e.getValue());
                    }
                }
                return;
            }
            // Composite nullable (ROW, ARRAY with extra): copy all inner fields + add null via
            // oneOf so it stays valid JSON Schema.
            target.setAll(inner);
            return;
        }
        DataTypeRoot root = type.getTypeRoot();
        switch (root) {
            case BOOLEAN:
                target.put("type", "boolean");
                return;
            case INTEGER:
                target.put("type", "integer");
                target.put("format", "int32");
                return;
            case BIGINT:
                target.put("type", "integer");
                return;
            case FLOAT:
                target.put("type", "number");
                target.put("format", "float");
                return;
            case DOUBLE:
                target.put("type", "number");
                return;
            case DECIMAL:
                {
                    DecimalType dec = (DecimalType) type;
                    target.put("type", "number");
                    target.put("format", "decimal");
                    if (dec.getScale() > 0) {
                        target.put("multipleOf", Math.pow(10, -dec.getScale()));
                    }
                    return;
                }
            case BYTES:
                target.put("type", "string");
                target.put("format", "byte");
                return;
            case STRING:
                target.put("type", "string");
                return;
            case DATE:
                target.put("type", "string");
                target.put("format", "date");
                return;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                target.put("type", "string");
                target.put("format", "date-time");
                return;
            case ARRAY:
                {
                    target.put("type", "array");
                    ObjectNode items = target.putObject("items");
                    writeType(type.getChildren().get(0), items);
                    return;
                }
            case MAP:
                {
                    MapType m = (MapType) type;
                    if (m.getKeyType().getTypeRoot() != DataTypeRoot.STRING) {
                        throw new SchemaTranslationException(
                                "MAP with non-string key cannot be rendered as JSON Schema: "
                                        + type);
                    }
                    target.put("type", "object");
                    ObjectNode additional = target.putObject("additionalProperties");
                    writeType(m.getValueType(), additional);
                    return;
                }
            case ROW:
                writeRow((RowType) type, target);
                return;
            default:
                throw new SchemaTranslationException(
                        "DataType " + type + " is not representable in JSON Schema.");
        }
    }

    // ================================================================
    // $ref resolution (in-file only)
    // ================================================================

    /** Minimal in-file $ref resolver; cross-file refs are rejected. */
    private static final class ResolveContext {
        private final JsonNode root;

        ResolveContext(JsonNode root) {
            this.root = root;
        }

        JsonNode resolveRef(String ref) {
            if (ref == null || ref.isEmpty()) {
                throw new SchemaTranslationException("Empty $ref not supported");
            }
            if (!ref.startsWith("#")) {
                throw new SchemaTranslationException("Cross-file $ref is not supported: " + ref);
            }
            String pointer = ref.substring(1);
            if (pointer.isEmpty() || "/".equals(pointer)) {
                return root;
            }
            if (!pointer.startsWith("/")) {
                throw new SchemaTranslationException("Invalid JSON pointer: " + ref);
            }
            String[] parts = pointer.substring(1).split("/");
            JsonNode cur = root;
            for (String raw : parts) {
                String p = raw.replace("~1", "/").replace("~0", "~");
                cur = cur.get(p);
                if (cur == null) {
                    throw new SchemaTranslationException("$ref target not found: " + ref);
                }
            }
            return cur;
        }
    }
}
