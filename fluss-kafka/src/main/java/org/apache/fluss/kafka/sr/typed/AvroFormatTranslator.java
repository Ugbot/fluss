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

import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/**
 * Maps Apache Avro schemas to / from Fluss {@link RowType}.
 *
 * <p>Type-mapping decisions (plan §22.5):
 *
 * <table>
 *   <caption>Avro to Fluss</caption>
 *   <tr><th>Avro</th><th>Fluss</th></tr>
 *   <tr><td>{@code null} (only inside union)</td><td>--- (nullability marker)</td></tr>
 *   <tr><td>{@code boolean}</td><td>{@code BOOLEAN}</td></tr>
 *   <tr><td>{@code int}</td><td>{@code INT}</td></tr>
 *   <tr><td>{@code long}</td><td>{@code BIGINT}</td></tr>
 *   <tr><td>{@code float}</td><td>{@code FLOAT}</td></tr>
 *   <tr><td>{@code double}</td><td>{@code DOUBLE}</td></tr>
 *   <tr><td>{@code bytes}</td><td>{@code BYTES}</td></tr>
 *   <tr><td>{@code string}</td><td>{@code STRING}</td></tr>
 *   <tr><td>{@code fixed(n)}</td><td>{@code BYTES} (length property)</td></tr>
 *   <tr><td>{@code enum}</td><td>{@code STRING} (symbols property)</td></tr>
 *   <tr><td>{@code array<T>}</td><td>{@code ARRAY<T>}</td></tr>
 *   <tr><td>{@code map<V>}</td><td>{@code MAP<STRING,V>}</td></tr>
 *   <tr><td>{@code record}</td><td>{@code ROW<...>}</td></tr>
 *   <tr><td>{@code union [null, T]}</td><td>nullable {@code T}</td></tr>
 *   <tr><td>{@code union} (other)</td><td><b>rejected</b></td></tr>
 *   <tr><td>{@code decimal(p,s)}</td><td>{@code DECIMAL(p,s)}</td></tr>
 *   <tr><td>{@code date}</td><td>{@code DATE}</td></tr>
 *   <tr><td>{@code timestamp-millis}</td><td>{@code TIMESTAMP_LTZ(3)}</td></tr>
 *   <tr><td>{@code timestamp-micros}</td><td>{@code TIMESTAMP_LTZ(6)}</td></tr>
 * </table>
 *
 * <p>Recursive / self-referential named types are rejected (Fluss row types are tree-shaped).
 *
 * @since 0.9
 */
@ThreadSafe
public final class AvroFormatTranslator implements FormatTranslator {

    /** Stable format id, used by callers that resolve codec caches by format. */
    public static final String FORMAT_ID = "AVRO";

    @Override
    public String formatId() {
        return FORMAT_ID;
    }

    @Override
    public RowType translateTo(String schemaText) {
        if (schemaText == null || schemaText.trim().isEmpty()) {
            throw new SchemaTranslationException("Avro schema text must not be empty.");
        }
        final Schema root;
        try {
            root = new Schema.Parser().parse(schemaText);
        } catch (RuntimeException e) {
            throw new SchemaTranslationException(
                    "Failed to parse Avro schema: " + e.getMessage(), e);
        }
        // The top-level schema must be a record; unions-at-top or naked primitives aren't a
        // meaningful "table row".
        if (root.getType() != Schema.Type.RECORD) {
            throw new SchemaTranslationException(
                    "Top-level Avro schema must be a RECORD for table mapping, got "
                            + root.getType());
        }
        return toRowType(root, new IdentityHashMap<Schema, Boolean>());
    }

    @Override
    public String translateFrom(RowType rowType) {
        if (rowType == null) {
            throw new SchemaTranslationException("RowType must not be null.");
        }
        SchemaBuilder.FieldAssembler<Schema> assembler =
                SchemaBuilder.record("FlussRow").namespace("org.apache.fluss.generated").fields();
        for (DataField f : rowType.getFields()) {
            Schema inner = toAvro(f.getType());
            if (f.getType().isNullable()) {
                // Nullable-union idiom: [null, T]. Default null.
                Schema nullable =
                        Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), inner));
                assembler = assembler.name(f.getName()).type(nullable).withDefault(null);
            } else {
                assembler = assembler.name(f.getName()).type(inner).noDefault();
            }
        }
        return assembler.endRecord().toString();
    }

    // ------------------------------------------------------------------------------------------
    // Avro -> Fluss
    // ------------------------------------------------------------------------------------------

    private RowType toRowType(Schema record, IdentityHashMap<Schema, Boolean> stack) {
        assertNotRecursive(record, stack);
        stack.put(record, Boolean.TRUE);
        try {
            List<Schema.Field> avroFields = record.getFields();
            if (avroFields.isEmpty()) {
                throw new SchemaTranslationException(
                        "Avro record '" + record.getFullName() + "' has no fields.");
            }
            List<DataField> fields = new ArrayList<>(avroFields.size());
            Set<String> seen = new HashSet<>();
            for (Schema.Field f : avroFields) {
                if (!seen.add(f.name())) {
                    throw new SchemaTranslationException(
                            "Duplicate field name '"
                                    + f.name()
                                    + "' in record "
                                    + record.getFullName());
                }
                DataType ft = toDataType(f.schema(), stack);
                fields.add(new DataField(f.name(), ft, f.doc()));
            }
            // Top-level RowType — nullability is governed by the caller. For an Avro record that
            // appears as a nested field, the enclosing toDataType(...) handles the union-wrap.
            return new RowType(true, fields);
        } finally {
            stack.remove(record);
        }
    }

    private DataType toDataType(Schema s, IdentityHashMap<Schema, Boolean> stack) {
        Schema.Type t = s.getType();
        switch (t) {
            case NULL:
                throw new SchemaTranslationException(
                        "Avro 'null' is only valid inside a [null, T] union, not as a standalone type.");
            case BOOLEAN:
                return DataTypes.BOOLEAN().copy(false);
            case INT:
                {
                    LogicalType lt = s.getLogicalType();
                    if (lt instanceof LogicalTypes.Date) {
                        return DataTypes.DATE().copy(false);
                    }
                    // time-millis is Avro int; we don't currently map to TIME because Fluss TIME
                    // has
                    // different semantics. Reject with a clear error so T.2 can pick it up.
                    if (lt != null && "time-millis".equals(lt.getName())) {
                        throw new SchemaTranslationException(
                                "Avro logical type 'time-millis' is not yet supported; "
                                        + "use a plain int or wait for T.4.");
                    }
                    return DataTypes.INT().copy(false);
                }
            case LONG:
                {
                    LogicalType lt = s.getLogicalType();
                    if (lt instanceof LogicalTypes.TimestampMillis) {
                        return DataTypes.TIMESTAMP_LTZ(3).copy(false);
                    }
                    if (lt instanceof LogicalTypes.TimestampMicros) {
                        return DataTypes.TIMESTAMP_LTZ(6).copy(false);
                    }
                    if (lt != null
                            && ("time-micros".equals(lt.getName())
                                    || "local-timestamp-millis".equals(lt.getName())
                                    || "local-timestamp-micros".equals(lt.getName()))) {
                        throw new SchemaTranslationException(
                                "Avro logical type '"
                                        + lt.getName()
                                        + "' is not yet supported in Phase T.1.");
                    }
                    return DataTypes.BIGINT().copy(false);
                }
            case FLOAT:
                return DataTypes.FLOAT().copy(false);
            case DOUBLE:
                return DataTypes.DOUBLE().copy(false);
            case BYTES:
                {
                    LogicalType lt = s.getLogicalType();
                    if (lt instanceof LogicalTypes.Decimal) {
                        LogicalTypes.Decimal d = (LogicalTypes.Decimal) lt;
                        validateDecimal(d.getPrecision(), d.getScale());
                        return DataTypes.DECIMAL(d.getPrecision(), d.getScale()).copy(false);
                    }
                    return DataTypes.BYTES().copy(false);
                }
            case STRING:
                return DataTypes.STRING().copy(false);
            case FIXED:
                {
                    LogicalType lt = s.getLogicalType();
                    if (lt instanceof LogicalTypes.Decimal) {
                        LogicalTypes.Decimal d = (LogicalTypes.Decimal) lt;
                        validateDecimal(d.getPrecision(), d.getScale());
                        return DataTypes.DECIMAL(d.getPrecision(), d.getScale()).copy(false);
                    }
                    if (s.getFixedSize() <= 0) {
                        throw new SchemaTranslationException(
                                "Avro fixed type '"
                                        + s.getFullName()
                                        + "' must have a positive size, got "
                                        + s.getFixedSize());
                    }
                    // Fluss has no fixed-length BYTES column at the public API; variable BYTES is
                    // the natural fit. A future phase can attach a length constraint as a column
                    // property (plan §22.5, "column property" note).
                    return DataTypes.BYTES().copy(false);
                }
            case ENUM:
                if (s.getEnumSymbols() == null || s.getEnumSymbols().isEmpty()) {
                    throw new SchemaTranslationException(
                            "Avro enum '"
                                    + s.getFullName()
                                    + "' must declare at least one symbol.");
                }
                return DataTypes.STRING().copy(false);
            case ARRAY:
                {
                    DataType elem = toDataType(s.getElementType(), stack);
                    return new ArrayType(false, elem);
                }
            case MAP:
                {
                    // Avro maps always have string keys. The value type is recursively mapped.
                    DataType value = toDataType(s.getValueType(), stack);
                    return new MapType(false, DataTypes.STRING().copy(false), value);
                }
            case RECORD:
                {
                    RowType nested = toRowType(s, stack);
                    return nested.copy(false);
                }
            case UNION:
                return fromUnion(s, stack);
            default:
                throw new SchemaTranslationException("Unsupported Avro type: " + t);
        }
    }

    private DataType fromUnion(Schema u, IdentityHashMap<Schema, Boolean> stack) {
        List<Schema> branches = u.getTypes();
        if (branches.size() != 2) {
            throw new SchemaTranslationException(
                    "Fluss only supports the nullable-union idiom [null, T]; "
                            + "got a "
                            + branches.size()
                            + "-way union: "
                            + u);
        }
        Schema a = branches.get(0);
        Schema b = branches.get(1);
        final Schema other;
        if (a.getType() == Schema.Type.NULL && b.getType() != Schema.Type.NULL) {
            other = b;
        } else if (b.getType() == Schema.Type.NULL && a.getType() != Schema.Type.NULL) {
            other = a;
        } else {
            throw new SchemaTranslationException(
                    "Fluss only supports unions of the form [null, T]; got: " + u);
        }
        DataType inner = toDataType(other, stack);
        return inner.copy(true);
    }

    private void assertNotRecursive(Schema s, IdentityHashMap<Schema, Boolean> stack) {
        if (stack.containsKey(s)) {
            throw new SchemaTranslationException(
                    "Recursive / self-referential Avro schema '"
                            + s.getFullName()
                            + "' cannot be mapped to a Fluss row type.");
        }
    }

    private static void validateDecimal(int precision, int scale) {
        if (precision < 1 || precision > 38) {
            throw new SchemaTranslationException(
                    "Decimal precision " + precision + " is out of Fluss range [1, 38].");
        }
        if (scale < 0 || scale > precision) {
            throw new SchemaTranslationException(
                    "Decimal scale "
                            + scale
                            + " is invalid for precision "
                            + precision
                            + " (require 0 <= scale <= precision).");
        }
    }

    // ------------------------------------------------------------------------------------------
    // Fluss -> Avro
    // ------------------------------------------------------------------------------------------

    private Schema toAvro(DataType dt) {
        DataTypeRoot root = dt.getTypeRoot();
        switch (root) {
            case BOOLEAN:
                return Schema.create(Schema.Type.BOOLEAN);
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return Schema.create(Schema.Type.INT);
            case BIGINT:
                return Schema.create(Schema.Type.LONG);
            case FLOAT:
                return Schema.create(Schema.Type.FLOAT);
            case DOUBLE:
                return Schema.create(Schema.Type.DOUBLE);
            case CHAR:
            case STRING:
                return Schema.create(Schema.Type.STRING);
            case BINARY:
            case BYTES:
                return Schema.create(Schema.Type.BYTES);
            case DECIMAL:
                {
                    DecimalType d = (DecimalType) dt;
                    return LogicalTypes.decimal(d.getPrecision(), d.getScale())
                            .addToSchema(Schema.create(Schema.Type.BYTES));
                }
            case DATE:
                return LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    LocalZonedTimestampType ts = (LocalZonedTimestampType) dt;
                    if (ts.getPrecision() <= 3) {
                        return LogicalTypes.timestampMillis()
                                .addToSchema(Schema.create(Schema.Type.LONG));
                    }
                    return LogicalTypes.timestampMicros()
                            .addToSchema(Schema.create(Schema.Type.LONG));
                }
            case ARRAY:
                {
                    ArrayType at = (ArrayType) dt;
                    return Schema.createArray(toAvro(at.getElementType()));
                }
            case MAP:
                {
                    MapType mt = (MapType) dt;
                    if (mt.getKeyType().getTypeRoot() != DataTypeRoot.STRING
                            && mt.getKeyType().getTypeRoot() != DataTypeRoot.CHAR) {
                        throw new SchemaTranslationException(
                                "Avro maps require STRING keys, got " + mt.getKeyType());
                    }
                    return Schema.createMap(toAvro(mt.getValueType()));
                }
            case ROW:
                {
                    RowType nested = (RowType) dt;
                    SchemaBuilder.FieldAssembler<Schema> nb =
                            SchemaBuilder.record("Nested" + Integer.toHexString(nested.hashCode()))
                                    .namespace("org.apache.fluss.generated")
                                    .fields();
                    for (DataField ff : nested.getFields()) {
                        Schema inner = toAvro(ff.getType());
                        if (ff.getType().isNullable()) {
                            Schema nullable =
                                    Schema.createUnion(
                                            Arrays.asList(Schema.create(Schema.Type.NULL), inner));
                            nb = nb.name(ff.getName()).type(nullable).withDefault(null);
                        } else {
                            nb = nb.name(ff.getName()).type(inner).noDefault();
                        }
                    }
                    return nb.endRecord();
                }
            default:
                throw new SchemaTranslationException(
                        "Fluss type " + dt + " has no Avro representation.");
        }
    }
}
