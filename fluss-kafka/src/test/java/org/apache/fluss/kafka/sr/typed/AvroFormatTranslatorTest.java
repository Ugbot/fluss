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

import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Golden-matrix coverage of {@link AvroFormatTranslator} against plan §22.5. Exercises the common
 * scalar + nullable-union mapping plus the set of rejection vectors (multi-way unions, non-null
 * default in a union, recursive references, empty / malformed schemas).
 */
class AvroFormatTranslatorTest {

    private final AvroFormatTranslator translator = new AvroFormatTranslator();

    @Test
    void simpleRecordMapsAllPrimitives() {
        String schema =
                "{"
                        + "\"type\":\"record\",\"name\":\"P\",\"fields\":["
                        + "{\"name\":\"b\",\"type\":\"boolean\"},"
                        + "{\"name\":\"i\",\"type\":\"int\"},"
                        + "{\"name\":\"l\",\"type\":\"long\"},"
                        + "{\"name\":\"f\",\"type\":\"float\"},"
                        + "{\"name\":\"d\",\"type\":\"double\"},"
                        + "{\"name\":\"s\",\"type\":\"string\"},"
                        + "{\"name\":\"y\",\"type\":\"bytes\"}"
                        + "]}";
        RowType rt = translator.translateTo(schema);
        assertThat(rt.getFieldCount()).isEqualTo(7);
        assertThat(rt.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.BOOLEAN);
        assertThat(rt.getTypeAt(1).getTypeRoot()).isEqualTo(DataTypeRoot.INTEGER);
        assertThat(rt.getTypeAt(2).getTypeRoot()).isEqualTo(DataTypeRoot.BIGINT);
        assertThat(rt.getTypeAt(3).getTypeRoot()).isEqualTo(DataTypeRoot.FLOAT);
        assertThat(rt.getTypeAt(4).getTypeRoot()).isEqualTo(DataTypeRoot.DOUBLE);
        assertThat(rt.getTypeAt(5).getTypeRoot()).isEqualTo(DataTypeRoot.STRING);
        assertThat(rt.getTypeAt(6).getTypeRoot()).isEqualTo(DataTypeRoot.BYTES);
        // All primitives above are declared non-nullable (no null-union).
        for (int i = 0; i < rt.getFieldCount(); i++) {
            assertThat(rt.getTypeAt(i).isNullable()).as("field %d nullable", i).isFalse();
        }
    }

    @Test
    void nullableUnionIdiomYieldsNullableFluss() {
        String schema =
                "{\"type\":\"record\",\"name\":\"U\",\"fields\":["
                        + "{\"name\":\"maybe\",\"type\":[\"null\",\"string\"],\"default\":null},"
                        + "{\"name\":\"other\",\"type\":[\"int\",\"null\"]}"
                        + "]}";
        RowType rt = translator.translateTo(schema);
        assertThat(rt.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.STRING);
        assertThat(rt.getTypeAt(0).isNullable()).isTrue();
        assertThat(rt.getTypeAt(1).getTypeRoot()).isEqualTo(DataTypeRoot.INTEGER);
        assertThat(rt.getTypeAt(1).isNullable()).isTrue();
    }

    @Test
    void logicalTypesMapCleanly() {
        String schema =
                "{\"type\":\"record\",\"name\":\"L\",\"fields\":["
                        + "{\"name\":\"d\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},"
                        + "{\"name\":\"tsm\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},"
                        + "{\"name\":\"tsu\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}},"
                        + "{\"name\":\"dec\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":10,\"scale\":2}}"
                        + "]}";
        RowType rt = translator.translateTo(schema);
        assertThat(rt.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.DATE);
        assertThat(rt.getTypeAt(1).getTypeRoot())
                .isEqualTo(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        assertThat(rt.getTypeAt(2).getTypeRoot())
                .isEqualTo(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        assertThat(rt.getTypeAt(3).getTypeRoot()).isEqualTo(DataTypeRoot.DECIMAL);
    }

    @Test
    void enumMapsToString() {
        String schema =
                "{\"type\":\"record\",\"name\":\"E\",\"fields\":["
                        + "{\"name\":\"c\",\"type\":{\"type\":\"enum\",\"name\":\"Color\",\"symbols\":[\"RED\",\"GREEN\",\"BLUE\"]}}"
                        + "]}";
        RowType rt = translator.translateTo(schema);
        assertThat(rt.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.STRING);
    }

    @Test
    void fixedMapsToBytes() {
        String schema =
                "{\"type\":\"record\",\"name\":\"F\",\"fields\":["
                        + "{\"name\":\"raw\",\"type\":{\"type\":\"fixed\",\"name\":\"Sha\",\"size\":32}}"
                        + "]}";
        RowType rt = translator.translateTo(schema);
        assertThat(rt.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.BYTES);
    }

    @Test
    void nestedRecordMapsToRow() {
        String schema =
                "{\"type\":\"record\",\"name\":\"Outer\",\"fields\":["
                        + "{\"name\":\"inner\",\"type\":{\"type\":\"record\",\"name\":\"Inner\",\"fields\":["
                        + "{\"name\":\"x\",\"type\":\"int\"}]}}"
                        + "]}";
        RowType rt = translator.translateTo(schema);
        DataType inner = rt.getTypeAt(0);
        assertThat(inner.getTypeRoot()).isEqualTo(DataTypeRoot.ROW);
        assertThat(((RowType) inner).getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.INTEGER);
    }

    @Test
    void arrayAndMapMapCleanly() {
        String schema =
                "{\"type\":\"record\",\"name\":\"AM\",\"fields\":["
                        + "{\"name\":\"arr\",\"type\":{\"type\":\"array\",\"items\":\"long\"}},"
                        + "{\"name\":\"m\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}"
                        + "]}";
        RowType rt = translator.translateTo(schema);
        assertThat(rt.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.ARRAY);
        assertThat(rt.getTypeAt(1).getTypeRoot()).isEqualTo(DataTypeRoot.MAP);
    }

    @Test
    void threeWayUnionIsRejected() {
        String schema =
                "{\"type\":\"record\",\"name\":\"W\",\"fields\":["
                        + "{\"name\":\"u\",\"type\":[\"null\",\"int\",\"string\"]}"
                        + "]}";
        assertThatThrownBy(() -> translator.translateTo(schema))
                .isInstanceOf(SchemaTranslationException.class)
                .hasMessageContaining("union");
    }

    @Test
    void unionWithoutNullIsRejected() {
        String schema =
                "{\"type\":\"record\",\"name\":\"WN\",\"fields\":["
                        + "{\"name\":\"u\",\"type\":[\"int\",\"string\"]}"
                        + "]}";
        assertThatThrownBy(() -> translator.translateTo(schema))
                .isInstanceOf(SchemaTranslationException.class);
    }

    @Test
    void recursiveReferenceIsRejected() {
        // Node { value: int, next: [null, Node] }  — self-referential.
        String schema =
                "{\"type\":\"record\",\"name\":\"Node\",\"fields\":["
                        + "{\"name\":\"value\",\"type\":\"int\"},"
                        + "{\"name\":\"next\",\"type\":[\"null\",\"Node\"],\"default\":null}"
                        + "]}";
        assertThatThrownBy(() -> translator.translateTo(schema))
                .isInstanceOf(SchemaTranslationException.class)
                .hasMessageContaining("Recursive");
    }

    @Test
    void topLevelNonRecordIsRejected() {
        assertThatThrownBy(() -> translator.translateTo("\"int\""))
                .isInstanceOf(SchemaTranslationException.class)
                .hasMessageContaining("RECORD");
    }

    @Test
    void emptyOrMalformedSchemaIsRejected() {
        assertThatThrownBy(() -> translator.translateTo(""))
                .isInstanceOf(SchemaTranslationException.class);
        assertThatThrownBy(() -> translator.translateTo("not-json"))
                .isInstanceOf(SchemaTranslationException.class)
                .hasMessageContaining("parse");
    }

    @Test
    void decimalOutOfRangeIsRejected() {
        String schema =
                "{\"type\":\"record\",\"name\":\"D\",\"fields\":["
                        + "{\"name\":\"d\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":40,\"scale\":2}}"
                        + "]}";
        assertThatThrownBy(() -> translator.translateTo(schema))
                .isInstanceOf(SchemaTranslationException.class)
                .hasMessageContaining("precision");
    }

    @Test
    void roundTripFromRowTypeYieldsParsableAvro() {
        RowType rt =
                DataTypes.ROW(
                        DataTypes.FIELD("a", DataTypes.INT()),
                        DataTypes.FIELD("b", DataTypes.STRING()),
                        DataTypes.FIELD("c", DataTypes.BIGINT().copy(true)));
        String avro = translator.translateFrom(rt);
        // Re-parse via Avro; it must survive a round-trip through translateTo as well.
        RowType back = translator.translateTo(avro);
        assertThat(back.getFieldCount()).isEqualTo(3);
        assertThat(back.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.INTEGER);
        assertThat(back.getTypeAt(1).getTypeRoot()).isEqualTo(DataTypeRoot.STRING);
        assertThat(back.getTypeAt(2).getTypeRoot()).isEqualTo(DataTypeRoot.BIGINT);
        assertThat(back.getTypeAt(2).isNullable()).isTrue();
    }

    @Test
    void formatIdIsAvro() {
        assertThat(translator.formatId()).isEqualTo("AVRO");
    }
}
