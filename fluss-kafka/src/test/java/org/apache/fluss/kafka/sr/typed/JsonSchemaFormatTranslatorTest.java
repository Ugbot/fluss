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

import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link JsonSchemaFormatTranslator}. */
class JsonSchemaFormatTranslatorTest {

    private final JsonSchemaFormatTranslator translator = new JsonSchemaFormatTranslator();

    @Test
    void formatIdIsJson() {
        assertThat(translator.formatId()).isEqualTo("JSON");
    }

    @Test
    void simpleRecordMaps() {
        String schema =
                "{\"type\":\"object\",\"properties\":{"
                        + "\"id\":{\"type\":\"integer\",\"format\":\"int32\"},"
                        + "\"name\":{\"type\":\"string\"}"
                        + "},\"required\":[\"id\",\"name\"]}";
        RowType row = translator.translateTo(schema);
        assertThat(row.getFieldCount()).isEqualTo(2);
        assertThat(row.getTypeAt(0)).isEqualTo(DataTypes.INT().copy(false));
        assertThat(row.getTypeAt(1)).isEqualTo(DataTypes.STRING().copy(false));
    }

    @Test
    void bigintAndDoubleDefaults() {
        String schema =
                "{\"type\":\"object\",\"properties\":{"
                        + "\"bignum\":{\"type\":\"integer\"},"
                        + "\"ratio\":{\"type\":\"number\"}"
                        + "},\"required\":[\"bignum\",\"ratio\"]}";
        RowType row = translator.translateTo(schema);
        assertThat(row.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.BIGINT);
        assertThat(row.getTypeAt(1).getTypeRoot()).isEqualTo(DataTypeRoot.DOUBLE);
    }

    @Test
    void floatFormat() {
        String schema =
                "{\"type\":\"object\",\"properties\":{"
                        + "\"x\":{\"type\":\"number\",\"format\":\"float\"}"
                        + "},\"required\":[\"x\"]}";
        RowType row = translator.translateTo(schema);
        assertThat(row.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.FLOAT);
    }

    @Test
    void stringFormats() {
        String schema =
                "{\"type\":\"object\",\"properties\":{"
                        + "\"raw\":{\"type\":\"string\"},"
                        + "\"blob\":{\"type\":\"string\",\"format\":\"byte\"},"
                        + "\"day\":{\"type\":\"string\",\"format\":\"date\"},"
                        + "\"moment\":{\"type\":\"string\",\"format\":\"date-time\"}"
                        + "},\"required\":[\"raw\",\"blob\",\"day\",\"moment\"]}";
        RowType row = translator.translateTo(schema);
        assertThat(row.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.STRING);
        assertThat(row.getTypeAt(1).getTypeRoot()).isEqualTo(DataTypeRoot.BYTES);
        assertThat(row.getTypeAt(2).getTypeRoot()).isEqualTo(DataTypeRoot.DATE);
        assertThat(row.getTypeAt(3).getTypeRoot())
                .isEqualTo(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    }

    @Test
    void arrayOfPrimitives() {
        String schema =
                "{\"type\":\"object\",\"properties\":{"
                        + "\"tags\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}"
                        + "},\"required\":[\"tags\"]}";
        RowType row = translator.translateTo(schema);
        assertThat(row.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.ARRAY);
    }

    @Test
    void nestedRecord() {
        String schema =
                "{\"type\":\"object\",\"properties\":{"
                        + "\"addr\":{\"type\":\"object\",\"properties\":{"
                        + "\"street\":{\"type\":\"string\"}"
                        + "},\"required\":[\"street\"]}"
                        + "},\"required\":[\"addr\"]}";
        RowType row = translator.translateTo(schema);
        assertThat(row.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.ROW);
    }

    @Test
    void mapViaAdditionalProperties() {
        String schema =
                "{\"type\":\"object\",\"properties\":{"
                        + "\"labels\":{\"type\":\"object\",\"additionalProperties\":{\"type\":\"string\"}}"
                        + "},\"required\":[\"labels\"]}";
        RowType row = translator.translateTo(schema);
        assertThat(row.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.MAP);
    }

    @Test
    void nullableViaOneOf() {
        String schema =
                "{\"type\":\"object\",\"properties\":{"
                        + "\"maybe\":{\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]}"
                        + "}}";
        RowType row = translator.translateTo(schema);
        assertThat(row.getTypeAt(0).isNullable()).isTrue();
        assertThat(row.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.STRING);
    }

    @Test
    void nullableViaTypeArray() {
        String schema =
                "{\"type\":\"object\",\"properties\":{"
                        + "\"maybe\":{\"type\":[\"null\",\"string\"]}"
                        + "}}";
        RowType row = translator.translateTo(schema);
        assertThat(row.getTypeAt(0).isNullable()).isTrue();
        assertThat(row.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.STRING);
    }

    @Test
    void threeWayUnionRejected() {
        String schema =
                "{\"type\":\"object\",\"properties\":{"
                        + "\"x\":{\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"},{\"type\":\"integer\"}]}"
                        + "}}";
        assertThatThrownBy(() -> translator.translateTo(schema))
                .isInstanceOf(SchemaTranslationException.class)
                .hasMessageContaining("sum type");
    }

    @Test
    void nonNullOneOfRejected() {
        String schema =
                "{\"type\":\"object\",\"properties\":{"
                        + "\"x\":{\"oneOf\":[{\"type\":\"string\"},{\"type\":\"integer\"}]}"
                        + "}}";
        assertThatThrownBy(() -> translator.translateTo(schema))
                .isInstanceOf(SchemaTranslationException.class);
    }

    @Test
    void recursiveRefRejected() {
        String schema =
                "{\"type\":\"object\",\"properties\":{"
                        + "\"node\":{\"$ref\":\"#/definitions/Node\"}"
                        + "},\"definitions\":{"
                        + "\"Node\":{\"type\":\"object\",\"properties\":{"
                        + "\"next\":{\"$ref\":\"#/definitions/Node\"}}}}}";
        assertThatThrownBy(() -> translator.translateTo(schema))
                .isInstanceOf(SchemaTranslationException.class)
                .hasMessageContaining("Recursive");
    }

    @Test
    void crossFileRefRejected() {
        String schema =
                "{\"type\":\"object\",\"properties\":{"
                        + "\"x\":{\"$ref\":\"other.json#/Foo\"}"
                        + "}}";
        assertThatThrownBy(() -> translator.translateTo(schema))
                .isInstanceOf(SchemaTranslationException.class)
                .hasMessageContaining("Cross-file");
    }

    @Test
    void topLevelNonObjectRejected() {
        assertThatThrownBy(() -> translator.translateTo("{\"type\":\"integer\"}"))
                .isInstanceOf(SchemaTranslationException.class)
                .hasMessageContaining("object");
    }

    @Test
    void emptySchemaRejected() {
        assertThatThrownBy(() -> translator.translateTo(""))
                .isInstanceOf(SchemaTranslationException.class);
        assertThatThrownBy(() -> translator.translateTo("   "))
                .isInstanceOf(SchemaTranslationException.class);
    }

    @Test
    void malformedJsonRejected() {
        assertThatThrownBy(() -> translator.translateTo("{not json"))
                .isInstanceOf(SchemaTranslationException.class)
                .hasMessageContaining("parse");
    }

    @Test
    void unknownTypeRejected() {
        assertThatThrownBy(
                        () ->
                                translator.translateTo(
                                        "{\"type\":\"object\",\"properties\":{"
                                                + "\"x\":{\"type\":\"widget\"}}}"))
                .isInstanceOf(SchemaTranslationException.class)
                .hasMessageContaining("Unknown");
    }

    @Test
    void inFileRefResolved() {
        String schema =
                "{\"type\":\"object\",\"properties\":{"
                        + "\"addr\":{\"$ref\":\"#/definitions/Addr\"}"
                        + "},\"required\":[\"addr\"],"
                        + "\"definitions\":{"
                        + "\"Addr\":{\"type\":\"object\",\"properties\":{"
                        + "\"street\":{\"type\":\"string\"}},\"required\":[\"street\"]}"
                        + "}}";
        RowType row = translator.translateTo(schema);
        assertThat(row.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.ROW);
    }

    @Test
    void translateFromRoundTrip() {
        RowType original =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT().copy(false)),
                        DataTypes.FIELD("name", DataTypes.STRING().copy(false)),
                        DataTypes.FIELD("nick", DataTypes.STRING().copy(true)));
        String schema = translator.translateFrom(original);
        assertThat(schema).contains("\"type\":\"object\"");
        // Round-trip back; nullability of nick is preserved.
        RowType reconstructed = translator.translateTo(schema);
        assertThat(reconstructed.getFieldCount()).isEqualTo(3);
        assertThat(reconstructed.getTypeAt(2).isNullable()).isTrue();
    }

    @Test
    void decimalViaFormatAndMultipleOf() {
        String schema =
                "{\"type\":\"object\",\"properties\":{"
                        + "\"price\":{\"type\":\"number\",\"format\":\"decimal\",\"multipleOf\":0.01}"
                        + "},\"required\":[\"price\"]}";
        RowType row = translator.translateTo(schema);
        assertThat(row.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.DECIMAL);
    }
}
