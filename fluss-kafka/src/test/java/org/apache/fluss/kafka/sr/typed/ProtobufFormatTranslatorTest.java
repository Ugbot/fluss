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
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link ProtobufFormatTranslator}. */
class ProtobufFormatTranslatorTest {

    private final ProtobufFormatTranslator translator = new ProtobufFormatTranslator();

    @Test
    void formatIdIsProtobuf() {
        assertThat(translator.formatId()).isEqualTo("PROTOBUF");
    }

    @Test
    void proto3ScalarRecord() {
        String proto =
                "syntax = \"proto3\";\n"
                        + "message Person {\n"
                        + "  int32 id = 1;\n"
                        + "  string name = 2;\n"
                        + "  bool active = 3;\n"
                        + "}\n";
        RowType row = translator.translateTo(proto);
        assertThat(row.getFieldCount()).isEqualTo(3);
        assertThat(row.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.INTEGER);
        assertThat(row.getTypeAt(1).getTypeRoot()).isEqualTo(DataTypeRoot.STRING);
        assertThat(row.getTypeAt(2).getTypeRoot()).isEqualTo(DataTypeRoot.BOOLEAN);
    }

    @Test
    void allIntegerScalarsMap() {
        String proto =
                "syntax = \"proto3\";\n"
                        + "message M {\n"
                        + "  int32 a = 1;\n"
                        + "  sint32 b = 2;\n"
                        + "  sfixed32 c = 3;\n"
                        + "  int64 d = 4;\n"
                        + "  sint64 e = 5;\n"
                        + "  sfixed64 f = 6;\n"
                        + "  uint32 g = 7;\n"
                        + "  uint64 h = 8;\n"
                        + "  fixed32 i = 9;\n"
                        + "  fixed64 j = 10;\n"
                        + "}\n";
        RowType row = translator.translateTo(proto);
        assertThat(row.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.INTEGER);
        assertThat(row.getTypeAt(1).getTypeRoot()).isEqualTo(DataTypeRoot.INTEGER);
        assertThat(row.getTypeAt(2).getTypeRoot()).isEqualTo(DataTypeRoot.INTEGER);
        assertThat(row.getTypeAt(3).getTypeRoot()).isEqualTo(DataTypeRoot.BIGINT);
        assertThat(row.getTypeAt(4).getTypeRoot()).isEqualTo(DataTypeRoot.BIGINT);
        assertThat(row.getTypeAt(5).getTypeRoot()).isEqualTo(DataTypeRoot.BIGINT);
        assertThat(row.getTypeAt(6).getTypeRoot()).isEqualTo(DataTypeRoot.BIGINT);
        assertThat(row.getTypeAt(7).getTypeRoot()).isEqualTo(DataTypeRoot.BIGINT);
        assertThat(row.getTypeAt(8).getTypeRoot()).isEqualTo(DataTypeRoot.BIGINT);
        assertThat(row.getTypeAt(9).getTypeRoot()).isEqualTo(DataTypeRoot.BIGINT);
    }

    @Test
    void floatAndDouble() {
        String proto =
                "syntax = \"proto3\";\n"
                        + "message M {\n"
                        + "  float x = 1;\n"
                        + "  double y = 2;\n"
                        + "}\n";
        RowType row = translator.translateTo(proto);
        assertThat(row.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.FLOAT);
        assertThat(row.getTypeAt(1).getTypeRoot()).isEqualTo(DataTypeRoot.DOUBLE);
    }

    @Test
    void bytesAndString() {
        String proto =
                "syntax = \"proto3\";\n"
                        + "message M {\n"
                        + "  bytes b = 1;\n"
                        + "  string s = 2;\n"
                        + "}\n";
        RowType row = translator.translateTo(proto);
        assertThat(row.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.BYTES);
        assertThat(row.getTypeAt(1).getTypeRoot()).isEqualTo(DataTypeRoot.STRING);
    }

    @Test
    void repeatedMapsToArray() {
        String proto =
                "syntax = \"proto3\";\n"
                        + "message M {\n"
                        + "  repeated string tags = 1;\n"
                        + "}\n";
        RowType row = translator.translateTo(proto);
        assertThat(row.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.ARRAY);
    }

    @Test
    void mapMapsToMap() {
        String proto =
                "syntax = \"proto3\";\n"
                        + "message M {\n"
                        + "  map<string, int32> counts = 1;\n"
                        + "}\n";
        RowType row = translator.translateTo(proto);
        assertThat(row.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.MAP);
    }

    @Test
    void nestedMessageMapsToRow() {
        String proto =
                "syntax = \"proto3\";\n"
                        + "message Outer {\n"
                        + "  message Inner { string v = 1; }\n"
                        + "  Inner inner = 1;\n"
                        + "}\n";
        RowType row = translator.translateTo(proto);
        assertThat(row.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.ROW);
    }

    @Test
    void siblingMessageReferenceResolves() {
        String proto =
                "syntax = \"proto3\";\n"
                        + "message Outer {\n"
                        + "  Sibling s = 1;\n"
                        + "}\n"
                        + "message Sibling { string v = 1; }\n";
        RowType row = translator.translateTo(proto);
        assertThat(row.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.ROW);
    }

    @Test
    void enumMapsToString() {
        String proto =
                "syntax = \"proto3\";\n"
                        + "message M {\n"
                        + "  Color c = 1;\n"
                        + "}\n"
                        + "enum Color { RED = 0; GREEN = 1; BLUE = 2; }\n";
        RowType row = translator.translateTo(proto);
        assertThat(row.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.STRING);
    }

    @Test
    void timestampWellKnownType() {
        String proto =
                "syntax = \"proto3\";\n"
                        + "import \"google/protobuf/timestamp.proto\";\n"
                        + "message M {\n"
                        + "  google.protobuf.Timestamp created_at = 1;\n"
                        + "}\n";
        RowType row = translator.translateTo(proto);
        assertThat(row.getTypeAt(0).getTypeRoot())
                .isEqualTo(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    }

    @Test
    void durationWellKnownType() {
        String proto =
                "syntax = \"proto3\";\n"
                        + "import \"google/protobuf/duration.proto\";\n"
                        + "message M {\n"
                        + "  google.protobuf.Duration ttl = 1;\n"
                        + "}\n";
        RowType row = translator.translateTo(proto);
        assertThat(row.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.BIGINT);
    }

    @Test
    void wrappersMapToNullablePrimitives() {
        String proto =
                "syntax = \"proto3\";\n"
                        + "import \"google/protobuf/wrappers.proto\";\n"
                        + "message M {\n"
                        + "  google.protobuf.StringValue s = 1;\n"
                        + "  google.protobuf.Int32Value i = 2;\n"
                        + "}\n";
        RowType row = translator.translateTo(proto);
        assertThat(row.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.STRING);
        assertThat(row.getTypeAt(0).isNullable()).isTrue();
        assertThat(row.getTypeAt(1).getTypeRoot()).isEqualTo(DataTypeRoot.INTEGER);
        assertThat(row.getTypeAt(1).isNullable()).isTrue();
    }

    @Test
    void anyRejected() {
        String proto =
                "syntax = \"proto3\";\n"
                        + "import \"google/protobuf/any.proto\";\n"
                        + "message M {\n"
                        + "  google.protobuf.Any payload = 1;\n"
                        + "}\n";
        assertThatThrownBy(() -> translator.translateTo(proto))
                .isInstanceOf(SchemaTranslationException.class)
                .hasMessageContaining("Any");
    }

    @Test
    void oneofRejected() {
        String proto =
                "syntax = \"proto3\";\n"
                        + "message M {\n"
                        + "  oneof value {\n"
                        + "    string text_value = 1;\n"
                        + "    int32 int_value = 2;\n"
                        + "  }\n"
                        + "}\n";
        assertThatThrownBy(() -> translator.translateTo(proto))
                .isInstanceOf(SchemaTranslationException.class)
                .hasMessageContaining("oneof");
    }

    @Test
    void syntheticOneofIsProto3Optional() {
        // proto3 optional generates a synthetic oneof prefixed with '_'.
        String proto =
                "syntax = \"proto3\";\n"
                        + "message M {\n"
                        + "  oneof _maybe {\n"
                        + "    string maybe = 1;\n"
                        + "  }\n"
                        + "}\n";
        RowType row = translator.translateTo(proto);
        assertThat(row.getFieldCount()).isEqualTo(1);
        assertThat(row.getTypeAt(0).isNullable()).isTrue();
        assertThat(row.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.STRING);
    }

    @Test
    void recursiveMessageRejected() {
        String proto = "syntax = \"proto3\";\n" + "message Node {\n" + "  Node next = 1;\n" + "}\n";
        assertThatThrownBy(() -> translator.translateTo(proto))
                .isInstanceOf(SchemaTranslationException.class)
                .hasMessageContaining("Recursive");
    }

    @Test
    void emptyTextRejected() {
        assertThatThrownBy(() -> translator.translateTo(""))
                .isInstanceOf(SchemaTranslationException.class);
    }

    @Test
    void fileWithoutTopLevelMessageRejected() {
        String proto = "syntax = \"proto3\";\npackage foo.bar;\n";
        assertThatThrownBy(() -> translator.translateTo(proto))
                .isInstanceOf(SchemaTranslationException.class);
    }

    @Test
    void unresolvedTypeRejected() {
        String proto = "syntax = \"proto3\";\n" + "message M {\n" + "  Mystery x = 1;\n" + "}\n";
        assertThatThrownBy(() -> translator.translateTo(proto))
                .isInstanceOf(SchemaTranslationException.class)
                .hasMessageContaining("Unresolved");
    }

    @Test
    void translateFromRoundTrip() {
        // Build a RowType and translate it out.
        String schema =
                "syntax = \"proto3\";\n"
                        + "message M {\n"
                        + "  int32 id = 1;\n"
                        + "  string name = 2;\n"
                        + "}\n";
        RowType original = translator.translateTo(schema);
        String regenerated = translator.translateFrom(original);
        assertThat(regenerated).contains("message Record");
        assertThat(regenerated).contains("int32 id");
        assertThat(regenerated).contains("string name");
    }

    @Test
    void commentsAndOptionsSkipped() {
        String proto =
                "// top-level comment\n"
                        + "syntax = \"proto3\";\n"
                        + "option optimize_for = SPEED;\n"
                        + "/* block\n"
                        + "   comment */\n"
                        + "message M {\n"
                        + "  // field comment\n"
                        + "  int32 id = 1 [deprecated=true];\n"
                        + "}\n";
        RowType row = translator.translateTo(proto);
        assertThat(row.getFieldCount()).isEqualTo(1);
    }

    @Test
    void proto2WithRequiredNotSupported() {
        // proto2 required is just a modifier; our translator accepts it as non-nullable.
        String proto =
                "syntax = \"proto2\";\n"
                        + "message M {\n"
                        + "  required int32 id = 1;\n"
                        + "  optional string name = 2;\n"
                        + "}\n";
        RowType row = translator.translateTo(proto);
        assertThat(row.getFieldCount()).isEqualTo(2);
        assertThat(row.getTypeAt(0).isNullable()).isFalse();
        assertThat(row.getTypeAt(1).isNullable()).isTrue();
    }
}
