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

import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit-level coverage for {@link TypedTableEvolver#computeAdditiveDelta(RowType, RowType)} and
 * {@link TypedTableEvolver#stripReserved(RowType, boolean)}. Exercises every reject branch in
 * design 0015 §5.3 plus the accept-with-appended-nullable case, without requiring an embedded Fluss
 * cluster. The integration coverage for the full register-and-reshape flow lives in {@code
 * KafkaTypedAlterITCase}.
 */
class TypedTableEvolverTest {

    @Test
    void noChangeYieldsEmptyAccept() {
        RowType current =
                row(field("id", DataTypes.INT(), true), field("name", DataTypes.STRING(), true));
        RowType proposed =
                row(field("id", DataTypes.INT(), true), field("name", DataTypes.STRING(), true));

        TypedTableEvolver.AdditiveDelta delta =
                TypedTableEvolver.computeAdditiveDelta(current, proposed);

        assertThat(delta.reject()).isNull();
        assertThat(delta.changes()).isEmpty();
    }

    @Test
    void appendedNullableColumnAccepted() {
        RowType current = row(field("id", DataTypes.INT(), true));
        RowType proposed =
                row(
                        field("id", DataTypes.INT(), true),
                        field("email", DataTypes.STRING(), /* nullable */ true));

        TypedTableEvolver.AdditiveDelta delta =
                TypedTableEvolver.computeAdditiveDelta(current, proposed);

        assertThat(delta.reject()).isNull();
        assertThat(delta.changes()).hasSize(1);
        TableChange.AddColumn add = (TableChange.AddColumn) delta.changes().get(0);
        assertThat(add.getName()).isEqualTo("email");
        assertThat(add.getDataType().isNullable()).isTrue();
        assertThat(add.getPosition()).isEqualTo(TableChange.ColumnPosition.last());
    }

    @Test
    void appendedNonNullColumnRejected() {
        RowType current = row(field("id", DataTypes.INT(), true));
        RowType proposed =
                row(
                        field("id", DataTypes.INT(), true),
                        field("score", DataTypes.INT(), /* nullable */ false));

        TypedTableEvolver.AdditiveDelta delta =
                TypedTableEvolver.computeAdditiveDelta(current, proposed);

        assertThat(delta.reject()).contains("new column must be nullable").contains("score");
        assertThat(delta.changes()).isEmpty();
    }

    @Test
    void columnRemovalRejected() {
        RowType current =
                row(field("id", DataTypes.INT(), true), field("name", DataTypes.STRING(), true));
        RowType proposed = row(field("id", DataTypes.INT(), true));

        TypedTableEvolver.AdditiveDelta delta =
                TypedTableEvolver.computeAdditiveDelta(current, proposed);

        assertThat(delta.reject()).contains("column removed").contains("name");
    }

    @Test
    void columnRenameRejected() {
        RowType current = row(field("id", DataTypes.INT(), true));
        RowType proposed = row(field("user_id", DataTypes.INT(), true));

        TypedTableEvolver.AdditiveDelta delta =
                TypedTableEvolver.computeAdditiveDelta(current, proposed);

        assertThat(delta.reject()).contains("rename").contains("id").contains("user_id");
    }

    @Test
    void columnTypeChangeRejected() {
        RowType current = row(field("id", DataTypes.INT(), true));
        RowType proposed = row(field("id", DataTypes.BIGINT(), true));

        TypedTableEvolver.AdditiveDelta delta =
                TypedTableEvolver.computeAdditiveDelta(current, proposed);

        assertThat(delta.reject()).contains("column type changed").contains("id");
    }

    @Test
    void columnReorderRejected() {
        // Same fields, swapped positions. Both names exist in current, types match → reorder.
        RowType current =
                row(field("id", DataTypes.INT(), true), field("name", DataTypes.STRING(), true));
        RowType proposed =
                row(field("name", DataTypes.STRING(), true), field("id", DataTypes.INT(), true));

        TypedTableEvolver.AdditiveDelta delta =
                TypedTableEvolver.computeAdditiveDelta(current, proposed);

        assertThat(delta.reject()).contains("reorder");
    }

    @Test
    void stripReservedExtractsUserRegion() {
        RowType full =
                row(
                        field("record_key", DataTypes.BYTES(), true),
                        field("id", DataTypes.INT(), true),
                        field("name", DataTypes.STRING(), true),
                        field("event_time", DataTypes.TIMESTAMP_LTZ(3), false),
                        field(
                                "headers",
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("name", DataTypes.STRING()),
                                                DataTypes.FIELD("value", DataTypes.BYTES()))),
                                true));

        RowType user = TypedTableEvolver.stripReserved(full, /* compacted */ false);

        assertThat(user.getFieldNames()).containsExactly("id", "name");
    }

    @Test
    void stripReservedFallsBackOnUnexpectedShape() {
        // No record_key prefix — fall back to identity, log a warn (not asserted).
        RowType full = row(field("a", DataTypes.INT(), true), field("b", DataTypes.STRING(), true));
        RowType user = TypedTableEvolver.stripReserved(full, false);
        assertThat(user.getFieldNames()).containsExactly("a", "b");
    }

    private static RowType row(DataField... fields) {
        if (fields.length == 0) {
            return new RowType(true, Collections.emptyList());
        }
        return new RowType(true, Arrays.asList(fields));
    }

    private static DataField field(
            String name, org.apache.fluss.types.DataType type, boolean nullable) {
        return new DataField(name, type.copy(nullable));
    }
}
