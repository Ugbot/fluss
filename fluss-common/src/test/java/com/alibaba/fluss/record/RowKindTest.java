/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.record;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link com.alibaba.fluss.record.RowKind} class. */
public class RowKindTest {

    @Test
    void testGetSortString() {
        assertThat(RowKind.APPEND_ONLY.shortString()).isEqualTo("+A");
        assertThat(RowKind.INSERT.shortString()).isEqualTo("+I");
        assertThat(RowKind.UPDATE_BEFORE.shortString()).isEqualTo("-U");
        assertThat(RowKind.UPDATE_AFTER.shortString()).isEqualTo("+U");
        assertThat(RowKind.DELETE.shortString()).isEqualTo("-D");
    }

    @Test
    void testToByteValue() {
        assertThat(RowKind.APPEND_ONLY.toByteValue()).isEqualTo((byte) 0);
        assertThat(RowKind.INSERT.toByteValue()).isEqualTo((byte) 1);
        assertThat(RowKind.UPDATE_BEFORE.toByteValue()).isEqualTo((byte) 2);
        assertThat(RowKind.UPDATE_AFTER.toByteValue()).isEqualTo((byte) 3);
        assertThat(RowKind.DELETE.toByteValue()).isEqualTo((byte) 4);
    }

    @Test
    void testFromByteValue() {
        assertThat(RowKind.fromByteValue((byte) 0)).isEqualTo(RowKind.APPEND_ONLY);
        assertThat(RowKind.fromByteValue((byte) 1)).isEqualTo(RowKind.INSERT);
        assertThat(RowKind.fromByteValue((byte) 2)).isEqualTo(RowKind.UPDATE_BEFORE);
        assertThat(RowKind.fromByteValue((byte) 3)).isEqualTo(RowKind.UPDATE_AFTER);
        assertThat(RowKind.fromByteValue((byte) 4)).isEqualTo(RowKind.DELETE);

        assertThatThrownBy(() -> RowKind.fromByteValue((byte) 5))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Unsupported byte value");
    }
}
