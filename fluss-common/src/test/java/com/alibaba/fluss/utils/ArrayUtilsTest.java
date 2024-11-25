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

package com.alibaba.fluss.utils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link com.alibaba.fluss.utils.ArrayUtils}. */
public class ArrayUtilsTest {

    @Test
    void concatWithEmptyArray() {
        String[] emptyArray = new String[] {};
        String[] nonEmptyArray = new String[] {"some value"};

        assertThat(ArrayUtils.concat(emptyArray, nonEmptyArray)).isSameAs(nonEmptyArray);

        assertThat(ArrayUtils.concat(nonEmptyArray, emptyArray)).isSameAs(nonEmptyArray);
    }

    @Test
    void concatArrays() {
        String[] array1 = new String[] {"A", "B", "C", "D", "E", "F", "G"};
        String[] array2 = new String[] {"1", "2", "3"};

        assertThat(ArrayUtils.concat(array1, array2))
                .isEqualTo(new String[] {"A", "B", "C", "D", "E", "F", "G", "1", "2", "3"});

        assertThat(ArrayUtils.concat(array2, array1))
                .isEqualTo(new String[] {"1", "2", "3", "A", "B", "C", "D", "E", "F", "G"});
    }
}
