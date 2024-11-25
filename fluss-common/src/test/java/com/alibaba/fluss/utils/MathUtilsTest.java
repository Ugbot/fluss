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

import static com.alibaba.fluss.utils.MathUtils.log2strict;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test of {@link com.alibaba.fluss.utils.MathUtils}. */
public class MathUtilsTest {

    @Test
    void testLog2strict() {
        assertThatThrownBy(() -> log2strict(0))
                .isInstanceOf(ArithmeticException.class)
                .hasMessageContaining("Logarithm of zero is undefined.");

        assertThatThrownBy(() -> log2strict(10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is not a power of two.");

        assertThat(log2strict(16)).isEqualTo(4);
    }
}
