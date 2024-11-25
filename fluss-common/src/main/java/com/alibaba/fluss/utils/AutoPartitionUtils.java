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

import com.alibaba.fluss.config.AutoPartitionTimeUnit;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/** Utils for auto partition. */
public class AutoPartitionUtils {

    private static final String YEAR_FORMAT = "yyyy";
    private static final String QUARTER_FORMAT = "yyyyQ";
    private static final String MONTH_FORMAT = "yyyyMM";
    private static final String DAY_FORMAT = "yyyyMMdd";
    private static final String HOUR_FORMAT = "yyyyMMddHH";

    public static String getPartitionString(
            ZonedDateTime current, int offset, AutoPartitionTimeUnit timeUnit) {
        switch (timeUnit) {
            case YEAR:
                return getFormattedTime(current.plusYears(offset), YEAR_FORMAT);
            case QUARTER:
                return getFormattedTime(current.plusMonths(offset * 3L), QUARTER_FORMAT);
            case MONTH:
                return getFormattedTime(current.plusMonths(offset), MONTH_FORMAT);
            case DAY:
                return getFormattedTime(current.plusDays(offset), DAY_FORMAT);
            case HOUR:
                return getFormattedTime(current.plusHours(offset), HOUR_FORMAT);
            default:
                throw new IllegalArgumentException("Unsupported time unit: " + timeUnit);
        }
    }

    private static String getFormattedTime(ZonedDateTime zonedDateTime, String format) {
        return DateTimeFormatter.ofPattern(format).format(zonedDateTime);
    }
}
