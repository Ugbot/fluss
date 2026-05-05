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

package org.apache.fluss.catalog.entities;

import org.apache.fluss.annotation.PublicEvolving;

import java.util.Objects;

/**
 * One row in {@code _catalog.__sr_config__}. Stores free-form string key/value pairs for the Kafka
 * SR projection: global and per-subject compatibility levels, registry modes, etc. Keys follow the
 * convention {@code global_compatibility}, {@code subject_compatibility:<subject>}, {@code
 * global_mode}, {@code subject_mode:<subject>}.
 */
@PublicEvolving
public final class SrConfigEntry {

    private final String key;
    private final String value;

    public SrConfigEntry(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String key() {
        return key;
    }

    public String value() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SrConfigEntry)) {
            return false;
        }
        SrConfigEntry other = (SrConfigEntry) o;
        return key.equals(other.key) && value.equals(other.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }
}
