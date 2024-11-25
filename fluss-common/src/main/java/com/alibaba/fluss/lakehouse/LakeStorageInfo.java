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

package com.alibaba.fluss.lakehouse;

import java.util.Map;

/** Info about lake storage, used to create catalog or others for lake format. */
public class LakeStorageInfo {

    // the type of lake storage
    private final String lakeStorage;
    private final Map<String, String> catalogProperties;

    public LakeStorageInfo(String lakeStorage, Map<String, String> catalogProperties) {
        this.lakeStorage = lakeStorage;
        this.catalogProperties = catalogProperties;
    }

    public String getLakeStorage() {
        return lakeStorage;
    }

    public Map<String, String> getCatalogProperties() {
        return catalogProperties;
    }
}
