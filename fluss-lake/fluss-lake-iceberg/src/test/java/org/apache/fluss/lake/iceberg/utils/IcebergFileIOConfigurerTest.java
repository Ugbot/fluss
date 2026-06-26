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

package org.apache.fluss.lake.iceberg.utils;

import org.apache.iceberg.CatalogProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.lake.iceberg.utils.IcebergFileIOConfigurer.S3_FILE_IO_ENABLED_KEY;
import static org.apache.fluss.lake.iceberg.utils.IcebergFileIOConfigurer.S3_FILE_IO_IMPL;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link IcebergFileIOConfigurer}. */
class IcebergFileIOConfigurerTest {

    @ParameterizedTest
    @ValueSource(
            strings = {
                "s3://my-bucket/warehouse",
                "s3a://my-bucket/warehouse",
                "s3n://my-bucket/warehouse",
                "S3://My-Bucket/Warehouse"
            })
    void testSelectsS3FileIOForS3Warehouse(String warehouse) {
        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);

        IcebergFileIOConfigurer.configureFileIO(props);

        assertThat(props).containsEntry(CatalogProperties.FILE_IO_IMPL, S3_FILE_IO_IMPL);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "hdfs://namenode:8020/warehouse",
                "file:///tmp/warehouse",
                "/tmp/warehouse",
                "gs://my-bucket/warehouse",
                "oss://my-bucket/warehouse"
            })
    void testKeepsHadoopFileIOForNonS3Warehouse(String warehouse) {
        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);

        IcebergFileIOConfigurer.configureFileIO(props);

        // FileIO left untouched so HadoopFileIO remains the Iceberg default.
        assertThat(props).doesNotContainKey(CatalogProperties.FILE_IO_IMPL);
    }

    @Test
    void testFallsBackToCatalogUriWhenWarehouseAbsent() {
        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.URI, "s3://my-bucket/metadata");

        IcebergFileIOConfigurer.configureFileIO(props);

        assertThat(props).containsEntry(CatalogProperties.FILE_IO_IMPL, S3_FILE_IO_IMPL);
    }

    @Test
    void testExplicitIoImplIsNeverOverridden() {
        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://my-bucket/warehouse");
        props.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.hadoop.HadoopFileIO");

        IcebergFileIOConfigurer.configureFileIO(props);

        assertThat(props)
                .containsEntry(
                        CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.hadoop.HadoopFileIO");
    }

    @Test
    void testExplicitFlagForceEnablesForNonS3Warehouse() {
        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.WAREHOUSE_LOCATION, "file:///tmp/warehouse");
        props.put(S3_FILE_IO_ENABLED_KEY, "true");

        IcebergFileIOConfigurer.configureFileIO(props);

        assertThat(props).containsEntry(CatalogProperties.FILE_IO_IMPL, S3_FILE_IO_IMPL);
    }

    @Test
    void testExplicitFlagForceDisablesForS3Warehouse() {
        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://my-bucket/warehouse");
        props.put(S3_FILE_IO_ENABLED_KEY, "false");

        IcebergFileIOConfigurer.configureFileIO(props);

        assertThat(props).doesNotContainKey(CatalogProperties.FILE_IO_IMPL);
    }

    @Test
    void testNoWarehouseNoUriLeavesFileIODefault() {
        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.inmemory.InMemoryCatalog");

        IcebergFileIOConfigurer.configureFileIO(props);

        assertThat(props).doesNotContainKey(CatalogProperties.FILE_IO_IMPL);
    }
}
