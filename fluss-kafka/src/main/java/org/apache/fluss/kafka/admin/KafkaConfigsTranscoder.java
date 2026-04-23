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

package org.apache.fluss.kafka.admin;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.kafka.catalog.CustomPropertiesTopicsCatalog;
import org.apache.fluss.kafka.catalog.KafkaTopicsCatalog;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.entity.TablePropertyChanges;

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.AlterConfigsRequestData;
import org.apache.kafka.common.message.AlterConfigsRequestData.AlterConfigsResource;
import org.apache.kafka.common.message.AlterConfigsRequestData.AlterableConfig;
import org.apache.kafka.common.message.AlterConfigsResponseData;
import org.apache.kafka.common.message.AlterConfigsResponseData.AlterConfigsResourceResponse;
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.DescribeConfigsRequestData.DescribeConfigsResource;
import org.apache.kafka.common.message.DescribeConfigsResponseData;
import org.apache.kafka.common.message.DescribeConfigsResponseData.DescribeConfigsResourceResult;
import org.apache.kafka.common.message.DescribeConfigsResponseData.DescribeConfigsResult;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Translates Kafka {@code DescribeConfigs}/{@code AlterConfigs}/{@code IncrementalAlterConfigs}
 * requests against topic resources into reads/writes of Fluss table custom properties via {@link
 * MetadataManager#alterTableProperties}.
 *
 * <p>Scope is topic-only in this phase: requests for {@link ConfigResource.Type#BROKER} and {@code
 * BROKER_LOGGER} are rejected with {@link Errors#INVALID_REQUEST}. The set of well-known Kafka
 * config keys exposed in {@code DescribeConfigs} results is deliberately small; arbitrary
 * user-supplied keys round-trip verbatim via the Fluss {@code customProperties} map.
 */
@Internal
public final class KafkaConfigsTranscoder {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConfigsTranscoder.class);

    /**
     * Kafka topic-level config keys this phase surfaces on every DescribeConfigs call, derived from
     * known Fluss {@link ConfigOptions}. Every entry round-trips through {@link
     * MetadataManager#alterTableProperties} as a Fluss table (not custom) property.
     *
     * <p>Unknown Kafka keys we don't recognise go through as custom properties verbatim; that keeps
     * tools that persist their own annotations (e.g. "owner=team-foo") working without schema
     * extensions.
     */
    private static final Map<String, String> KAFKA_TO_FLUSS_WELL_KNOWN;

    static {
        Map<String, String> m = new LinkedHashMap<>();
        // Kafka retention semantics map cleanest to Fluss's table.log.ttl (a Duration).
        m.put("retention.ms", ConfigOptions.TABLE_LOG_TTL.key());
        // Compression advertised to Kafka clients; Fluss stores payload uncompressed but a topic's
        // binding records its declared compression for re-framing on the wire.
        m.put("compression.type", CustomPropertiesTopicsCatalog.PROP_COMPRESSION);
        m.put("message.timestamp.type", CustomPropertiesTopicsCatalog.PROP_TIMESTAMP_TYPE);
        KAFKA_TO_FLUSS_WELL_KNOWN = Collections.unmodifiableMap(m);
    }

    /** DescribeConfigs ConfigSource byte constants mirrored from Kafka's schema. */
    private static final byte CONFIG_SOURCE_TOPIC = (byte) 1;

    private static final byte CONFIG_SOURCE_DEFAULT = (byte) 5;

    /** DescribeConfigs ConfigType byte: string (default). */
    private static final byte CONFIG_TYPE_STRING = (byte) 2;

    /** IncrementalAlterConfigs OpType codes from the Kafka protocol. */
    private static final byte OP_SET = (byte) 0;

    private static final byte OP_DELETE = (byte) 1;
    private static final byte OP_APPEND = (byte) 2;
    private static final byte OP_SUBTRACT = (byte) 3;

    private final MetadataManager metadataManager;
    private final KafkaTopicsCatalog catalog;
    private final String kafkaDatabase;

    public KafkaConfigsTranscoder(
            MetadataManager metadataManager, KafkaTopicsCatalog catalog, String kafkaDatabase) {
        this.metadataManager = metadataManager;
        this.catalog = catalog;
        this.kafkaDatabase = kafkaDatabase;
    }

    // ------------------------------------------------------------------------
    // DescribeConfigs
    // ------------------------------------------------------------------------

    public DescribeConfigsResponseData describeConfigs(DescribeConfigsRequestData request) {
        DescribeConfigsResponseData response = new DescribeConfigsResponseData();
        response.setThrottleTimeMs(0);
        if (request.resources() == null) {
            return response;
        }
        for (DescribeConfigsResource resource : request.resources()) {
            response.results().add(describeOne(resource));
        }
        return response;
    }

    private DescribeConfigsResult describeOne(DescribeConfigsResource resource) {
        DescribeConfigsResult result =
                new DescribeConfigsResult()
                        .setResourceType(resource.resourceType())
                        .setResourceName(resource.resourceName())
                        .setConfigs(new ArrayList<>());

        ConfigResource.Type type = ConfigResource.Type.forId(resource.resourceType());
        if (type != ConfigResource.Type.TOPIC) {
            return result.setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage(
                            "Fluss Kafka-compat only supports ConfigResource.Type.TOPIC for "
                                    + "DescribeConfigs; got "
                                    + type
                                    + ".");
        }

        String topic = resource.resourceName();
        if (topic == null || topic.isEmpty()) {
            return result.setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code())
                    .setErrorMessage("Topic name is null or empty.");
        }

        Optional<TableInfo> tableInfo = loadKafkaTable(topic);
        if (!tableInfo.isPresent()) {
            return result.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                    .setErrorMessage("Topic '" + topic + "' does not exist.");
        }

        Set<String> requested = toKeySet(resource.configurationKeys());
        List<DescribeConfigsResourceResult> entries = buildEntries(tableInfo.get(), requested);
        return result.setErrorCode(Errors.NONE.code()).setErrorMessage(null).setConfigs(entries);
    }

    private List<DescribeConfigsResourceResult> buildEntries(
            TableInfo tableInfo, Set<String> requestedKeys) {
        Map<String, String> flussProps = tableInfo.getProperties().toMap();
        Map<String, String> customProps = tableInfo.getCustomProperties().toMap();

        // Preserve insertion order so the response is deterministic.
        Map<String, DescribeConfigsResourceResult> entries = new LinkedHashMap<>();

        // 1) Well-known Kafka keys projected from Fluss properties (curated set).
        for (Map.Entry<String, String> mapping : KAFKA_TO_FLUSS_WELL_KNOWN.entrySet()) {
            String kafkaKey = mapping.getKey();
            String flussKey = mapping.getValue();
            if (!matchesRequested(kafkaKey, requestedKeys)) {
                continue;
            }
            String value = resolveWellKnownValue(kafkaKey, flussKey, flussProps, customProps);
            boolean isDefault = value == null;
            entries.put(
                    kafkaKey,
                    newEntry(
                            kafkaKey,
                            value,
                            /* readOnly */ false,
                            isDefault ? CONFIG_SOURCE_DEFAULT : CONFIG_SOURCE_TOPIC));
        }

        // 2) Custom properties, verbatim. Skip CustomPropertiesTopicsCatalog internal markers —
        // they are implementation-owned bindings, not user-visible topic configs.
        for (Map.Entry<String, String> e : customProps.entrySet()) {
            String k = e.getKey();
            if (isReservedKafkaBindingKey(k)) {
                continue;
            }
            if (!matchesRequested(k, requestedKeys)) {
                continue;
            }
            if (entries.containsKey(k)) {
                // Already surfaced as a well-known key (same key name); don't duplicate.
                continue;
            }
            entries.put(k, newEntry(k, e.getValue(), /* readOnly */ false, CONFIG_SOURCE_TOPIC));
        }

        return new ArrayList<>(entries.values());
    }

    private static String resolveWellKnownValue(
            String kafkaKey,
            String flussKey,
            Map<String, String> flussProps,
            Map<String, String> customProps) {
        String raw = flussProps.get(flussKey);
        if (raw == null) {
            raw = customProps.get(flussKey);
        }
        if (raw == null) {
            return null;
        }
        if ("retention.ms".equals(kafkaKey)) {
            // Fluss persists a Duration string ("P7D", "1 h", "-1 ms"). Convert to ms so Kafka
            // clients get an int64 they understand.
            return durationToMillis(raw);
        }
        return raw;
    }

    private static String durationToMillis(String duration) {
        try {
            // Try ISO-8601 first; fall back to Fluss's TimeUtils semantics ("7 d", "1 h").
            return Long.toString(Duration.parse(duration).toMillis());
        } catch (Exception ignore) {
            try {
                return Long.toString(
                        org.apache.fluss.utils.TimeUtils.parseDuration(duration).toMillis());
            } catch (Exception nested) {
                LOG.debug("Could not parse duration '{}' for retention.ms", duration, nested);
                return duration;
            }
        }
    }

    private static DescribeConfigsResourceResult newEntry(
            String name, String value, boolean readOnly, byte source) {
        // Kafka's DescribeConfigsResourceResult serializer rejects isDefault=true on v1+, where
        // clients instead read the configSource byte. Keep isDefault=false and communicate
        // defaulted entries via configSource=DEFAULT_CONFIG (5). v0 clients lose the isDefault
        // marker but continue to get the correct value, which matches server-side expectations
        // when talking to a flexible-versions AdminClient.
        return new DescribeConfigsResourceResult()
                .setName(name)
                .setValue(value)
                .setReadOnly(readOnly)
                .setIsDefault(false)
                .setConfigSource(source)
                .setIsSensitive(false)
                .setConfigType(CONFIG_TYPE_STRING)
                .setDocumentation(null);
    }

    private static Set<String> toKeySet(List<String> keys) {
        if (keys == null || keys.isEmpty()) {
            return Collections.emptySet();
        }
        return new java.util.HashSet<>(keys);
    }

    private static boolean matchesRequested(String key, Set<String> requested) {
        return requested.isEmpty() || requested.contains(key);
    }

    private static boolean isReservedKafkaBindingKey(String key) {
        return CustomPropertiesTopicsCatalog.PROP_BINDING_MARKER.equals(key)
                || CustomPropertiesTopicsCatalog.PROP_TOPIC_NAME.equals(key)
                || CustomPropertiesTopicsCatalog.PROP_TOPIC_ID.equals(key);
    }

    // ------------------------------------------------------------------------
    // AlterConfigs (v0 full-replace semantics)
    // ------------------------------------------------------------------------

    public AlterConfigsResponseData alterConfigs(AlterConfigsRequestData request) {
        AlterConfigsResponseData response = new AlterConfigsResponseData();
        response.setThrottleTimeMs(0);
        boolean validateOnly = request.validateOnly();
        if (request.resources() == null) {
            return response;
        }
        for (AlterConfigsResource resource : request.resources()) {
            response.responses().add(alterOne(resource, validateOnly));
        }
        return response;
    }

    private AlterConfigsResourceResponse alterOne(AlterConfigsResource resource, boolean validate) {
        AlterConfigsResourceResponse out =
                new AlterConfigsResourceResponse()
                        .setResourceType(resource.resourceType())
                        .setResourceName(resource.resourceName());

        ConfigResource.Type type = ConfigResource.Type.forId(resource.resourceType());
        if (type != ConfigResource.Type.TOPIC) {
            return out.setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage(
                            "Fluss Kafka-compat only supports ConfigResource.Type.TOPIC for "
                                    + "AlterConfigs; got "
                                    + type
                                    + ".");
        }

        String topic = resource.resourceName();
        Optional<TableInfo> existing = loadKafkaTable(topic);
        if (!existing.isPresent()) {
            return out.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                    .setErrorMessage("Topic '" + topic + "' does not exist.");
        }

        // v0 full-replace semantics: compute the diff against the current custom properties so the
        // coordinator sees a minimal TablePropertyChanges. Any existing custom property whose key
        // is not in the new set is reset; reserved binding keys are preserved untouched.
        Map<String, String> currentCustom = existing.get().getCustomProperties().toMap();
        Map<String, String> desired = new LinkedHashMap<>();
        if (resource.configs() != null) {
            for (AlterableConfig cfg : resource.configs()) {
                if (cfg.name() == null) {
                    return out.setErrorCode(Errors.INVALID_CONFIG.code())
                            .setErrorMessage("Config key must not be null.");
                }
                if (isReservedKafkaBindingKey(cfg.name())) {
                    return out.setErrorCode(Errors.INVALID_CONFIG.code())
                            .setErrorMessage(
                                    "Config key '"
                                            + cfg.name()
                                            + "' is reserved by the Kafka-compat binding and"
                                            + " cannot be altered.");
                }
                if (cfg.value() == null) {
                    return out.setErrorCode(Errors.INVALID_CONFIG.code())
                            .setErrorMessage(
                                    "AlterConfigs v0 does not permit null values (use"
                                            + " IncrementalAlterConfigs with DELETE instead).");
                }
                desired.put(cfg.name(), cfg.value());
            }
        }

        TablePropertyChanges.Builder builder = TablePropertyChanges.builder();
        // Reset everything the caller didn't include (except the reserved binding keys).
        for (String key : currentCustom.keySet()) {
            if (isReservedKafkaBindingKey(key)) {
                continue;
            }
            if (!desired.containsKey(key)) {
                builder.resetCustomProperty(key);
            }
        }
        // Set / overwrite the desired keys.
        for (Map.Entry<String, String> e : desired.entrySet()) {
            builder.setCustomProperty(e.getKey(), e.getValue());
        }

        if (validate) {
            return out.setErrorCode(Errors.NONE.code());
        }

        return applyPropertyChanges(out, existing.get().getTablePath(), builder.build());
    }

    // ------------------------------------------------------------------------
    // IncrementalAlterConfigs (v1+ delta semantics)
    // ------------------------------------------------------------------------

    public IncrementalAlterConfigsResponseData incrementalAlterConfigs(
            IncrementalAlterConfigsRequestData request) {
        IncrementalAlterConfigsResponseData response = new IncrementalAlterConfigsResponseData();
        response.setThrottleTimeMs(0);
        boolean validateOnly = request.validateOnly();
        if (request.resources() == null) {
            return response;
        }
        for (IncrementalAlterConfigsRequestData.AlterConfigsResource resource :
                request.resources()) {
            response.responses().add(incrementalOne(resource, validateOnly));
        }
        return response;
    }

    private IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse incrementalOne(
            IncrementalAlterConfigsRequestData.AlterConfigsResource resource, boolean validate) {
        IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse out =
                new IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse()
                        .setResourceType(resource.resourceType())
                        .setResourceName(resource.resourceName());

        ConfigResource.Type type = ConfigResource.Type.forId(resource.resourceType());
        if (type != ConfigResource.Type.TOPIC) {
            return out.setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage(
                            "Fluss Kafka-compat only supports ConfigResource.Type.TOPIC for "
                                    + "IncrementalAlterConfigs; got "
                                    + type
                                    + ".");
        }

        String topic = resource.resourceName();
        Optional<TableInfo> existing = loadKafkaTable(topic);
        if (!existing.isPresent()) {
            return out.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                    .setErrorMessage("Topic '" + topic + "' does not exist.");
        }

        TablePropertyChanges.Builder builder = TablePropertyChanges.builder();
        if (resource.configs() != null) {
            for (IncrementalAlterConfigsRequestData.AlterableConfig cfg : resource.configs()) {
                if (cfg.name() == null) {
                    return out.setErrorCode(Errors.INVALID_CONFIG.code())
                            .setErrorMessage("Config key must not be null.");
                }
                if (isReservedKafkaBindingKey(cfg.name())) {
                    return out.setErrorCode(Errors.INVALID_CONFIG.code())
                            .setErrorMessage(
                                    "Config key '"
                                            + cfg.name()
                                            + "' is reserved by the Kafka-compat binding and"
                                            + " cannot be altered.");
                }
                byte op = cfg.configOperation();
                if (op == OP_SET) {
                    if (cfg.value() == null) {
                        return out.setErrorCode(Errors.INVALID_CONFIG.code())
                                .setErrorMessage(
                                        "SET operation requires a value for '" + cfg.name() + "'.");
                    }
                    builder.setCustomProperty(cfg.name(), cfg.value());
                } else if (op == OP_DELETE) {
                    builder.resetCustomProperty(cfg.name());
                } else if (op == OP_APPEND || op == OP_SUBTRACT) {
                    return out.setErrorCode(Errors.INVALID_CONFIG.code())
                            .setErrorMessage(
                                    (op == OP_APPEND ? "APPEND" : "SUBTRACT")
                                            + " is unsupported: Fluss Kafka-compat does not map"
                                            + " list-valued topic configs.");
                } else {
                    return out.setErrorCode(Errors.INVALID_CONFIG.code())
                            .setErrorMessage(
                                    "Unknown IncrementalAlterConfigs op="
                                            + op
                                            + " for key '"
                                            + cfg.name()
                                            + "'.");
                }
            }
        }

        if (validate) {
            return out.setErrorCode(Errors.NONE.code());
        }

        TablePropertyChanges changes = builder.build();
        return applyIncrementalChanges(out, existing.get().getTablePath(), changes);
    }

    // ------------------------------------------------------------------------
    // shared helpers
    // ------------------------------------------------------------------------

    private AlterConfigsResourceResponse applyPropertyChanges(
            AlterConfigsResourceResponse out, TablePath path, TablePropertyChanges changes) {
        try {
            metadataManager.alterTableProperties(
                    path,
                    Collections.emptyList(),
                    changes,
                    /* ignoreIfNotExists */ false,
                    FlussPrincipal.ANONYMOUS);
            return out.setErrorCode(Errors.NONE.code());
        } catch (TableNotExistException gone) {
            return out.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                    .setErrorMessage(gone.getMessage());
        } catch (Exception e) {
            LOG.error("AlterConfigs failed for {}", path, e);
            return out.setErrorCode(Errors.INVALID_CONFIG.code())
                    .setErrorMessage(e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse
            applyIncrementalChanges(
                    IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse out,
                    TablePath path,
                    TablePropertyChanges changes) {
        try {
            metadataManager.alterTableProperties(
                    path,
                    Collections.emptyList(),
                    changes,
                    /* ignoreIfNotExists */ false,
                    FlussPrincipal.ANONYMOUS);
            return out.setErrorCode(Errors.NONE.code());
        } catch (TableNotExistException gone) {
            return out.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                    .setErrorMessage(gone.getMessage());
        } catch (Exception e) {
            LOG.error("IncrementalAlterConfigs failed for {}", path, e);
            return out.setErrorCode(Errors.INVALID_CONFIG.code())
                    .setErrorMessage(e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    /**
     * Resolve a Kafka topic name to its Fluss {@link TableInfo}, but only if the table is a
     * Kafka-managed binding. Returns {@link Optional#empty()} if the table is missing or if it
     * exists but wasn't created via Kafka CreateTopics (protects us from leaking configs of tables
     * owned by other frontends).
     */
    private Optional<TableInfo> loadKafkaTable(String topic) {
        if (topic == null || topic.isEmpty()) {
            return Optional.empty();
        }
        try {
            if (!catalog.lookup(topic).isPresent()) {
                return Optional.empty();
            }
        } catch (Exception e) {
            LOG.warn("Catalog lookup failed for '{}'", topic, e);
            return Optional.empty();
        }
        TablePath path = new TablePath(kafkaDatabase, topic);
        try {
            return Optional.of(metadataManager.getTable(path));
        } catch (TableNotExistException gone) {
            return Optional.empty();
        }
    }
}
