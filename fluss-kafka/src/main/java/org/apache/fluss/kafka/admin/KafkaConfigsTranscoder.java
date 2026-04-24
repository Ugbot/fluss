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
import org.apache.fluss.config.Configuration;
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

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Translates Kafka {@code DescribeConfigs}/{@code AlterConfigs}/{@code IncrementalAlterConfigs}
 * requests against topic <i>and</i> broker resources into reads/writes of Fluss table properties.
 *
 * <p>Phase K-CFG (plan §28) widened the surface so what a kafka-clients {@code AdminClient} or
 * {@code kafka-configs.sh} sees on this broker matches what a real Kafka broker reports:
 *
 * <ul>
 *   <li>TOPIC: {@link KafkaTopicConfigs} drives the catalogue; three tiers (MAPPED / STORED /
 *       READONLY_DEFAULT) control Describe output and Alter validation.
 *   <li>BROKER: {@link KafkaBrokerConfigs} returns a read-only catalogue of the canonical broker
 *       keys, with values reflecting the running Fluss configuration where derivable. AlterConfigs
 *       on BROKER continues to be rejected with {@code INVALID_REQUEST}.
 *   <li>BROKER_LOGGER: still rejected (dynamic log-level alter is out of scope).
 *   <li>Unknown config keys on a topic round-trip verbatim as custom properties — preserves the
 *       Phase D "user annotation" behaviour (e.g. {@code ext.owner=team-foo}).
 * </ul>
 */
@Internal
public final class KafkaConfigsTranscoder {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConfigsTranscoder.class);

    /** DescribeConfigs ConfigSource byte constants mirrored from Kafka's schema. */
    private static final byte CONFIG_SOURCE_DYNAMIC_TOPIC_CONFIG = (byte) 1;

    private static final byte CONFIG_SOURCE_STATIC_BROKER_CONFIG = (byte) 4;
    private static final byte CONFIG_SOURCE_DEFAULT_CONFIG = (byte) 5;

    /** DescribeConfigs ConfigType byte values. */
    private static final byte CONFIG_TYPE_BOOLEAN = (byte) 1;

    private static final byte CONFIG_TYPE_STRING = (byte) 2;
    private static final byte CONFIG_TYPE_INT = (byte) 3;
    private static final byte CONFIG_TYPE_LONG = (byte) 5;
    private static final byte CONFIG_TYPE_DOUBLE = (byte) 6;
    private static final byte CONFIG_TYPE_LIST = (byte) 7;

    /** IncrementalAlterConfigs OpType codes from the Kafka protocol. */
    private static final byte OP_SET = (byte) 0;

    private static final byte OP_DELETE = (byte) 1;
    private static final byte OP_APPEND = (byte) 2;
    private static final byte OP_SUBTRACT = (byte) 3;

    private final MetadataManager metadataManager;
    private final KafkaTopicsCatalog catalog;
    private final String kafkaDatabase;
    private final Configuration serverConf;
    private final int brokerId;

    public KafkaConfigsTranscoder(
            MetadataManager metadataManager, KafkaTopicsCatalog catalog, String kafkaDatabase) {
        this(metadataManager, catalog, kafkaDatabase, new Configuration(), 0);
    }

    public KafkaConfigsTranscoder(
            MetadataManager metadataManager,
            KafkaTopicsCatalog catalog,
            String kafkaDatabase,
            Configuration serverConf,
            int brokerId) {
        this.metadataManager = metadataManager;
        this.catalog = catalog;
        this.kafkaDatabase = kafkaDatabase;
        this.serverConf = serverConf == null ? new Configuration() : serverConf;
        this.brokerId = brokerId;
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
        boolean includeDocumentation = request.includeDocumentation();
        for (DescribeConfigsResource resource : request.resources()) {
            response.results().add(describeOne(resource, includeDocumentation));
        }
        return response;
    }

    private DescribeConfigsResult describeOne(
            DescribeConfigsResource resource, boolean includeDocumentation) {
        DescribeConfigsResult result =
                new DescribeConfigsResult()
                        .setResourceType(resource.resourceType())
                        .setResourceName(resource.resourceName())
                        .setConfigs(new ArrayList<>());

        ConfigResource.Type type = ConfigResource.Type.forId(resource.resourceType());
        if (type == ConfigResource.Type.TOPIC) {
            return describeTopic(result, resource, includeDocumentation);
        }
        if (type == ConfigResource.Type.BROKER) {
            return describeBroker(result, resource, includeDocumentation);
        }
        return result.setErrorCode(Errors.INVALID_REQUEST.code())
                .setErrorMessage(
                        "Fluss Kafka-compat does not support ConfigResource.Type."
                                + type
                                + " for DescribeConfigs.");
    }

    private DescribeConfigsResult describeTopic(
            DescribeConfigsResult result,
            DescribeConfigsResource resource,
            boolean includeDocumentation) {
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
        List<DescribeConfigsResourceResult> entries =
                buildTopicEntries(tableInfo.get(), requested, includeDocumentation);
        return result.setErrorCode(Errors.NONE.code()).setErrorMessage(null).setConfigs(entries);
    }

    private List<DescribeConfigsResourceResult> buildTopicEntries(
            TableInfo tableInfo, Set<String> requestedKeys, boolean includeDocumentation) {
        Map<String, String> tableProps = tableInfo.getProperties().toMap();
        Map<String, String> customProps = tableInfo.getCustomProperties().toMap();

        Map<String, DescribeConfigsResourceResult> entries = new LinkedHashMap<>();

        // 1. Catalogue entries — every catalogued key is returned.
        for (Map.Entry<String, KafkaTopicConfigs.Entry> catalogued :
                KafkaTopicConfigs.entries().entrySet()) {
            String kafkaKey = catalogued.getKey();
            KafkaTopicConfigs.Entry entry = catalogued.getValue();
            if (!matchesRequested(kafkaKey, requestedKeys)) {
                continue;
            }
            String value = resolveCataloguedValue(entry, tableProps, customProps);
            boolean isOverride = value != null;
            if (!isOverride) {
                value = entry.defaultValue;
            }
            byte source =
                    isOverride ? CONFIG_SOURCE_DYNAMIC_TOPIC_CONFIG : CONFIG_SOURCE_DEFAULT_CONFIG;
            boolean readOnly = entry.tier == KafkaConfigTier.READONLY_DEFAULT;
            entries.put(
                    kafkaKey,
                    newEntry(
                            kafkaKey,
                            value,
                            readOnly,
                            source,
                            typeByte(entry.type),
                            entry.isSensitive,
                            includeDocumentation ? entry.documentation : null));
        }

        // 2. User-annotation custom properties (not in the catalogue) — round-trip verbatim.
        for (Map.Entry<String, String> e : customProps.entrySet()) {
            String k = e.getKey();
            if (isReservedKafkaBindingKey(k)) {
                continue;
            }
            if (KafkaTopicConfigs.get(k) != null) {
                continue; // already surfaced via the catalogue path.
            }
            // Skip internal Fluss custom-property mirrors of catalogued Fluss keys.
            if (isCataloguedFlussKey(k)) {
                continue;
            }
            if (!matchesRequested(k, requestedKeys)) {
                continue;
            }
            entries.put(
                    k,
                    newEntry(
                            k,
                            e.getValue(),
                            /* readOnly */ false,
                            CONFIG_SOURCE_DYNAMIC_TOPIC_CONFIG,
                            CONFIG_TYPE_STRING,
                            /* isSensitive */ false,
                            /* documentation */ null));
        }

        return new ArrayList<>(entries.values());
    }

    private DescribeConfigsResult describeBroker(
            DescribeConfigsResult result,
            DescribeConfigsResource resource,
            boolean includeDocumentation) {
        String requestedBroker = resource.resourceName();
        if (requestedBroker != null
                && !requestedBroker.isEmpty()
                && !requestedBroker.equals(Integer.toString(brokerId))) {
            // Clients often query "0" / "". Accept either; otherwise 404.
            return result.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                    .setErrorMessage(
                            "This broker is id "
                                    + brokerId
                                    + "; describeConfigs asked for '"
                                    + requestedBroker
                                    + "'.");
        }
        Set<String> requested = toKeySet(resource.configurationKeys());
        Map<String, KafkaBrokerConfigs.Entry> broker =
                KafkaBrokerConfigs.entries(serverConf, brokerId);
        List<DescribeConfigsResourceResult> entries = new ArrayList<>(broker.size());
        for (KafkaBrokerConfigs.Entry entry : broker.values()) {
            if (!matchesRequested(entry.kafkaName, requested)) {
                continue;
            }
            entries.add(
                    newEntry(
                            entry.kafkaName,
                            entry.value,
                            /* readOnly */ true,
                            CONFIG_SOURCE_STATIC_BROKER_CONFIG,
                            CONFIG_TYPE_STRING,
                            entry.isSensitive,
                            includeDocumentation ? entry.documentation : null));
        }
        return result.setErrorCode(Errors.NONE.code()).setErrorMessage(null).setConfigs(entries);
    }

    /**
     * Look up a catalogued entry's current value on a table. Returns null when neither the mapped
     * table property nor the custom-property fallback is set.
     */
    @Nullable
    private static String resolveCataloguedValue(
            KafkaTopicConfigs.Entry entry,
            Map<String, String> tableProps,
            Map<String, String> customProps) {
        if (entry.tier == KafkaConfigTier.READONLY_DEFAULT) {
            return null;
        }
        String raw = null;
        if (entry.tier == KafkaConfigTier.MAPPED && entry.flussKey != null) {
            raw =
                    entry.flussKeyIsTableProperty
                            ? tableProps.get(entry.flussKey)
                            : customProps.get(entry.flussKey);
        }
        if (raw == null) {
            // STORED tier (and MAPPED fall-through): the kafka key itself may be stored as a
            // custom property.
            raw = customProps.get(entry.kafkaName);
        }
        if (raw == null) {
            return null;
        }
        return entry.flussToKafka != null ? entry.flussToKafka.apply(raw) : raw;
    }

    /** Kafka keys' Fluss counterparts we hide from the "user annotation" passthrough. */
    private static boolean isCataloguedFlussKey(String flussKey) {
        for (KafkaTopicConfigs.Entry e : KafkaTopicConfigs.entries().values()) {
            if (e.flussKey != null && e.flussKey.equals(flussKey)) {
                return true;
            }
        }
        return false;
    }

    private static byte typeByte(KafkaTopicConfigs.ConfigType t) {
        switch (t) {
            case BOOLEAN:
                return CONFIG_TYPE_BOOLEAN;
            case INT:
                return CONFIG_TYPE_INT;
            case LONG:
                return CONFIG_TYPE_LONG;
            case DOUBLE:
                return CONFIG_TYPE_DOUBLE;
            case LIST:
                return CONFIG_TYPE_LIST;
            case STRING:
            default:
                return CONFIG_TYPE_STRING;
        }
    }

    private static DescribeConfigsResourceResult newEntry(
            String name,
            String value,
            boolean readOnly,
            byte source,
            byte configType,
            boolean isSensitive,
            @Nullable String documentation) {
        return new DescribeConfigsResourceResult()
                .setName(name)
                .setValue(value)
                .setReadOnly(readOnly)
                .setIsDefault(false)
                .setConfigSource(source)
                .setIsSensitive(isSensitive)
                .setConfigType(configType)
                .setDocumentation(documentation);
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

        // v0 full-replace semantics. Accept catalogued keys with validation; unknown keys still
        // round-trip as custom properties (Phase D compat).
        Map<String, String> currentCustom = existing.get().getCustomProperties().toMap();
        Map<String, String> currentTable = existing.get().getProperties().toMap();
        Map<String, AlterableConfig> desired = new LinkedHashMap<>();
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
                KafkaTopicConfigs.Entry entry = KafkaTopicConfigs.get(cfg.name());
                if (entry != null) {
                    String err = validateCatalogued(entry, cfg.value());
                    if (err != null) {
                        return out.setErrorCode(Errors.INVALID_CONFIG.code()).setErrorMessage(err);
                    }
                }
                desired.put(cfg.name(), cfg);
            }
        }

        TablePropertyChanges.Builder builder = TablePropertyChanges.builder();

        // Reset every prior override the caller omitted (keep reserved + unmapped table props).
        for (String key : currentCustom.keySet()) {
            if (isReservedKafkaBindingKey(key)) {
                continue;
            }
            if (!desired.containsKey(key) && !isCataloguedKafkaKey(key)) {
                builder.resetCustomProperty(key);
            }
        }
        // Reset any MAPPED table-property keys whose kafka counterpart was dropped.
        for (KafkaTopicConfigs.Entry entry : KafkaTopicConfigs.entries().values()) {
            if (entry.tier != KafkaConfigTier.MAPPED || entry.flussKey == null) {
                continue;
            }
            if (desired.containsKey(entry.kafkaName)) {
                continue;
            }
            if (entry.flussKeyIsTableProperty && currentTable.containsKey(entry.flussKey)) {
                builder.resetTableProperty(entry.flussKey);
            } else if (!entry.flussKeyIsTableProperty
                    && currentCustom.containsKey(entry.flussKey)) {
                builder.resetCustomProperty(entry.flussKey);
            }
        }

        // Apply desired.
        for (Map.Entry<String, AlterableConfig> e : desired.entrySet()) {
            applyCataloguedOrCustom(builder, e.getKey(), e.getValue().value());
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
                KafkaTopicConfigs.Entry entry = KafkaTopicConfigs.get(cfg.name());
                byte op = cfg.configOperation();
                if (op == OP_SET) {
                    if (cfg.value() == null) {
                        return out.setErrorCode(Errors.INVALID_CONFIG.code())
                                .setErrorMessage(
                                        "SET operation requires a value for '" + cfg.name() + "'.");
                    }
                    if (entry != null) {
                        String err = validateCatalogued(entry, cfg.value());
                        if (err != null) {
                            return out.setErrorCode(Errors.INVALID_CONFIG.code())
                                    .setErrorMessage(err);
                        }
                    }
                    applyCataloguedOrCustom(builder, cfg.name(), cfg.value());
                } else if (op == OP_DELETE) {
                    if (entry != null && entry.tier == KafkaConfigTier.READONLY_DEFAULT) {
                        // Already default — no-op success per plan §28.5.
                        continue;
                    }
                    if (entry != null
                            && entry.tier == KafkaConfigTier.MAPPED
                            && entry.flussKey != null) {
                        if (entry.flussKeyIsTableProperty) {
                            builder.resetTableProperty(entry.flussKey);
                        } else {
                            builder.resetCustomProperty(entry.flussKey);
                        }
                    } else {
                        // STORED catalogued or user annotation — custom-property reset is enough.
                        builder.resetCustomProperty(cfg.name());
                    }
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

    /**
     * Validate an alter against a catalogued entry. Returns null on success, otherwise an error
     * string suitable for INVALID_CONFIG.
     */
    @Nullable
    private static String validateCatalogued(KafkaTopicConfigs.Entry entry, String value) {
        if (entry.tier == KafkaConfigTier.READONLY_DEFAULT) {
            return "Config '"
                    + entry.kafkaName
                    + "' is read-only on this broker; alter is not supported.";
        }
        return KafkaTopicConfigs.validateValue(entry, value);
    }

    /**
     * Write an accepted alter into the property-changes builder. MAPPED entries land on the right
     * Fluss target (table property or custom property) with value conversion; STORED and user
     * annotations land on the custom-property map.
     */
    private static void applyCataloguedOrCustom(
            TablePropertyChanges.Builder builder, String kafkaKey, String kafkaValue) {
        KafkaTopicConfigs.Entry entry = KafkaTopicConfigs.get(kafkaKey);
        if (entry != null && entry.tier == KafkaConfigTier.MAPPED && entry.flussKey != null) {
            String flussValue =
                    entry.kafkaToFluss != null ? entry.kafkaToFluss.apply(kafkaValue) : kafkaValue;
            if (entry.flussKeyIsTableProperty) {
                builder.setTableProperty(entry.flussKey, flussValue);
            } else {
                builder.setCustomProperty(entry.flussKey, flussValue);
            }
            return;
        }
        builder.setCustomProperty(kafkaKey, kafkaValue);
    }

    private static boolean isCataloguedKafkaKey(String key) {
        return KafkaTopicConfigs.get(key) != null;
    }

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

    /** See {@link KafkaConfigsTranscoder} class doc; same semantics as the prior implementation. */
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
