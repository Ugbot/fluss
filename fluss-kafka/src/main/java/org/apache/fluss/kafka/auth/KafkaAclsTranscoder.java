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

package org.apache.fluss.kafka.auth;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.rpc.netty.server.Session;
import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.AccessControlEntryFilter;
import org.apache.fluss.security.acl.AclBinding;
import org.apache.fluss.security.acl.AclBindingFilter;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.PermissionType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.security.acl.ResourceFilter;
import org.apache.fluss.security.acl.ResourceType;
import org.apache.fluss.server.authorizer.Authorizer;

import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreateAclsResponseData;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.DeleteAclsResponseData;
import org.apache.kafka.common.message.DescribeAclsRequestData;
import org.apache.kafka.common.message.DescribeAclsResponseData;
import org.apache.kafka.common.protocol.Errors;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Bridges Kafka's ACL management wire APIs (DescribeAcls / CreateAcls / DeleteAcls) onto Fluss's
 * {@link Authorizer}.
 *
 * <p>Resource-type mapping (Kafka → Fluss):
 *
 * <ul>
 *   <li>TOPIC → TABLE in {@code kafkaDatabase}
 *   <li>GROUP → GROUP
 *   <li>CLUSTER → CLUSTER
 *   <li>TRANSACTIONAL_ID / DELEGATION_TOKEN / USER → unsupported, reported per-binding.
 * </ul>
 *
 * <p>Pattern types: only LITERAL (exact) is supported. PREFIXED and MATCH return an error.
 *
 * <p>Operation mapping: Kafka's extended operations (DESCRIBE_CONFIGS, ALTER_CONFIGS,
 * CLUSTER_ACTION, IDEMPOTENT_WRITE, CREATE_TOKENS, DESCRIBE_TOKENS) collapse onto Fluss's smaller
 * set — DESCRIBE_CONFIGS → DESCRIBE, ALTER_CONFIGS → ALTER, IDEMPOTENT_WRITE → WRITE, others
 * (CLUSTER_ACTION, *_TOKENS) are rejected with a clear message.
 */
@Internal
public final class KafkaAclsTranscoder {

    private final Authorizer authorizer;
    private final String kafkaDatabase;
    private final Session session;

    public KafkaAclsTranscoder(Authorizer authorizer, String kafkaDatabase, Session session) {
        this.authorizer = authorizer;
        this.kafkaDatabase = kafkaDatabase;
        this.session = session;
    }

    // ---------- DescribeAcls ----------

    public DescribeAclsResponseData describeAcls(DescribeAclsRequestData request) {
        DescribeAclsResponseData response = new DescribeAclsResponseData();

        AclBindingFilter filter;
        try {
            filter = toFlussFilter(request);
        } catch (KafkaAclMappingException e) {
            response.setErrorCode(Errors.INVALID_REQUEST.code()).setErrorMessage(e.getMessage());
            return response;
        }

        Collection<AclBinding> acls;
        try {
            acls = authorizer.listAcls(session, filter);
        } catch (Throwable t) {
            response.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                    .setErrorMessage(t.getMessage());
            return response;
        }

        Map<Resource, DescribeAclsResponseData.DescribeAclsResource> byResource =
                new LinkedHashMap<>();
        for (AclBinding binding : acls) {
            DescribeAclsResponseData.DescribeAclsResource r =
                    byResource.computeIfAbsent(
                            binding.getResource(),
                            res ->
                                    toKafkaResource(
                                            res,
                                            new DescribeAclsResponseData.DescribeAclsResource()));
            r.acls().add(toKafkaEntry(binding.getAccessControlEntry()));
        }
        response.resources().addAll(byResource.values());
        response.setErrorCode(Errors.NONE.code());
        return response;
    }

    // ---------- CreateAcls ----------

    public CreateAclsResponseData createAcls(CreateAclsRequestData request) {
        CreateAclsResponseData response = new CreateAclsResponseData();
        List<AclBinding> bindings = new ArrayList<>();
        List<Integer> indexOfBinding = new ArrayList<>();
        List<CreateAclsResponseData.AclCreationResult> results = new ArrayList<>();

        for (int i = 0; i < request.creations().size(); i++) {
            results.add(new CreateAclsResponseData.AclCreationResult());
        }

        for (int i = 0; i < request.creations().size(); i++) {
            CreateAclsRequestData.AclCreation creation = request.creations().get(i);
            try {
                bindings.add(toFlussBinding(creation));
                indexOfBinding.add(i);
            } catch (KafkaAclMappingException e) {
                results.get(i)
                        .setErrorCode(Errors.INVALID_REQUEST.code())
                        .setErrorMessage(e.getMessage());
            }
        }

        if (!bindings.isEmpty()) {
            try {
                authorizer.addAcls(session, bindings);
                for (int j = 0; j < bindings.size(); j++) {
                    results.get(indexOfBinding.get(j)).setErrorCode(Errors.NONE.code());
                }
            } catch (org.apache.fluss.exception.AuthorizationException denied) {
                for (Integer idx : indexOfBinding) {
                    results.get(idx)
                            .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code())
                            .setErrorMessage(denied.getMessage());
                }
            } catch (Throwable t) {
                for (Integer idx : indexOfBinding) {
                    results.get(idx)
                            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                            .setErrorMessage(t.getMessage());
                }
            }
        }

        response.results().addAll(results);
        return response;
    }

    // ---------- DeleteAcls ----------

    public DeleteAclsResponseData deleteAcls(DeleteAclsRequestData request) {
        DeleteAclsResponseData response = new DeleteAclsResponseData();

        List<AclBindingFilter> filters = new ArrayList<>();
        List<Integer> indexOfFilter = new ArrayList<>();
        List<DeleteAclsResponseData.DeleteAclsFilterResult> results = new ArrayList<>();

        for (int i = 0; i < request.filters().size(); i++) {
            results.add(new DeleteAclsResponseData.DeleteAclsFilterResult());
        }

        for (int i = 0; i < request.filters().size(); i++) {
            DeleteAclsRequestData.DeleteAclsFilter kf = request.filters().get(i);
            try {
                filters.add(toFlussFilter(kf));
                indexOfFilter.add(i);
            } catch (KafkaAclMappingException e) {
                results.get(i)
                        .setErrorCode(Errors.INVALID_REQUEST.code())
                        .setErrorMessage(e.getMessage());
            }
        }

        if (!filters.isEmpty()) {
            try {
                // listAcls per filter, then call dropAcls on the resolved set.
                for (int j = 0; j < filters.size(); j++) {
                    AclBindingFilter f = filters.get(j);
                    Collection<AclBinding> matched = authorizer.listAcls(session, f);
                    if (!matched.isEmpty()) {
                        List<AclBindingFilter> exact = new ArrayList<>();
                        for (AclBinding b : matched) {
                            exact.add(
                                    new AclBindingFilter(
                                            new ResourceFilter(
                                                    b.getResource().getType(),
                                                    b.getResource().getName()),
                                            new AccessControlEntryFilter(
                                                    b.getAccessControlEntry().getPrincipal(),
                                                    b.getAccessControlEntry().getHost(),
                                                    b.getAccessControlEntry().getOperationType(),
                                                    b.getAccessControlEntry()
                                                            .getPermissionType())));
                        }
                        authorizer.dropAcls(session, exact);
                    }
                    results.get(indexOfFilter.get(j)).setErrorCode(Errors.NONE.code());
                    for (AclBinding b : matched) {
                        DeleteAclsResponseData.DeleteAclsMatchingAcl m =
                                new DeleteAclsResponseData.DeleteAclsMatchingAcl()
                                        .setResourceType(
                                                mapFlussResourceType(b.getResource().getType()))
                                        .setResourceName(toKafkaResourceName(b.getResource()))
                                        .setPatternType(KAFKA_PT_LITERAL);
                        AccessControlEntry entry = b.getAccessControlEntry();
                        m.setPrincipal(kafkaPrincipal(entry.getPrincipal()));
                        m.setHost(entry.getHost());
                        m.setOperation(toKafkaOp(entry.getOperationType()));
                        m.setPermissionType(toKafkaPerm(entry.getPermissionType()));
                        m.setErrorCode(Errors.NONE.code());
                        results.get(indexOfFilter.get(j)).matchingAcls().add(m);
                    }
                }
            } catch (org.apache.fluss.exception.AuthorizationException denied) {
                for (Integer idx : indexOfFilter) {
                    results.get(idx)
                            .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code())
                            .setErrorMessage(denied.getMessage());
                }
            } catch (Throwable t) {
                for (Integer idx : indexOfFilter) {
                    results.get(idx)
                            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                            .setErrorMessage(t.getMessage());
                }
            }
        }

        response.filterResults().addAll(results);
        return response;
    }

    // ---------- mapping helpers ----------

    /** Kafka ResourceType byte codes from {@code org.apache.kafka.common.resource.ResourceType}. */
    private static final byte KAFKA_RT_ANY = 1;

    private static final byte KAFKA_RT_TOPIC = 2;
    private static final byte KAFKA_RT_GROUP = 3;
    private static final byte KAFKA_RT_CLUSTER = 4;

    /** Kafka PatternType byte codes. */
    private static final byte KAFKA_PT_ANY = 1;

    private static final byte KAFKA_PT_MATCH = 2;
    private static final byte KAFKA_PT_LITERAL = 3;
    private static final byte KAFKA_PT_PREFIXED = 4;

    /** Kafka Operation byte codes. */
    private static final byte KAFKA_OP_ANY = 1;

    private static final byte KAFKA_OP_ALL = 2;
    private static final byte KAFKA_OP_READ = 3;
    private static final byte KAFKA_OP_WRITE = 4;
    private static final byte KAFKA_OP_CREATE = 5;
    private static final byte KAFKA_OP_DELETE = 6;
    private static final byte KAFKA_OP_ALTER = 7;
    private static final byte KAFKA_OP_DESCRIBE = 8;
    private static final byte KAFKA_OP_CLUSTER_ACTION = 9;
    private static final byte KAFKA_OP_DESCRIBE_CONFIGS = 10;
    private static final byte KAFKA_OP_ALTER_CONFIGS = 11;
    private static final byte KAFKA_OP_IDEMPOTENT_WRITE = 12;

    /** Kafka PermissionType byte codes. */
    private static final byte KAFKA_PM_ANY = 1;

    private static final byte KAFKA_PM_DENY = 2;
    private static final byte KAFKA_PM_ALLOW = 3;

    private AclBindingFilter toFlussFilter(DescribeAclsRequestData req)
            throws KafkaAclMappingException {
        return new AclBindingFilter(
                toResourceFilter(
                        req.resourceTypeFilter(),
                        req.resourceNameFilter(),
                        req.patternTypeFilter()),
                new AccessControlEntryFilter(
                        toPrincipalFilter(req.principalFilter()),
                        req.hostFilter(),
                        toFlussOpFilter(req.operation()),
                        toFlussPermFilter(req.permissionType())));
    }

    private AclBindingFilter toFlussFilter(DeleteAclsRequestData.DeleteAclsFilter req)
            throws KafkaAclMappingException {
        return new AclBindingFilter(
                toResourceFilter(
                        req.resourceTypeFilter(),
                        req.resourceNameFilter(),
                        req.patternTypeFilter()),
                new AccessControlEntryFilter(
                        toPrincipalFilter(req.principalFilter()),
                        req.hostFilter(),
                        toFlussOpFilter(req.operation()),
                        toFlussPermFilter(req.permissionType())));
    }

    private AclBinding toFlussBinding(CreateAclsRequestData.AclCreation creation)
            throws KafkaAclMappingException {
        if (creation.resourcePatternType() != KAFKA_PT_LITERAL) {
            throw new KafkaAclMappingException(
                    "Only LITERAL resource pattern is supported; got type "
                            + creation.resourcePatternType());
        }
        Resource resource = toFlussResource(creation.resourceType(), creation.resourceName());
        FlussPrincipal principal = toFlussPrincipal(creation.principal());
        OperationType op = toFlussOp(creation.operation());
        PermissionType perm = toFlussPerm(creation.permissionType());
        String host = creation.host() == null ? "*" : creation.host();
        return new AclBinding(resource, new AccessControlEntry(principal, host, op, perm));
    }

    private ResourceFilter toResourceFilter(byte type, String name, byte pattern)
            throws KafkaAclMappingException {
        if (pattern != KAFKA_PT_ANY && pattern != KAFKA_PT_LITERAL && pattern != KAFKA_PT_MATCH) {
            throw new KafkaAclMappingException(
                    "Only LITERAL / MATCH / ANY pattern types are supported; got " + pattern);
        }
        ResourceType rt = mapKafkaResourceType(type);
        String rn = name == null ? null : toFlussResourceName(rt, type, name);
        return new ResourceFilter(rt, rn);
    }

    private Resource toFlussResource(byte kafkaType, String kafkaName)
            throws KafkaAclMappingException {
        ResourceType rt = mapKafkaResourceType(kafkaType);
        if (rt == ResourceType.ANY) {
            throw new KafkaAclMappingException("Cannot create ACL on ANY resource");
        }
        switch (rt) {
            case TABLE:
                return Resource.table(kafkaDatabase, kafkaName);
            case CLUSTER:
                return Resource.cluster();
            default:
                throw new KafkaAclMappingException("Unsupported resource type for create: " + rt);
        }
    }

    private String toFlussResourceName(ResourceType rt, byte kafkaType, String kafkaName) {
        if (rt == ResourceType.TABLE) {
            return kafkaDatabase + "." + kafkaName;
        }
        return kafkaName;
    }

    private static ResourceType mapKafkaResourceType(byte type) throws KafkaAclMappingException {
        switch (type) {
            case KAFKA_RT_ANY:
                return ResourceType.ANY;
            case KAFKA_RT_TOPIC:
                return ResourceType.TABLE;
            case KAFKA_RT_GROUP:
                return ResourceType.CLUSTER;
            case KAFKA_RT_CLUSTER:
                return ResourceType.CLUSTER;
            default:
                throw new KafkaAclMappingException(
                        "Resource type "
                                + type
                                + " is not supported by this Fluss Kafka bolt-on"
                                + " (TRANSACTIONAL_ID, DELEGATION_TOKEN, USER are deferred).");
        }
    }

    private static byte mapFlussResourceType(ResourceType rt) {
        switch (rt) {
            case ANY:
                return KAFKA_RT_ANY;
            case TABLE:
                return KAFKA_RT_TOPIC;
            case CLUSTER:
                return KAFKA_RT_CLUSTER;
            default:
                return 0; // UNKNOWN
        }
    }

    private DescribeAclsResponseData.DescribeAclsResource toKafkaResource(
            Resource resource, DescribeAclsResponseData.DescribeAclsResource shell) {
        shell.setResourceType(mapFlussResourceType(resource.getType()));
        shell.setResourceName(toKafkaResourceName(resource));
        shell.setPatternType(KAFKA_PT_LITERAL);
        return shell;
    }

    private String toKafkaResourceName(Resource resource) {
        if (resource.getType() == ResourceType.TABLE) {
            // Strip "kafkaDatabase." prefix if present so the Kafka client sees just the topic.
            String prefix = kafkaDatabase + ".";
            String name = resource.getName();
            if (name != null && name.startsWith(prefix)) {
                return name.substring(prefix.length());
            }
            return name;
        }
        return resource.getName();
    }

    private DescribeAclsResponseData.AclDescription toKafkaEntry(AccessControlEntry entry) {
        return new DescribeAclsResponseData.AclDescription()
                .setPrincipal(kafkaPrincipal(entry.getPrincipal()))
                .setHost(entry.getHost())
                .setOperation(toKafkaOp(entry.getOperationType()))
                .setPermissionType(toKafkaPerm(entry.getPermissionType()));
    }

    private static String kafkaPrincipal(FlussPrincipal p) {
        String type = p.getType();
        if (type == null || type.isEmpty()) {
            type = "User";
        }
        // Kafka convention is lead-cap type: "User:alice"
        return capitalise(type) + ":" + p.getName();
    }

    private static String capitalise(String s) {
        if (s == null || s.isEmpty()) {
            return s;
        }
        return Character.toUpperCase(s.charAt(0)) + s.substring(1).toLowerCase(Locale.ROOT);
    }

    private static FlussPrincipal toFlussPrincipal(String kafkaPrincipal)
            throws KafkaAclMappingException {
        if (kafkaPrincipal == null) {
            throw new KafkaAclMappingException("Principal is required");
        }
        int idx = kafkaPrincipal.indexOf(':');
        if (idx < 0) {
            throw new KafkaAclMappingException(
                    "Principal must be 'Type:name'; got '" + kafkaPrincipal + "'");
        }
        String type = kafkaPrincipal.substring(0, idx);
        String name = kafkaPrincipal.substring(idx + 1);
        return new FlussPrincipal(name, type);
    }

    private static FlussPrincipal toPrincipalFilter(String kafkaPrincipal)
            throws KafkaAclMappingException {
        if (kafkaPrincipal == null) {
            return FlussPrincipal.ANY;
        }
        return toFlussPrincipal(kafkaPrincipal);
    }

    private static OperationType toFlussOp(byte kafkaOp) throws KafkaAclMappingException {
        switch (kafkaOp) {
            case KAFKA_OP_ANY:
                return OperationType.ANY;
            case KAFKA_OP_ALL:
                return OperationType.ALL;
            case KAFKA_OP_READ:
                return OperationType.READ;
            case KAFKA_OP_WRITE:
            case KAFKA_OP_IDEMPOTENT_WRITE:
                return OperationType.WRITE;
            case KAFKA_OP_CREATE:
                return OperationType.CREATE;
            case KAFKA_OP_DELETE:
                return OperationType.DROP;
            case KAFKA_OP_ALTER:
            case KAFKA_OP_ALTER_CONFIGS:
                return OperationType.ALTER;
            case KAFKA_OP_DESCRIBE:
            case KAFKA_OP_DESCRIBE_CONFIGS:
                return OperationType.DESCRIBE;
            case KAFKA_OP_CLUSTER_ACTION:
                throw new KafkaAclMappingException(
                        "CLUSTER_ACTION is broker-internal and not exposed as an ACL target");
            default:
                throw new KafkaAclMappingException("Unsupported Kafka operation code " + kafkaOp);
        }
    }

    private static OperationType toFlussOpFilter(byte kafkaOp) throws KafkaAclMappingException {
        return toFlussOp(kafkaOp);
    }

    private static byte toKafkaOp(OperationType op) {
        switch (op) {
            case ANY:
                return KAFKA_OP_ANY;
            case ALL:
                return KAFKA_OP_ALL;
            case READ:
                return KAFKA_OP_READ;
            case WRITE:
                return KAFKA_OP_WRITE;
            case CREATE:
                return KAFKA_OP_CREATE;
            case DROP:
                return KAFKA_OP_DELETE;
            case ALTER:
                return KAFKA_OP_ALTER;
            case DESCRIBE:
                return KAFKA_OP_DESCRIBE;
            default:
                return KAFKA_OP_ANY;
        }
    }

    private static PermissionType toFlussPerm(byte kafkaPerm) throws KafkaAclMappingException {
        switch (kafkaPerm) {
            case KAFKA_PM_ANY:
                return PermissionType.ANY;
            case KAFKA_PM_ALLOW:
                return PermissionType.ALLOW;
            case KAFKA_PM_DENY:
                throw new KafkaAclMappingException(
                        "DENY permissions are not supported by the Fluss authorizer yet");
            default:
                throw new KafkaAclMappingException("Unknown permission type " + kafkaPerm);
        }
    }

    private static PermissionType toFlussPermFilter(byte kafkaPerm)
            throws KafkaAclMappingException {
        return toFlussPerm(kafkaPerm);
    }

    private static byte toKafkaPerm(PermissionType perm) {
        switch (perm) {
            case ANY:
                return KAFKA_PM_ANY;
            case ALLOW:
                return KAFKA_PM_ALLOW;
            default:
                return KAFKA_PM_ANY;
        }
    }

    /** Thrown when a Kafka request contains a shape Fluss's authorizer model can't express. */
    static final class KafkaAclMappingException extends Exception {
        KafkaAclMappingException(String message) {
            super(message);
        }
    }
}
