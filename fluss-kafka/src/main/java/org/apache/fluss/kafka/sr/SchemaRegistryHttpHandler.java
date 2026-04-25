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

package org.apache.fluss.kafka.sr;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.sr.auth.HttpPrincipalExtractor;
import org.apache.fluss.kafka.sr.compat.CompatibilityResult;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.FullHttpRequest;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.HttpHeaderValues;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.HttpUtil;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.HttpVersion;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.QueryStringDecoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Confluent REST endpoints for Schema Registry Phase A1. Five routes:
 *
 * <ul>
 *   <li>{@code GET /} — compatibility ping.
 *   <li>{@code GET /subjects} — JSON array of subject names.
 *   <li>{@code GET /subjects/{s}/versions/latest} — latest schema for a subject.
 *   <li>{@code POST /subjects/{s}/versions} — register a new Avro schema, idempotent on exact
 *       repeat.
 *   <li>{@code GET /schemas/ids/{id}} — schema lookup by Confluent global id.
 * </ul>
 */
@Internal
public final class SchemaRegistryHttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryHttpHandler.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final SchemaRegistryService service;
    private final HttpPrincipalExtractor principalExtractor;

    public SchemaRegistryHttpHandler(
            SchemaRegistryService service, HttpPrincipalExtractor principalExtractor) {
        this.service = service;
        this.principalExtractor = principalExtractor;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        FullHttpResponse response;
        FlussPrincipal principal = resolvePrincipal(ctx, request);
        if (principal != null) {
            SchemaRegistryCallContext.set(principal);
        }
        try {
            try {
                response = dispatch(request);
            } catch (SchemaRegistryException sre) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "SR {} {} → {} ({})",
                            request.method(),
                            request.uri(),
                            toHttpStatus(sre.kind()),
                            sre.getMessage());
                }
                response = errorResponse(toHttpStatus(sre.kind()), sre.getMessage());
            } catch (Throwable t) {
                LOG.error("Unexpected error serving SR request {}", request.uri(), t);
                response = errorResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, t.getMessage());
            }
            boolean keepAlive = HttpUtil.isKeepAlive(request);
            HttpUtil.setKeepAlive(response, keepAlive);
            HttpUtil.setContentLength(response, response.content().readableBytes());
            ctx.writeAndFlush(response);
        } finally {
            SchemaRegistryCallContext.clear();
        }
    }

    private FlussPrincipal resolvePrincipal(ChannelHandlerContext ctx, FullHttpRequest request) {
        if (principalExtractor == null) {
            return null;
        }
        SocketAddress remote = ctx.channel().remoteAddress();
        InetSocketAddress inet =
                remote instanceof InetSocketAddress ? (InetSocketAddress) remote : null;
        try {
            return principalExtractor.extract(request, inet).orElse(null);
        } catch (RuntimeException e) {
            // A broken extractor must not take the SR down — log and fall back to anonymous.
            LOG.warn("Principal extraction threw; treating request as anonymous", e);
            return null;
        }
    }

    private FullHttpResponse dispatch(FullHttpRequest request) throws Exception {
        QueryStringDecoder decoder = new QueryStringDecoder(request.uri());
        String path = decoder.path();
        HttpMethod method = request.method();

        if (HttpMethod.GET.equals(method) && path.equals("/")) {
            ObjectNode body = MAPPER.createObjectNode();
            body.put("compatibility", service.defaultCompatibility());
            return jsonResponse(HttpResponseStatus.OK, body);
        }
        if (HttpMethod.GET.equals(method) && path.equals("/subjects")) {
            List<String> subjects = service.listSubjects();
            ArrayNode arr = MAPPER.createArrayNode();
            for (String s : subjects) {
                arr.add(s);
            }
            return jsonResponse(HttpResponseStatus.OK, arr);
        }
        if (HttpMethod.GET.equals(method) && path.matches("/schemas/ids/[0-9]+/subjects")) {
            int id =
                    parseIntOr400(
                            path.substring(
                                    "/schemas/ids/".length(), path.length() - "/subjects".length()),
                            "schema id");
            List<String> subjects = service.subjectsForId(id);
            ArrayNode arr = MAPPER.createArrayNode();
            for (String s : subjects) {
                arr.add(s);
            }
            return jsonResponse(HttpResponseStatus.OK, arr);
        }
        if (HttpMethod.GET.equals(method) && path.matches("/schemas/ids/[0-9]+/referencedby")) {
            int id =
                    parseIntOr400(
                            path.substring(
                                    "/schemas/ids/".length(),
                                    path.length() - "/referencedby".length()),
                            "schema id");
            List<SchemaRegistryService.SubjectVersion> referrers = service.referencedBy(id);
            ArrayNode arr = MAPPER.createArrayNode();
            for (SchemaRegistryService.SubjectVersion sv : referrers) {
                ObjectNode node = MAPPER.createObjectNode();
                node.put("subject", sv.subject());
                node.put("version", sv.version());
                arr.add(node);
            }
            return jsonResponse(HttpResponseStatus.OK, arr);
        }
        if (HttpMethod.GET.equals(method) && path.matches("/schemas/ids/[0-9]+/versions")) {
            int id =
                    parseIntOr400(
                            path.substring(
                                    "/schemas/ids/".length(), path.length() - "/versions".length()),
                            "schema id");
            List<SchemaRegistryService.SubjectVersion> svs = service.subjectVersionsForId(id);
            ArrayNode arr = MAPPER.createArrayNode();
            for (SchemaRegistryService.SubjectVersion sv : svs) {
                ObjectNode node = MAPPER.createObjectNode();
                node.put("subject", sv.subject());
                node.put("version", sv.version());
                arr.add(node);
            }
            return jsonResponse(HttpResponseStatus.OK, arr);
        }
        if (HttpMethod.GET.equals(method) && path.startsWith("/schemas/ids/")) {
            String idPart = path.substring("/schemas/ids/".length());
            int id = parseIntOr400(idPart, "schema id");
            Optional<SchemaRegistryService.RegisteredSchema> found = service.schemaById(id);
            if (!found.isPresent()) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.NOT_FOUND, "schema id " + id + " not found");
            }
            ObjectNode body = MAPPER.createObjectNode();
            body.put("schema", found.get().schema());
            body.put("schemaType", found.get().format());
            putReferences(body, found.get().references());
            return jsonResponse(HttpResponseStatus.OK, body);
        }
        if (HttpMethod.GET.equals(method)
                && path.startsWith("/subjects/")
                && path.endsWith("/versions/latest")) {
            String subject =
                    path.substring(
                            "/subjects/".length(), path.length() - "/versions/latest".length());
            Optional<SchemaRegistryService.RegisteredSchema> found =
                    service.latestForSubject(subject);
            if (!found.isPresent()) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.NOT_FOUND,
                        "subject " + subject + " not found");
            }
            ObjectNode body = MAPPER.createObjectNode();
            body.put("id", found.get().id());
            body.put("subject", found.get().subject());
            body.put("version", found.get().version());
            body.put("schemaType", found.get().format());
            body.put("schema", found.get().schema());
            putReferences(body, found.get().references());
            return jsonResponse(HttpResponseStatus.OK, body);
        }
        if (HttpMethod.POST.equals(method)
                && path.startsWith("/subjects/")
                && path.endsWith("/versions")) {
            String subject =
                    path.substring("/subjects/".length(), path.length() - "/versions".length());
            JsonNode body = MAPPER.readTree(readUtf8(request));
            String schemaType =
                    body.hasNonNull("schemaType") ? body.get("schemaType").asText() : "AVRO";
            if (!body.hasNonNull("schema")) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.INVALID_INPUT,
                        "'schema' field is required in POST body");
            }
            List<SchemaRegistryService.RequestedReference> refs = parseReferences(body);
            int id = service.register(subject, body.get("schema").asText(), schemaType, refs);
            ObjectNode response = MAPPER.createObjectNode();
            response.put("id", id);
            return jsonResponse(HttpResponseStatus.OK, response);
        }
        if (HttpMethod.GET.equals(method) && path.equals("/schemas/types")) {
            ArrayNode arr = MAPPER.createArrayNode();
            for (String t : service.supportedTypes()) {
                arr.add(t);
            }
            return jsonResponse(HttpResponseStatus.OK, arr);
        }
        if (HttpMethod.GET.equals(method)
                && path.startsWith("/subjects/")
                && path.endsWith("/versions")) {
            String subject =
                    path.substring("/subjects/".length(), path.length() - "/versions".length());
            List<Integer> versions = service.listVersions(subject);
            if (versions.isEmpty()) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.NOT_FOUND,
                        "subject " + subject + " not found");
            }
            ArrayNode arr = MAPPER.createArrayNode();
            for (Integer v : versions) {
                arr.add(v);
            }
            return jsonResponse(HttpResponseStatus.OK, arr);
        }
        if (HttpMethod.GET.equals(method)
                && path.startsWith("/subjects/")
                && path.contains("/versions/")
                && !path.endsWith("/versions/latest")) {
            // GET /subjects/{s}/versions/{v}
            int versionsIdx = path.indexOf("/versions/");
            String subject = path.substring("/subjects/".length(), versionsIdx);
            String versionPart = path.substring(versionsIdx + "/versions/".length());
            int version;
            if ("latest".equalsIgnoreCase(versionPart)) {
                version = -1;
            } else {
                version = parseIntOr400(versionPart, "version");
            }
            Optional<SchemaRegistryService.RegisteredSchema> found =
                    service.versionForSubject(subject, version);
            if (!found.isPresent()) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.NOT_FOUND,
                        "subject " + subject + " version " + versionPart + " not found");
            }
            ObjectNode body = MAPPER.createObjectNode();
            body.put("id", found.get().id());
            body.put("subject", found.get().subject());
            body.put("version", found.get().version());
            body.put("schemaType", found.get().format());
            body.put("schema", found.get().schema());
            putReferences(body, found.get().references());
            return jsonResponse(HttpResponseStatus.OK, body);
        }
        // DELETE /subjects/{s}/versions/{v} — soft by default, ?permanent=true hard-deletes.
        if (HttpMethod.DELETE.equals(method)
                && path.startsWith("/subjects/")
                && path.contains("/versions/")) {
            int versionsIdx = path.indexOf("/versions/");
            String subject = path.substring("/subjects/".length(), versionsIdx);
            String versionPart = path.substring(versionsIdx + "/versions/".length());
            int version = parseIntOr400(versionPart, "version");
            boolean permanent = isPermanent(decoder);
            int deleted = service.deleteVersion(subject, version, permanent);
            ByteBuf buf =
                    Unpooled.wrappedBuffer(
                            Integer.toString(deleted).getBytes(StandardCharsets.UTF_8));
            FullHttpResponse resp =
                    new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, buf);
            resp.headers()
                    .set(HttpHeaderNames.CONTENT_TYPE, "application/vnd.schemaregistry.v1+json");
            return resp;
        }
        // DELETE /subjects/{s} — soft by default, ?permanent=true cascades into version rows.
        if (HttpMethod.DELETE.equals(method)
                && path.startsWith("/subjects/")
                && !path.contains("/versions")) {
            String subject = path.substring("/subjects/".length());
            boolean permanent = isPermanent(decoder);
            List<Integer> deleted = service.deleteSubject(subject, permanent);
            ArrayNode arr = MAPPER.createArrayNode();
            for (Integer v : deleted) {
                arr.add(v);
            }
            return jsonResponse(HttpResponseStatus.OK, arr);
        }
        // POST /compatibility/subjects/{s}/versions/{v} — run compat check without registering.
        if (HttpMethod.POST.equals(method)
                && path.startsWith("/compatibility/subjects/")
                && path.contains("/versions/")) {
            String afterPrefix = path.substring("/compatibility/subjects/".length());
            int versionsIdx = afterPrefix.indexOf("/versions/");
            if (versionsIdx < 0) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.INVALID_INPUT,
                        "compatibility path must be /compatibility/subjects/{s}/versions/{v}");
            }
            String subject = afterPrefix.substring(0, versionsIdx);
            String versionPart = afterPrefix.substring(versionsIdx + "/versions/".length());
            int version;
            if ("latest".equalsIgnoreCase(versionPart)) {
                version = -1;
            } else {
                version = parseIntOr400(versionPart, "version");
            }
            JsonNode body = MAPPER.readTree(readUtf8(request));
            if (!body.hasNonNull("schema")) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.INVALID_INPUT,
                        "'schema' field is required in POST body");
            }
            // schemaType is recorded but not separately validated here — checkCompatibility
            // already dispatches via FormatRegistry based on the subject's stored format (the
            // proposed schema must be same-format as the subject's prior versions; that's a
            // semantic choice, not a wire-contract break).
            List<SchemaRegistryService.RequestedReference> compatRefs = parseReferences(body);
            CompatibilityResult result =
                    service.checkCompatibility(
                            subject, version, body.get("schema").asText(), compatRefs);
            ObjectNode response = MAPPER.createObjectNode();
            response.put("is_compatible", result.isCompatible());
            if (!result.messages().isEmpty()) {
                ArrayNode messages = response.putArray("messages");
                for (String m : result.messages()) {
                    messages.add(m);
                }
            }
            return jsonResponse(HttpResponseStatus.OK, response);
        }
        if (HttpMethod.POST.equals(method)
                && path.startsWith("/subjects/")
                && !path.contains("/versions")) {
            // POST /subjects/{s} — schema-exists probe (never mints a new id).
            String subject = path.substring("/subjects/".length());
            JsonNode body = MAPPER.readTree(readUtf8(request));
            if (!body.hasNonNull("schema")) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.INVALID_INPUT,
                        "'schema' field is required in POST body");
            }
            // schemaExists keys on (subject, schemaText); schemaType is echoed from the
            // matched row's stored format.
            Optional<SchemaRegistryService.RegisteredSchema> found =
                    service.schemaExists(subject, body.get("schema").asText());
            if (!found.isPresent()) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.NOT_FOUND,
                        "Schema not found under subject " + subject);
            }
            ObjectNode response = MAPPER.createObjectNode();
            response.put("id", found.get().id());
            response.put("subject", found.get().subject());
            response.put("version", found.get().version());
            response.put("schemaType", found.get().format());
            response.put("schema", found.get().schema());
            putReferences(response, found.get().references());
            return jsonResponse(HttpResponseStatus.OK, response);
        }
        // --- /config endpoints ---
        if (HttpMethod.GET.equals(method) && path.equals("/config")) {
            ObjectNode body = MAPPER.createObjectNode();
            body.put("compatibilityLevel", service.defaultCompatibility());
            return jsonResponse(HttpResponseStatus.OK, body);
        }
        if (HttpMethod.PUT.equals(method) && path.equals("/config")) {
            JsonNode body = MAPPER.readTree(readUtf8(request));
            String level =
                    body.hasNonNull("compatibility") ? body.get("compatibility").asText() : null;
            if (level == null) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.INVALID_INPUT,
                        "'compatibility' is required in PUT /config body");
            }
            service.setGlobalCompatibility(level);
            ObjectNode response = MAPPER.createObjectNode();
            response.put("compatibility", level.toUpperCase(java.util.Locale.ROOT));
            return jsonResponse(HttpResponseStatus.OK, response);
        }
        if (HttpMethod.GET.equals(method) && path.startsWith("/config/")) {
            String subject = path.substring("/config/".length());
            Optional<String> level = service.subjectCompatibility(subject);
            if (!level.isPresent()) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.NOT_FOUND,
                        "Subject '" + subject + "' has no compatibility override");
            }
            ObjectNode body = MAPPER.createObjectNode();
            body.put("compatibilityLevel", level.get());
            return jsonResponse(HttpResponseStatus.OK, body);
        }
        if (HttpMethod.PUT.equals(method) && path.startsWith("/config/")) {
            String subject = path.substring("/config/".length());
            JsonNode body = MAPPER.readTree(readUtf8(request));
            String level =
                    body.hasNonNull("compatibility") ? body.get("compatibility").asText() : null;
            if (level == null) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.INVALID_INPUT,
                        "'compatibility' is required in PUT /config/{subject} body");
            }
            service.setSubjectCompatibility(subject, level);
            ObjectNode response = MAPPER.createObjectNode();
            response.put("compatibility", level.toUpperCase(java.util.Locale.ROOT));
            return jsonResponse(HttpResponseStatus.OK, response);
        }
        if (HttpMethod.DELETE.equals(method) && path.startsWith("/config/")) {
            String subject = path.substring("/config/".length());
            service.deleteSubjectCompatibility(subject);
            ObjectNode response = MAPPER.createObjectNode();
            response.put("compatibility", service.defaultCompatibility());
            return jsonResponse(HttpResponseStatus.OK, response);
        }

        // --- /mode endpoints ---
        if (HttpMethod.GET.equals(method) && path.equals("/mode")) {
            ObjectNode body = MAPPER.createObjectNode();
            body.put("mode", service.globalMode());
            return jsonResponse(HttpResponseStatus.OK, body);
        }
        if (HttpMethod.PUT.equals(method) && path.equals("/mode")) {
            JsonNode body = MAPPER.readTree(readUtf8(request));
            String mode = body.hasNonNull("mode") ? body.get("mode").asText() : null;
            if (mode == null) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.INVALID_INPUT,
                        "'mode' is required in PUT /mode body");
            }
            service.setGlobalMode(mode);
            ObjectNode response = MAPPER.createObjectNode();
            response.put("mode", mode.toUpperCase(java.util.Locale.ROOT));
            return jsonResponse(HttpResponseStatus.OK, response);
        }
        if (HttpMethod.GET.equals(method) && path.startsWith("/mode/")) {
            String subject = path.substring("/mode/".length());
            Optional<String> mode = service.subjectMode(subject);
            if (!mode.isPresent()) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.NOT_FOUND,
                        "Subject '" + subject + "' has no mode override");
            }
            ObjectNode body = MAPPER.createObjectNode();
            body.put("mode", mode.get());
            return jsonResponse(HttpResponseStatus.OK, body);
        }
        if (HttpMethod.PUT.equals(method) && path.startsWith("/mode/")) {
            String subject = path.substring("/mode/".length());
            JsonNode body = MAPPER.readTree(readUtf8(request));
            String mode = body.hasNonNull("mode") ? body.get("mode").asText() : null;
            if (mode == null) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.INVALID_INPUT,
                        "'mode' is required in PUT /mode/{subject} body");
            }
            service.setSubjectMode(subject, mode);
            ObjectNode response = MAPPER.createObjectNode();
            response.put("mode", mode.toUpperCase(java.util.Locale.ROOT));
            return jsonResponse(HttpResponseStatus.OK, response);
        }
        if (HttpMethod.DELETE.equals(method) && path.startsWith("/mode/")) {
            String subject = path.substring("/mode/".length());
            service.deleteSubjectMode(subject);
            ObjectNode response = MAPPER.createObjectNode();
            response.put("mode", service.globalMode());
            return jsonResponse(HttpResponseStatus.OK, response);
        }

        throw new SchemaRegistryException(
                SchemaRegistryException.Kind.NOT_FOUND,
                method + " " + path + " is not a Schema Registry endpoint");
    }

    /**
     * Parse the optional {@code references} array on a register / compatibility POST body. Returns
     * an empty list when the field is missing, null, or an empty array. Tolerates the SR 7.4+
     * {@code metadata} sub-object on each entry by ignoring it (forward-compat).
     */
    private static List<SchemaRegistryService.RequestedReference> parseReferences(JsonNode body) {
        JsonNode refsNode = body.get("references");
        if (refsNode == null || refsNode.isNull()) {
            return Collections.emptyList();
        }
        if (!refsNode.isArray()) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.INVALID_INPUT, "'references' must be an array");
        }
        List<SchemaRegistryService.RequestedReference> out = new ArrayList<>(refsNode.size());
        for (JsonNode entry : refsNode) {
            if (!entry.isObject()) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.INVALID_INPUT,
                        "every 'references' entry must be an object");
            }
            String name = entry.hasNonNull("name") ? entry.get("name").asText() : null;
            String subject = entry.hasNonNull("subject") ? entry.get("subject").asText() : null;
            JsonNode versionNode = entry.get("version");
            if (name == null || name.isEmpty()) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.INVALID_INPUT,
                        "every 'references' entry requires 'name'");
            }
            if (subject == null || subject.isEmpty()) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.INVALID_INPUT,
                        "every 'references' entry requires 'subject'");
            }
            if (versionNode == null || !versionNode.canConvertToInt()) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.INVALID_INPUT,
                        "every 'references' entry requires an integer 'version'");
            }
            out.add(
                    new SchemaRegistryService.RequestedReference(
                            name, subject, versionNode.asInt()));
        }
        return out;
    }

    /**
     * Echo the references list onto a response body in Confluent shape. Skips emission when the
     * list is empty so we stay byte-equivalent with the pre-Phase-SR-X.5 body for refless schemas.
     */
    private static void putReferences(
            ObjectNode body, List<SchemaRegistryService.SubjectReference> refs) {
        if (refs == null || refs.isEmpty()) {
            return;
        }
        ArrayNode arr = body.putArray("references");
        for (SchemaRegistryService.SubjectReference r : refs) {
            ObjectNode node = arr.addObject();
            node.put("name", r.name());
            node.put("subject", r.subject());
            node.put("version", r.version());
        }
    }

    /**
     * Inspect {@code ?permanent=true|false} on the decoded query string. Absent / any non-{@code
     * true} value → {@code false} (Confluent default: soft delete).
     */
    private static boolean isPermanent(QueryStringDecoder decoder) {
        List<String> values = decoder.parameters().get("permanent");
        if (values == null || values.isEmpty()) {
            return false;
        }
        return Boolean.parseBoolean(values.get(0));
    }

    private static int parseIntOr400(String raw, String fieldName) {
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException nfe) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.INVALID_INPUT,
                    fieldName + " must be an integer (got '" + raw + "')");
        }
    }

    private static String readUtf8(FullHttpRequest request) {
        return request.content().toString(StandardCharsets.UTF_8);
    }

    private static FullHttpResponse jsonResponse(HttpResponseStatus status, JsonNode body) {
        byte[] bytes;
        try {
            bytes = MAPPER.writeValueAsBytes(body);
        } catch (Exception e) {
            return errorResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
        }
        ByteBuf buf = Unpooled.wrappedBuffer(bytes);
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, buf);
        response.headers()
                .set(HttpHeaderNames.CONTENT_TYPE, "application/vnd.schemaregistry.v1+json");
        return response;
    }

    private static FullHttpResponse errorResponse(HttpResponseStatus status, String message) {
        ObjectNode body = MAPPER.createObjectNode();
        body.put("error_code", status.code());
        body.put("message", message == null ? "" : message);
        byte[] bytes;
        try {
            bytes = MAPPER.writeValueAsBytes(body);
        } catch (Exception e) {
            bytes = ("{\"error_code\":" + status.code() + "}").getBytes(StandardCharsets.UTF_8);
        }
        FullHttpResponse response =
                new DefaultFullHttpResponse(
                        HttpVersion.HTTP_1_1, status, Unpooled.wrappedBuffer(bytes));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
        return response;
    }

    static HttpResponseStatus toHttpStatus(SchemaRegistryException.Kind kind) {
        switch (kind) {
            case INVALID_INPUT:
                return HttpResponseStatus.BAD_REQUEST;
            case UNSUPPORTED:
                return HttpResponseStatus.BAD_REQUEST;
            case NOT_FOUND:
                return HttpResponseStatus.NOT_FOUND;
            case CONFLICT:
                return HttpResponseStatus.CONFLICT;
            case UNPROCESSABLE_ENTITY:
                return HttpResponseStatus.UNPROCESSABLE_ENTITY;
            case FORBIDDEN:
                return HttpResponseStatus.FORBIDDEN;
            case INTERNAL:
            default:
                return HttpResponseStatus.INTERNAL_SERVER_ERROR;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.warn("Unhandled SR channel exception", cause);
        ctx.close();
    }
}
