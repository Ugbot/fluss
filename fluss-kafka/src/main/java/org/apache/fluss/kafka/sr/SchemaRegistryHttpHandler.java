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

import java.nio.charset.StandardCharsets;
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

    public SchemaRegistryHttpHandler(SchemaRegistryService service) {
        this.service = service;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        FullHttpResponse response;
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
    }

    private FullHttpResponse dispatch(FullHttpRequest request) throws Exception {
        String path = new QueryStringDecoder(request.uri()).path();
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
            if (!"AVRO".equalsIgnoreCase(schemaType)) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.UNSUPPORTED,
                        "schemaType '" + schemaType + "' is not supported in Phase A1 (only AVRO)");
            }
            if (!body.hasNonNull("schema")) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.INVALID_INPUT,
                        "'schema' field is required in POST body");
            }
            int id = service.register(subject, body.get("schema").asText());
            ObjectNode response = MAPPER.createObjectNode();
            response.put("id", id);
            return jsonResponse(HttpResponseStatus.OK, response);
        }
        throw new SchemaRegistryException(
                SchemaRegistryException.Kind.NOT_FOUND,
                method + " " + path + " is not a Schema Registry endpoint");
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
