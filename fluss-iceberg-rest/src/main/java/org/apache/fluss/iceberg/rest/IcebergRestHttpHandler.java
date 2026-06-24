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

package org.apache.fluss.iceberg.rest;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.catalog.CatalogException;
import org.apache.fluss.catalog.CatalogService;
import org.apache.fluss.catalog.entities.NamespaceEntity;
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

/**
 * Iceberg REST Catalog HTTP routes (Phase E preview). Three endpoints:
 *
 * <ul>
 *   <li>{@code GET /v1/config} — returns {@code {"defaults":{},"overrides":{}}} so Iceberg clients
 *       accept the server as a valid REST catalog.
 *   <li>{@code GET /v1/namespaces} — returns every top-level namespace as a single-element string
 *       array, matching the Iceberg namespace representation.
 *   <li>{@code POST /v1/namespaces} — creates a top-level namespace from {@code
 *       {"namespace":["name"],"properties":{}}}; {@code properties} is accepted but ignored.
 * </ul>
 *
 * <p>Every other path returns {@code 501 Not Implemented} with an Iceberg-style {@code
 * ErrorResponse} body. {@link CatalogException} instances are translated to HTTP status via {@link
 * #toHttpStatus(CatalogException.Kind)}.
 */
@Internal
public final class IcebergRestHttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergRestHttpHandler.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final CatalogService catalog;

    public IcebergRestHttpHandler(CatalogService catalog) {
        this.catalog = catalog;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        FullHttpResponse response;
        try {
            response = dispatch(request);
        } catch (CatalogException ce) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Iceberg REST {} {} → {} ({})",
                        request.method(),
                        request.uri(),
                        toHttpStatus(ce.kind()),
                        ce.getMessage());
            }
            response = errorResponse(toHttpStatus(ce.kind()), ce.kind().name(), ce.getMessage());
        } catch (Throwable t) {
            LOG.error("Unexpected error serving Iceberg REST request {}", request.uri(), t);
            response =
                    errorResponse(
                            HttpResponseStatus.INTERNAL_SERVER_ERROR,
                            "InternalServerError",
                            t.getMessage());
        }
        boolean keepAlive = HttpUtil.isKeepAlive(request);
        HttpUtil.setKeepAlive(response, keepAlive);
        HttpUtil.setContentLength(response, response.content().readableBytes());
        ctx.writeAndFlush(response);
    }

    private FullHttpResponse dispatch(FullHttpRequest request) throws Exception {
        String path = new QueryStringDecoder(request.uri()).path();
        HttpMethod method = request.method();

        if (HttpMethod.GET.equals(method) && path.equals("/v1/config")) {
            ObjectNode body = MAPPER.createObjectNode();
            body.set("defaults", MAPPER.createObjectNode());
            body.set("overrides", MAPPER.createObjectNode());
            return jsonResponse(HttpResponseStatus.OK, body);
        }

        if (HttpMethod.GET.equals(method) && path.equals("/v1/namespaces")) {
            List<NamespaceEntity> namespaces = catalog.listNamespaces(null);
            ArrayNode outer = MAPPER.createArrayNode();
            for (NamespaceEntity ns : namespaces) {
                ArrayNode levels = MAPPER.createArrayNode();
                levels.add(ns.name());
                outer.add(levels);
            }
            ObjectNode body = MAPPER.createObjectNode();
            body.set("namespaces", outer);
            return jsonResponse(HttpResponseStatus.OK, body);
        }

        if (HttpMethod.POST.equals(method) && path.equals("/v1/namespaces")) {
            JsonNode body = MAPPER.readTree(readUtf8(request));
            if (!body.hasNonNull("namespace") || !body.get("namespace").isArray()) {
                throw new CatalogException(
                        CatalogException.Kind.INVALID_INPUT,
                        "'namespace' field is required and must be an array of strings");
            }
            JsonNode levels = body.get("namespace");
            if (levels.size() != 1) {
                throw new CatalogException(
                        CatalogException.Kind.UNSUPPORTED,
                        "Only top-level (single-element) namespaces are supported in this preview; "
                                + "got "
                                + levels.size()
                                + " levels");
            }
            String name = levels.get(0).asText();
            if (name == null || name.isEmpty()) {
                throw new CatalogException(
                        CatalogException.Kind.INVALID_INPUT,
                        "namespace element must be a non-empty string");
            }
            // 'properties' is accepted-but-ignored in this preview — we don't yet persist
            // namespace metadata map entries through the catalog service.
            catalog.createNamespace(null, name, null);
            ObjectNode response = MAPPER.createObjectNode();
            ArrayNode echo = MAPPER.createArrayNode();
            echo.add(name);
            response.set("namespace", echo);
            response.set("properties", MAPPER.createObjectNode());
            return jsonResponse(HttpResponseStatus.CREATED, response);
        }

        return errorResponse(
                HttpResponseStatus.NOT_IMPLEMENTED,
                "NotImplemented",
                method + " " + path + " is not implemented in this Iceberg REST Catalog preview");
    }

    private static String readUtf8(FullHttpRequest request) {
        return request.content().toString(StandardCharsets.UTF_8);
    }

    private static FullHttpResponse jsonResponse(HttpResponseStatus status, JsonNode body) {
        byte[] bytes;
        try {
            bytes = MAPPER.writeValueAsBytes(body);
        } catch (Exception e) {
            return errorResponse(
                    HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    "InternalServerError",
                    e.getMessage());
        }
        ByteBuf buf = Unpooled.wrappedBuffer(bytes);
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, buf);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
        return response;
    }

    private static FullHttpResponse errorResponse(
            HttpResponseStatus status, String type, String message) {
        // Iceberg REST ErrorResponse: { "error": { "message": ..., "type": ..., "code": ... } }
        ObjectNode root = MAPPER.createObjectNode();
        ObjectNode error = MAPPER.createObjectNode();
        error.put("message", message == null ? "" : message);
        error.put("type", type);
        error.put("code", status.code());
        root.set("error", error);
        byte[] bytes;
        try {
            bytes = MAPPER.writeValueAsBytes(root);
        } catch (Exception e) {
            bytes =
                    ("{\"error\":{\"code\":" + status.code() + "}}")
                            .getBytes(StandardCharsets.UTF_8);
        }
        FullHttpResponse response =
                new DefaultFullHttpResponse(
                        HttpVersion.HTTP_1_1, status, Unpooled.wrappedBuffer(bytes));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
        return response;
    }

    static HttpResponseStatus toHttpStatus(CatalogException.Kind kind) {
        switch (kind) {
            case INVALID_INPUT:
                return HttpResponseStatus.BAD_REQUEST;
            case UNSUPPORTED:
                return HttpResponseStatus.BAD_REQUEST;
            case NOT_FOUND:
                return HttpResponseStatus.NOT_FOUND;
            case ALREADY_EXISTS:
                return HttpResponseStatus.CONFLICT;
            case CONFLICT:
                return HttpResponseStatus.CONFLICT;
            case INTERNAL:
            default:
                return HttpResponseStatus.INTERNAL_SERVER_ERROR;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.warn("Unhandled Iceberg REST channel exception", cause);
        ctx.close();
    }
}
