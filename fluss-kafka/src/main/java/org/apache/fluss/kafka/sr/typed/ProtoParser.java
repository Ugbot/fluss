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

package org.apache.fluss.kafka.sr.typed;

import org.apache.fluss.annotation.Internal;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Hand-rolled recursive-descent parser for the {@code .proto} text format subset Confluent's {@code
 * kafka-protobuf-serializer} emits. This avoids pulling Wire + Kotlin transitive dependencies for
 * what is a bounded, closed grammar.
 *
 * <p>Supported:
 *
 * <ul>
 *   <li>{@code syntax = "proto2" | "proto3"}
 *   <li>{@code package foo.bar;}
 *   <li>{@code import "google/protobuf/timestamp.proto";} (tracked, not resolved)
 *   <li>{@code message Foo { fields ... }} including nested messages and enums
 *   <li>Fields: {@code [optional|required|repeated] <type> <name> = <tag>;}
 *   <li>Types: bool, int32/int64/uint32/uint64/sint32/sint64/fixed32/fixed64/sfixed32/sfixed64,
 *       float, double, string, bytes, or a user-defined name.
 *   <li>{@code map<K, V> name = tag;}
 *   <li>{@code enum Foo { BAR = 0; ... }}
 *   <li>{@code oneof X { ... }} — parsed and marked; translator rejects them.
 *   <li>{@code //} and {@code /* ... *}{@code /} comments.
 *   <li>Field-level options {@code [...]} parsed shallowly and discarded.
 * </ul>
 *
 * <p>Not supported (rejected at parse or translate time):
 *
 * <ul>
 *   <li>{@code extend} statements, {@code reserved} ranges, custom options, services.
 *   <li>Groups (deprecated proto2 feature).
 * </ul>
 */
@Internal
public final class ProtoParser {

    private final String src;
    private int pos;

    public ProtoParser(String src) {
        this.src = src;
        this.pos = 0;
    }

    /** Parse an entire {@code .proto} file. */
    public ProtoFile parseFile() {
        ProtoFile file = new ProtoFile();
        skipWs();
        while (pos < src.length()) {
            String token = peekIdent();
            if (token == null) {
                break;
            }
            switch (token) {
                case "syntax":
                    file.syntax = parseSyntax();
                    break;
                case "package":
                    expectKeyword("package");
                    file.pkg = parseIdentPath();
                    expect(';');
                    break;
                case "import":
                    expectKeyword("import");
                    // Skip `public` or `weak` keyword.
                    skipWs();
                    String maybeMod = peekIdent();
                    if ("public".equals(maybeMod) || "weak".equals(maybeMod)) {
                        expectKeyword(maybeMod);
                    }
                    file.imports.add(parseQuotedString());
                    expect(';');
                    break;
                case "option":
                    expectKeyword("option");
                    // Skip options (we don't use them).
                    skipUntil(';');
                    expect(';');
                    break;
                case "message":
                    file.messages.add(parseMessage());
                    break;
                case "enum":
                    file.enums.add(parseEnum());
                    break;
                case "service":
                case "extend":
                    throw new SchemaTranslationException(
                            "Top-level '" + token + "' is not supported in SR translator.");
                default:
                    throw new SchemaTranslationException(
                            "Unexpected token '" + token + "' at position " + pos);
            }
            skipWs();
        }
        if (file.syntax == null) {
            // Proto2 is the default per spec; Confluent emits proto3 with explicit syntax line,
            // so this shouldn't happen in practice.
            file.syntax = "proto2";
        }
        return file;
    }

    private String parseSyntax() {
        expectKeyword("syntax");
        expect('=');
        String s = parseQuotedString();
        expect(';');
        if (!"proto2".equals(s) && !"proto3".equals(s)) {
            throw new SchemaTranslationException("Unsupported proto syntax: " + s);
        }
        return s;
    }

    private ProtoMessage parseMessage() {
        expectKeyword("message");
        ProtoMessage m = new ProtoMessage();
        m.name = parseIdent();
        expect('{');
        while (true) {
            skipWs();
            if (peek() == '}') {
                pos++;
                return m;
            }
            if (peek() == ';') {
                pos++;
                continue;
            }
            String token = peekIdent();
            if (token == null) {
                throw new SchemaTranslationException(
                        "Unexpected character inside message at position " + pos);
            }
            switch (token) {
                case "message":
                    m.nestedMessages.add(parseMessage());
                    break;
                case "enum":
                    m.nestedEnums.add(parseEnum());
                    break;
                case "oneof":
                    m.oneofs.add(parseOneof());
                    break;
                case "reserved":
                    expectKeyword("reserved");
                    skipUntil(';');
                    expect(';');
                    break;
                case "option":
                    expectKeyword("option");
                    skipUntil(';');
                    expect(';');
                    break;
                case "extensions":
                case "extend":
                    throw new SchemaTranslationException(
                            "'" + token + "' inside a message is not supported.");
                default:
                    m.fields.add(parseField());
            }
        }
    }

    private ProtoOneof parseOneof() {
        expectKeyword("oneof");
        ProtoOneof o = new ProtoOneof();
        o.name = parseIdent();
        expect('{');
        while (true) {
            skipWs();
            if (peek() == '}') {
                pos++;
                return o;
            }
            if (peek() == ';') {
                pos++;
                continue;
            }
            // oneof members are just plain fields (without repeated/optional keyword).
            ProtoField f = parseField();
            f.oneofName = o.name;
            o.fields.add(f);
        }
    }

    private ProtoEnum parseEnum() {
        expectKeyword("enum");
        ProtoEnum e = new ProtoEnum();
        e.name = parseIdent();
        expect('{');
        while (true) {
            skipWs();
            if (peek() == '}') {
                pos++;
                return e;
            }
            if (peek() == ';') {
                pos++;
                continue;
            }
            String token = peekIdent();
            if (token == null) {
                throw new SchemaTranslationException(
                        "Unexpected character inside enum at position " + pos);
            }
            if ("option".equals(token) || "reserved".equals(token)) {
                expectKeyword(token);
                skipUntil(';');
                expect(';');
                continue;
            }
            // Plain enum value: IDENT = INT;
            String symbol = parseIdent();
            expect('=');
            int value = parseIntLiteral();
            // Optional [...] options.
            skipWs();
            if (peek() == '[') {
                skipBalanced('[', ']');
            }
            expect(';');
            e.values.put(symbol, value);
        }
    }

    private ProtoField parseField() {
        ProtoField f = new ProtoField();
        String token = peekIdent();
        if (token == null) {
            throw new SchemaTranslationException("Expected field declaration at position " + pos);
        }
        switch (token) {
            case "optional":
            case "required":
            case "repeated":
                expectKeyword(token);
                f.modifier = token;
                break;
            case "map":
                expectKeyword("map");
                expect('<');
                f.mapKeyType = parseTypeName();
                expect(',');
                f.mapValueType = parseTypeName();
                expect('>');
                f.type = "map";
                f.name = parseIdent();
                expect('=');
                f.tag = parseIntLiteral();
                skipWs();
                if (peek() == '[') {
                    skipBalanced('[', ']');
                }
                expect(';');
                return f;
            default:
                // proto3 fields can be declared without modifier.
                f.modifier = null;
        }
        f.type = parseTypeName();
        f.name = parseIdent();
        expect('=');
        f.tag = parseIntLiteral();
        skipWs();
        if (peek() == '[') {
            skipBalanced('[', ']');
        }
        expect(';');
        return f;
    }

    private String parseTypeName() {
        skipWs();
        StringBuilder sb = new StringBuilder();
        if (peek() == '.') {
            sb.append((char) src.charAt(pos++));
        }
        sb.append(parseIdent());
        while (pos < src.length() && src.charAt(pos) == '.') {
            sb.append('.');
            pos++;
            sb.append(parseIdent());
        }
        return sb.toString();
    }

    private String parseIdentPath() {
        return parseTypeName();
    }

    private String parseIdent() {
        skipWs();
        int start = pos;
        if (pos >= src.length()) {
            throw new SchemaTranslationException("Unexpected end of input, expected identifier");
        }
        char c = src.charAt(pos);
        if (!(Character.isLetter(c) || c == '_')) {
            throw new SchemaTranslationException(
                    "Expected identifier at position " + pos + " (got '" + c + "')");
        }
        pos++;
        while (pos < src.length()) {
            c = src.charAt(pos);
            if (Character.isLetterOrDigit(c) || c == '_') {
                pos++;
            } else {
                break;
            }
        }
        return src.substring(start, pos);
    }

    private String peekIdent() {
        skipWs();
        int saved = pos;
        try {
            if (pos >= src.length()) {
                return null;
            }
            char c = src.charAt(pos);
            if (!(Character.isLetter(c) || c == '_')) {
                return null;
            }
            String ident = parseIdent();
            pos = saved;
            return ident;
        } finally {
            pos = saved;
        }
    }

    private void expectKeyword(String kw) {
        String got = parseIdent();
        if (!kw.equals(got)) {
            throw new SchemaTranslationException(
                    "Expected keyword '" + kw + "' but got '" + got + "' at position " + pos);
        }
    }

    private void expect(char c) {
        skipWs();
        if (pos >= src.length() || src.charAt(pos) != c) {
            throw new SchemaTranslationException(
                    "Expected '"
                            + c
                            + "' at position "
                            + pos
                            + " (got '"
                            + (pos >= src.length() ? "<eof>" : String.valueOf(src.charAt(pos)))
                            + "')");
        }
        pos++;
    }

    private int peek() {
        if (pos >= src.length()) {
            return -1;
        }
        return src.charAt(pos);
    }

    private int parseIntLiteral() {
        skipWs();
        int start = pos;
        if (pos < src.length() && (src.charAt(pos) == '-' || src.charAt(pos) == '+')) {
            pos++;
        }
        // Accept hex (0x...) and decimal.
        if (pos + 1 < src.length()
                && src.charAt(pos) == '0'
                && (src.charAt(pos + 1) == 'x' || src.charAt(pos + 1) == 'X')) {
            pos += 2;
            while (pos < src.length() && isHex(src.charAt(pos))) {
                pos++;
            }
            return Integer.parseInt(src.substring(start + 2, pos), 16)
                    * (src.charAt(start) == '-' ? -1 : 1);
        }
        while (pos < src.length() && Character.isDigit(src.charAt(pos))) {
            pos++;
        }
        if (start == pos) {
            throw new SchemaTranslationException("Expected integer literal at position " + pos);
        }
        return Integer.parseInt(src.substring(start, pos));
    }

    private static boolean isHex(char c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
    }

    private String parseQuotedString() {
        skipWs();
        if (pos >= src.length() || src.charAt(pos) != '"') {
            throw new SchemaTranslationException("Expected quoted string at position " + pos);
        }
        pos++;
        StringBuilder sb = new StringBuilder();
        while (pos < src.length() && src.charAt(pos) != '"') {
            char c = src.charAt(pos++);
            if (c == '\\' && pos < src.length()) {
                char esc = src.charAt(pos++);
                switch (esc) {
                    case 'n':
                        sb.append('\n');
                        break;
                    case 't':
                        sb.append('\t');
                        break;
                    case '"':
                        sb.append('"');
                        break;
                    case '\\':
                        sb.append('\\');
                        break;
                    default:
                        sb.append(esc);
                }
            } else {
                sb.append(c);
            }
        }
        if (pos >= src.length()) {
            throw new SchemaTranslationException("Unterminated string literal");
        }
        pos++; // closing quote
        return sb.toString();
    }

    private void skipWs() {
        while (pos < src.length()) {
            char c = src.charAt(pos);
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                pos++;
            } else if (c == '/' && pos + 1 < src.length()) {
                char n = src.charAt(pos + 1);
                if (n == '/') {
                    pos += 2;
                    while (pos < src.length() && src.charAt(pos) != '\n') {
                        pos++;
                    }
                } else if (n == '*') {
                    pos += 2;
                    while (pos + 1 < src.length()
                            && !(src.charAt(pos) == '*' && src.charAt(pos + 1) == '/')) {
                        pos++;
                    }
                    if (pos + 1 < src.length()) {
                        pos += 2;
                    }
                } else {
                    return;
                }
            } else {
                return;
            }
        }
    }

    private void skipUntil(char term) {
        while (pos < src.length() && src.charAt(pos) != term) {
            if (src.charAt(pos) == '"') {
                // Skip string literal contents so a ';' inside a quoted value isn't the terminator.
                pos++;
                while (pos < src.length() && src.charAt(pos) != '"') {
                    if (src.charAt(pos) == '\\' && pos + 1 < src.length()) {
                        pos += 2;
                    } else {
                        pos++;
                    }
                }
            }
            pos++;
        }
    }

    private void skipBalanced(char open, char close) {
        expect(open);
        int depth = 1;
        while (pos < src.length() && depth > 0) {
            char c = src.charAt(pos);
            if (c == '"') {
                pos++;
                while (pos < src.length() && src.charAt(pos) != '"') {
                    if (src.charAt(pos) == '\\' && pos + 1 < src.length()) {
                        pos += 2;
                    } else {
                        pos++;
                    }
                }
                pos++;
            } else if (c == open) {
                depth++;
                pos++;
            } else if (c == close) {
                depth--;
                pos++;
            } else {
                pos++;
            }
        }
    }

    // ================================================================
    // AST
    // ================================================================

    /** Parsed {@code .proto} file. */
    public static final class ProtoFile {
        public String syntax;
        public String pkg;
        public final List<String> imports = new ArrayList<>();
        public final List<ProtoMessage> messages = new ArrayList<>();
        public final List<ProtoEnum> enums = new ArrayList<>();
    }

    /** Parsed message declaration. */
    public static final class ProtoMessage {
        public String name;
        public final List<ProtoField> fields = new ArrayList<>();
        public final List<ProtoMessage> nestedMessages = new ArrayList<>();
        public final List<ProtoEnum> nestedEnums = new ArrayList<>();
        public final List<ProtoOneof> oneofs = new ArrayList<>();
    }

    /** Parsed field declaration. */
    public static final class ProtoField {
        /** {@code "optional"}, {@code "required"}, {@code "repeated"}, or null (proto3 default). */
        public String modifier;

        /** For example {@code "int32"}, {@code "string"}, {@code "map"}, or a message/enum name. */
        public String type;

        public String name;
        public int tag;

        /** Only set when {@code type == "map"}. */
        public String mapKeyType;

        /** Only set when {@code type == "map"}. */
        public String mapValueType;

        /** Non-null iff this field is inside a oneof block. */
        public String oneofName;
    }

    /** Parsed oneof block. */
    public static final class ProtoOneof {
        public String name;
        public final List<ProtoField> fields = new ArrayList<>();
    }

    /** Parsed enum declaration. */
    public static final class ProtoEnum {
        public String name;
        public final Map<String, Integer> values = new LinkedHashMap<>();
    }
}
