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

package org.apache.fluss.kafka.sr.auth;

import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpRequest;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.FullHttpRequest;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.HttpVersion;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit coverage for {@link HttpPrincipalExtractor} — CIDR gating on {@code X-Forwarded-User}, HTTP
 * Basic decode + JAAS lookup, and empty-input edge cases.
 */
class HttpPrincipalExtractorTest {

    private static final String JAAS_ALICE =
            "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required "
                    + "user_alice=\"alice-secret\" user_bob=\"bob-secret\";";

    @Test
    void forwardedUserAcceptedFromTrustedLoopback() throws Exception {
        HttpPrincipalExtractor extractor =
                new HttpPrincipalExtractor(Collections.singletonList("127.0.0.0/8"), emptyStore());
        FullHttpRequest req = request();
        req.headers().set("X-Forwarded-User", "alice");

        Optional<FlussPrincipal> principal = extractor.extract(req, loopback());

        assertThat(principal).isPresent();
        assertThat(principal.get().getName()).isEqualTo("alice");
        assertThat(principal.get().getType()).isEqualTo("User");
    }

    @Test
    void forwardedUserRejectedFromUntrustedSource() throws Exception {
        HttpPrincipalExtractor extractor =
                new HttpPrincipalExtractor(Collections.singletonList("10.0.0.0/8"), emptyStore());
        FullHttpRequest req = request();
        req.headers().set("X-Forwarded-User", "alice");

        Optional<FlussPrincipal> principal = extractor.extract(req, loopback());

        assertThat(principal).as("loopback is not in 10/8").isEmpty();
    }

    @Test
    void forwardedUserRejectedWhenCidrListIsEmpty() throws Exception {
        HttpPrincipalExtractor extractor =
                new HttpPrincipalExtractor(Collections.<String>emptyList(), emptyStore());
        FullHttpRequest req = request();
        req.headers().set("X-Forwarded-User", "alice");

        Optional<FlussPrincipal> principal = extractor.extract(req, loopback());

        assertThat(principal).isEmpty();
    }

    @Test
    void basicAuthAcceptsCorrectCredentials() throws Exception {
        HttpPrincipalExtractor extractor =
                new HttpPrincipalExtractor(
                        Collections.<String>emptyList(),
                        JaasHttpPrincipalStore.fromJaasText(JAAS_ALICE));
        FullHttpRequest req = request();
        req.headers().set("Authorization", basic("alice", "alice-secret"));

        Optional<FlussPrincipal> principal = extractor.extract(req, loopback());

        assertThat(principal).isPresent();
        assertThat(principal.get().getName()).isEqualTo("alice");
    }

    @Test
    void basicAuthRejectsWrongPassword() throws Exception {
        HttpPrincipalExtractor extractor =
                new HttpPrincipalExtractor(
                        Collections.<String>emptyList(),
                        JaasHttpPrincipalStore.fromJaasText(JAAS_ALICE));
        FullHttpRequest req = request();
        req.headers().set("Authorization", basic("alice", "wrong"));

        Optional<FlussPrincipal> principal = extractor.extract(req, loopback());

        assertThat(principal).isEmpty();
    }

    @Test
    void basicAuthRejectsUnknownUser() throws Exception {
        HttpPrincipalExtractor extractor =
                new HttpPrincipalExtractor(
                        Collections.<String>emptyList(),
                        JaasHttpPrincipalStore.fromJaasText(JAAS_ALICE));
        FullHttpRequest req = request();
        req.headers().set("Authorization", basic("charlie", "whatever"));

        Optional<FlussPrincipal> principal = extractor.extract(req, loopback());

        assertThat(principal).isEmpty();
    }

    @Test
    void basicAuthRejectsMalformedHeader() throws Exception {
        HttpPrincipalExtractor extractor =
                new HttpPrincipalExtractor(
                        Collections.<String>emptyList(),
                        JaasHttpPrincipalStore.fromJaasText(JAAS_ALICE));
        FullHttpRequest req = request();
        req.headers().set("Authorization", "Basic not-base64!!");

        Optional<FlussPrincipal> principal = extractor.extract(req, loopback());

        assertThat(principal).isEmpty();
    }

    @Test
    void headerWinsOverBasicWhenBothPresent() throws Exception {
        HttpPrincipalExtractor extractor =
                new HttpPrincipalExtractor(
                        Collections.singletonList("127.0.0.0/8"),
                        JaasHttpPrincipalStore.fromJaasText(JAAS_ALICE));
        FullHttpRequest req = request();
        req.headers().set("X-Forwarded-User", "alice");
        req.headers().set("Authorization", basic("bob", "bob-secret"));

        Optional<FlussPrincipal> principal = extractor.extract(req, loopback());

        assertThat(principal).isPresent();
        assertThat(principal.get().getName()).isEqualTo("alice");
    }

    @Test
    void noCredentialsYieldEmpty() throws Exception {
        HttpPrincipalExtractor extractor =
                new HttpPrincipalExtractor(
                        Collections.singletonList("127.0.0.0/8"),
                        JaasHttpPrincipalStore.fromJaasText(JAAS_ALICE));
        Optional<FlussPrincipal> principal = extractor.extract(request(), loopback());
        assertThat(principal).isEmpty();
    }

    @Test
    void cidrAcceptsMultipleRanges() throws Exception {
        HttpPrincipalExtractor extractor =
                new HttpPrincipalExtractor(
                        Arrays.asList("10.0.0.0/8", "127.0.0.0/8"), emptyStore());
        FullHttpRequest req = request();
        req.headers().set("X-Forwarded-User", "alice");
        assertThat(extractor.extract(req, loopback())).isPresent();
    }

    @Test
    void bareIpAddressActsAsSingleHostRange() throws Exception {
        HttpPrincipalExtractor extractor =
                new HttpPrincipalExtractor(Collections.singletonList("127.0.0.1"), emptyStore());
        FullHttpRequest req = request();
        req.headers().set("X-Forwarded-User", "alice");
        assertThat(extractor.extract(req, loopback())).isPresent();
        // 127.0.0.2 is not 127.0.0.1.
        InetSocketAddress other = new InetSocketAddress(InetAddress.getByName("127.0.0.2"), 0);
        FullHttpRequest req2 = request();
        req2.headers().set("X-Forwarded-User", "alice");
        assertThat(extractor.extract(req2, other)).isEmpty();
    }

    @Test
    void malformedCidrIsRejectedAtConstruction() {
        try {
            new HttpPrincipalExtractor(Collections.singletonList("not-a-cidr"), emptyStore());
            throw new AssertionError("should have thrown");
        } catch (IllegalArgumentException expected) {
            // Expected.
        }
    }

    private static JaasHttpPrincipalStore emptyStore() {
        return JaasHttpPrincipalStore.fromJaasText("");
    }

    private static FullHttpRequest request() {
        return new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, HttpMethod.GET, "/subjects", Unpooled.EMPTY_BUFFER);
    }

    private static InetSocketAddress loopback() throws Exception {
        return new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
    }

    private static String basic(String user, String password) {
        String raw = user + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }
}
