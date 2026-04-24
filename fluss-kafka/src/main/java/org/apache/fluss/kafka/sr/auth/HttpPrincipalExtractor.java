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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.FullHttpRequest;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Resolves the caller's {@link FlussPrincipal} for a single Schema Registry HTTP request.
 *
 * <p>Two trust paths, tried in order:
 *
 * <ol>
 *   <li><b>X-Forwarded-User</b> — the reverse-proxy path. The extractor only honours the header
 *       when {@code remote} matches one of the {@link #trustedProxyCidrs} entries; an empty CIDR
 *       list means the header is never trusted. This prevents a client that speaks directly to the
 *       SR from impersonating anyone it likes.
 *   <li><b>HTTP Basic</b> — the dev-mode fallback. {@code Authorization: Basic <base64>} is
 *       decoded, split on the first {@code :}, and the pair is checked against {@link
 *       JaasHttpPrincipalStore}.
 * </ol>
 *
 * <p>The header wins when both are present. Returns {@link Optional#empty()} when neither path
 * yields a principal — the caller is then expected to fall back to {@code ANONYMOUS}.
 */
@Internal
public final class HttpPrincipalExtractor {

    private static final Logger LOG = LoggerFactory.getLogger(HttpPrincipalExtractor.class);

    /** Header Confluent / NGINX-style proxies set when forwarding the authenticated username. */
    public static final String HEADER_FORWARDED_USER = "X-Forwarded-User";

    /** Standard HTTP authorisation header. */
    public static final String HEADER_AUTHORIZATION = "Authorization";

    /** Scheme prefix for HTTP Basic auth, case-insensitive in practice but canonical here. */
    private static final String BASIC_PREFIX = "Basic ";

    /** Principal type stored on {@link FlussPrincipal} for HTTP-extracted identities. */
    private static final String PRINCIPAL_TYPE_USER = "User";

    private final List<CidrRange> trustedProxyCidrs;
    private final JaasHttpPrincipalStore basicAuthStore;

    public HttpPrincipalExtractor(
            List<String> trustedProxyCidrs, JaasHttpPrincipalStore basicAuthStore) {
        checkNotNull(trustedProxyCidrs, "trustedProxyCidrs");
        checkNotNull(basicAuthStore, "basicAuthStore");
        List<CidrRange> parsed = new ArrayList<>(trustedProxyCidrs.size());
        for (String cidr : trustedProxyCidrs) {
            if (cidr == null) {
                continue;
            }
            String trimmed = cidr.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            parsed.add(CidrRange.parse(trimmed));
        }
        this.trustedProxyCidrs = Collections.unmodifiableList(parsed);
        this.basicAuthStore = basicAuthStore;
    }

    /**
     * Extract the caller principal. Returns empty when neither trust path yields a result; the
     * caller translates empty to {@code ANONYMOUS}.
     */
    public Optional<FlussPrincipal> extract(FullHttpRequest request, InetSocketAddress remote) {
        if (request == null) {
            return Optional.empty();
        }
        HttpHeaders headers = request.headers();

        // 1. X-Forwarded-User — only honoured from a trusted source address.
        String forwarded = headers.get(HEADER_FORWARDED_USER);
        if (forwarded != null && !forwarded.isEmpty() && isTrustedRemote(remote)) {
            String name = forwarded.trim();
            if (!name.isEmpty()) {
                return Optional.of(new FlussPrincipal(name, PRINCIPAL_TYPE_USER));
            }
        }

        // 2. HTTP Basic.
        String auth = headers.get(HEADER_AUTHORIZATION);
        if (auth != null && auth.regionMatches(true, 0, BASIC_PREFIX, 0, BASIC_PREFIX.length())) {
            String encoded = auth.substring(BASIC_PREFIX.length()).trim();
            if (!encoded.isEmpty()) {
                String decoded;
                try {
                    decoded =
                            new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
                } catch (IllegalArgumentException bad) {
                    LOG.debug("SR Basic auth header is not valid base64", bad);
                    return Optional.empty();
                }
                int sep = decoded.indexOf(':');
                if (sep < 0) {
                    return Optional.empty();
                }
                String user = decoded.substring(0, sep);
                String password = decoded.substring(sep + 1);
                Optional<String> expected = basicAuthStore.lookup(user);
                if (expected.isPresent() && expected.get().equals(password)) {
                    return Optional.of(new FlussPrincipal(user, PRINCIPAL_TYPE_USER));
                }
            }
        }

        return Optional.empty();
    }

    private boolean isTrustedRemote(InetSocketAddress remote) {
        if (trustedProxyCidrs.isEmpty() || remote == null) {
            return false;
        }
        InetAddress addr = remote.getAddress();
        if (addr == null) {
            return false;
        }
        for (CidrRange range : trustedProxyCidrs) {
            if (range.contains(addr)) {
                return true;
            }
        }
        return false;
    }

    /**
     * IPv4/IPv6 CIDR block with a {@code contains(InetAddress)} membership test. Used to gate the
     * {@code X-Forwarded-User} header on source-address trust.
     */
    static final class CidrRange {
        private final byte[] network;
        private final int prefixBits;

        private CidrRange(byte[] network, int prefixBits) {
            this.network = network;
            this.prefixBits = prefixBits;
        }

        static CidrRange parse(String cidr) {
            int slash = cidr.indexOf('/');
            String host;
            int prefix;
            if (slash < 0) {
                host = cidr;
                // Bare IP → /32 (IPv4) or /128 (IPv6) single-host range.
                prefix = -1;
            } else {
                host = cidr.substring(0, slash);
                String prefixStr = cidr.substring(slash + 1);
                try {
                    prefix = Integer.parseInt(prefixStr);
                } catch (NumberFormatException nfe) {
                    throw new IllegalArgumentException(
                            "Invalid CIDR prefix in '" + cidr + "'", nfe);
                }
            }
            InetAddress addr;
            try {
                addr = InetAddress.getByName(host);
            } catch (UnknownHostException uhe) {
                throw new IllegalArgumentException(
                        "Invalid CIDR host in '" + cidr + "': " + uhe.getMessage(), uhe);
            }
            byte[] bytes = addr.getAddress();
            int max = bytes.length * 8;
            if (prefix < 0) {
                prefix = max;
            }
            if (prefix < 0 || prefix > max) {
                throw new IllegalArgumentException(
                        "CIDR prefix out of range for address family in '" + cidr + "'");
            }
            byte[] masked = applyMask(bytes, prefix);
            return new CidrRange(masked, prefix);
        }

        boolean contains(InetAddress candidate) {
            byte[] bytes = candidate.getAddress();
            if (bytes.length * 8 != network.length * 8) {
                // IPv4 vs IPv6 family mismatch — not in this range.
                return false;
            }
            byte[] candidateMasked = applyMask(bytes, prefixBits);
            for (int i = 0; i < network.length; i++) {
                if (candidateMasked[i] != network[i]) {
                    return false;
                }
            }
            return true;
        }

        private static byte[] applyMask(byte[] bytes, int prefixBits) {
            byte[] out = new byte[bytes.length];
            int fullBytes = prefixBits / 8;
            int remainingBits = prefixBits % 8;
            for (int i = 0; i < bytes.length; i++) {
                if (i < fullBytes) {
                    out[i] = bytes[i];
                } else if (i == fullBytes && remainingBits > 0) {
                    int mask = (0xFF << (8 - remainingBits)) & 0xFF;
                    out[i] = (byte) (bytes[i] & mask);
                } else {
                    out[i] = 0;
                }
            }
            return out;
        }
    }
}
