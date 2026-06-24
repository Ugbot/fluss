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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Source-address trust list for the Schema Registry's {@code X-Forwarded-User} header path.
 *
 * <p>Parses a list of CIDR strings once at bootstrap (invalid inputs fail fast with {@link
 * IllegalArgumentException}) into a {@link CidrRange}-per-entry list. Runtime membership is a
 * single linear scan over the list — trusted-proxy fleets are small (typically 1–10 entries) so a
 * linear scan is strictly cheaper than a hash-based lookup after prefix normalisation.
 *
 * <p>Both IPv4 and IPv6 ranges are accepted. A bare IP ({@code 10.1.2.3}) is treated as {@code /32}
 * (IPv4) or {@code /128} (IPv6) — a single-host range. Family mismatch ({@code contains} called
 * with an IPv6 address against an IPv4 range or vice versa) always returns {@code false}.
 *
 * <p>An empty list means no remote is trusted: {@link #contains(InetAddress)} returns {@code false}
 * for every address and the header path is effectively disabled.
 */
@Internal
final class TrustedCidrSet {

    private final List<CidrRange> ranges;

    private TrustedCidrSet(List<CidrRange> ranges) {
        this.ranges = Collections.unmodifiableList(ranges);
    }

    /**
     * Parse the given CIDR strings. {@code null} and blank entries are skipped; other entries must
     * be valid CIDR notation or the constructor throws.
     */
    static TrustedCidrSet parse(List<String> cidrs) {
        if (cidrs == null || cidrs.isEmpty()) {
            return new TrustedCidrSet(Collections.<CidrRange>emptyList());
        }
        List<CidrRange> parsed = new ArrayList<>(cidrs.size());
        for (String cidr : cidrs) {
            if (cidr == null) {
                continue;
            }
            String trimmed = cidr.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            parsed.add(CidrRange.parse(trimmed));
        }
        return new TrustedCidrSet(parsed);
    }

    /** Whether any ranges are configured. Empty → header path is disabled. */
    boolean isEmpty() {
        return ranges.isEmpty();
    }

    /** Whether {@code candidate} falls inside any configured range. */
    boolean contains(InetAddress candidate) {
        if (candidate == null || ranges.isEmpty()) {
            return false;
        }
        for (CidrRange range : ranges) {
            if (range.contains(candidate)) {
                return true;
            }
        }
        return false;
    }

    /**
     * One CIDR block. Package-private so the extractor's unit tests can exercise boundary cases
     * directly without having to round-trip through string parsing.
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
            if (prefix > max) {
                throw new IllegalArgumentException(
                        "CIDR prefix out of range for address family in '" + cidr + "'");
            }
            byte[] masked = applyMask(bytes, prefix);
            return new CidrRange(masked, prefix);
        }

        boolean contains(InetAddress candidate) {
            byte[] bytes = candidate.getAddress();
            if (bytes.length != network.length) {
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
