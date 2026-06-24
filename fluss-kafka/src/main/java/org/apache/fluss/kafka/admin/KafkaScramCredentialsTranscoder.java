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
import org.apache.fluss.security.auth.sasl.scram.ScramCredential;
import org.apache.fluss.security.auth.sasl.scram.ScramCredentialStore;
import org.apache.fluss.security.auth.sasl.scram.ScramCredentialUtils;
import org.apache.fluss.security.auth.sasl.scram.ScramMechanism;

import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData;
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData.ScramCredentialDeletion;
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData.ScramCredentialUpsertion;
import org.apache.kafka.common.message.AlterUserScramCredentialsResponseData;
import org.apache.kafka.common.message.AlterUserScramCredentialsResponseData.AlterUserScramCredentialsResult;
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData.UserName;
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData.CredentialInfo;
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult;
import org.apache.kafka.common.protocol.Errors;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Transcoder for the Kafka admin APIs {@code DESCRIBE_USER_SCRAM_CREDENTIALS} (50) and {@code
 * ALTER_USER_SCRAM_CREDENTIALS} (51).
 *
 * <p>Describe returns only {@code (user, mechanism, iterations)} — never salt or stored keys.
 *
 * <p>Alter consumes the Kafka {@code ScramCredentialUpsertion} payload which carries a
 * client-computed {@code salt} and {@code saltedPassword} (PBKDF2 output). The server derives
 * {@code ClientKey}, {@code StoredKey} and {@code ServerKey} from that saltedPassword and writes
 * the row via {@link ScramCredentialStore#upsert}.
 *
 * <p>Both handlers expect the caller to have already gated on {@code ALTER on CLUSTER}.
 */
@Internal
public final class KafkaScramCredentialsTranscoder {

    /** Kafka's enum byte for {@code SCRAM-SHA-256}. */
    public static final byte MECHANISM_SHA_256 = 1;
    /** Kafka's enum byte for {@code SCRAM-SHA-512}. */
    public static final byte MECHANISM_SHA_512 = 2;

    private KafkaScramCredentialsTranscoder() {}

    /** Handles a Kafka {@code DescribeUserScramCredentialsRequest}. */
    public static DescribeUserScramCredentialsResponseData handleDescribe(
            DescribeUserScramCredentialsRequestData request, ScramCredentialStore store) {
        DescribeUserScramCredentialsResponseData response =
                new DescribeUserScramCredentialsResponseData();

        // Null users() means "describe all users".
        List<UserName> users = request.users();
        Map<String, DescribeUserScramCredentialsResult> resultsByUser = new LinkedHashMap<>();
        Set<String> requestedUsers;
        if (users == null || users.isEmpty()) {
            requestedUsers = null;
        } else {
            requestedUsers = new HashSet<>(users.size());
            for (UserName u : users) {
                requestedUsers.add(u.name());
                // Seed slot so unknown-user shows up with RESOURCE_NOT_FOUND error.
                resultsByUser.put(
                        u.name(),
                        new DescribeUserScramCredentialsResult()
                                .setUser(u.name())
                                .setErrorCode(Errors.RESOURCE_NOT_FOUND.code())
                                .setErrorMessage(
                                        "Attempt to describe a user credential that does not exist"));
            }
        }

        for (ScramCredentialStore.Entry entry : store.list()) {
            if (requestedUsers != null && !requestedUsers.contains(entry.principalName())) {
                continue;
            }
            DescribeUserScramCredentialsResult row =
                    resultsByUser.computeIfAbsent(
                            entry.principalName(),
                            u ->
                                    new DescribeUserScramCredentialsResult()
                                            .setUser(u)
                                            .setErrorCode(Errors.NONE.code())
                                            .setCredentialInfos(new ArrayList<>()));
            // Switch status from NOT_FOUND to NONE the moment we find at least one credential.
            row.setErrorCode(Errors.NONE.code());
            row.setErrorMessage(null);
            CredentialInfo info =
                    new CredentialInfo()
                            .setMechanism(kafkaMechanismByte(entry.mechanism()))
                            .setIterations(entry.iterations());
            row.credentialInfos().add(info);
        }

        response.setResults(new ArrayList<>(resultsByUser.values()));
        return response;
    }

    /** Handles a Kafka {@code AlterUserScramCredentialsRequest}. */
    public static AlterUserScramCredentialsResponseData handleAlter(
            AlterUserScramCredentialsRequestData request, ScramCredentialStore store) {
        AlterUserScramCredentialsResponseData response =
                new AlterUserScramCredentialsResponseData();
        Map<String, AlterUserScramCredentialsResult> results = new LinkedHashMap<>();

        List<ScramCredentialDeletion> deletions =
                Optional.ofNullable(request.deletions()).orElse(Collections.emptyList());
        List<ScramCredentialUpsertion> upsertions =
                Optional.ofNullable(request.upsertions()).orElse(Collections.emptyList());

        for (ScramCredentialDeletion del : deletions) {
            AlterUserScramCredentialsResult res =
                    results.computeIfAbsent(
                            del.name(), u -> new AlterUserScramCredentialsResult().setUser(u));
            ScramMechanism mech = resolve(del.mechanism());
            if (mech == null) {
                res.setErrorCode(Errors.UNSUPPORTED_SASL_MECHANISM.code())
                        .setErrorMessage("Unknown SCRAM mechanism byte " + del.mechanism());
                continue;
            }
            try {
                store.delete(del.name(), mech.mechanismName());
                if (res.errorCode() == Errors.NONE.code()) {
                    res.setErrorCode(Errors.NONE.code());
                }
            } catch (UnsupportedOperationException e) {
                res.setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code())
                        .setErrorMessage("Credential store is read-only");
            } catch (RuntimeException e) {
                res.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                        .setErrorMessage(e.getMessage());
            }
        }

        for (ScramCredentialUpsertion ups : upsertions) {
            AlterUserScramCredentialsResult res =
                    results.computeIfAbsent(
                            ups.name(), u -> new AlterUserScramCredentialsResult().setUser(u));
            ScramMechanism mech = resolve(ups.mechanism());
            if (mech == null) {
                res.setErrorCode(Errors.UNSUPPORTED_SASL_MECHANISM.code())
                        .setErrorMessage("Unknown SCRAM mechanism byte " + ups.mechanism());
                continue;
            }
            if (ups.iterations() < mech.minIterations()) {
                res.setErrorCode(Errors.UNACCEPTABLE_CREDENTIAL.code())
                        .setErrorMessage(
                                "iterations "
                                        + ups.iterations()
                                        + " below mechanism minimum "
                                        + mech.minIterations());
                continue;
            }
            if (ups.salt() == null || ups.salt().length == 0) {
                res.setErrorCode(Errors.UNACCEPTABLE_CREDENTIAL.code())
                        .setErrorMessage("salt must be non-empty");
                continue;
            }
            if (ups.saltedPassword() == null || ups.saltedPassword().length == 0) {
                res.setErrorCode(Errors.UNACCEPTABLE_CREDENTIAL.code())
                        .setErrorMessage("saltedPassword must be non-empty");
                continue;
            }
            // saltedPassword in the wire protocol is already Hi(password, salt, iterations).
            byte[] saltedPassword = ups.saltedPassword();
            byte[] clientKey =
                    ScramCredentialUtils.hmac(
                            mech, saltedPassword, "Client Key".getBytes(StandardCharsets.UTF_8));
            byte[] storedKey = ScramCredentialUtils.h(mech, clientKey);
            byte[] serverKey =
                    ScramCredentialUtils.hmac(
                            mech, saltedPassword, "Server Key".getBytes(StandardCharsets.UTF_8));
            ScramCredential credential =
                    new ScramCredential(ups.salt(), storedKey, serverKey, ups.iterations());
            try {
                store.upsert(ups.name(), mech.mechanismName(), credential);
                res.setErrorCode(Errors.NONE.code());
                res.setErrorMessage(null);
            } catch (UnsupportedOperationException e) {
                res.setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code())
                        .setErrorMessage("Credential store is read-only");
            } catch (RuntimeException e) {
                res.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                        .setErrorMessage(e.getMessage());
            }
        }

        response.setResults(new ArrayList<>(results.values()));
        return response;
    }

    /** Maps Fluss/SASL mechanism names to Kafka's byte enum. */
    public static byte kafkaMechanismByte(String mechanismName) {
        if (ScramMechanism.SCRAM_SHA_256.mechanismName().equals(mechanismName)) {
            return MECHANISM_SHA_256;
        }
        if (ScramMechanism.SCRAM_SHA_512.mechanismName().equals(mechanismName)) {
            return MECHANISM_SHA_512;
        }
        return 0;
    }

    /** Maps Kafka's byte enum to Fluss mechanisms. Returns {@code null} for unknown bytes. */
    public static ScramMechanism resolve(byte mechanism) {
        switch (mechanism) {
            case MECHANISM_SHA_256:
                return ScramMechanism.SCRAM_SHA_256;
            case MECHANISM_SHA_512:
                return ScramMechanism.SCRAM_SHA_512;
            default:
                return null;
        }
    }
}
