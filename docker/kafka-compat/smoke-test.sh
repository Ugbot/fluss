#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Smoke test for the 3-tablet kafka-compat Docker/Podman stack.
# Requires: kcat, kafka-topics (Kafka CLI), curl, jq on PATH.
# Run after: podman compose up -d && sleep 15
set -euo pipefail

BOOTSTRAP="localhost:9092"
SR="http://localhost:8081"
TOPIC="smoke-test"

echo "==> [1] Bootstrap check — expect 3 brokers"
BROKER_COUNT=$(kcat -b "$BOOTSTRAP" -L 2>/dev/null | grep -c "broker " || true)
if [[ "$BROKER_COUNT" -lt 3 ]]; then
    echo "FAIL: only $BROKER_COUNT broker(s) visible from $BOOTSTRAP"
    exit 1
fi
echo "    OK: $BROKER_COUNT brokers"

echo "==> [2] Create topic $TOPIC (3 partitions, RF 3)"
kafka-topics --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$TOPIC" \
    --partitions 3 \
    --replication-factor 3

echo "==> [3] Produce 9 messages (3 per partition)"
for P in 0 1 2; do
    printf "p%d-msg0\np%d-msg1\np%d-msg2\n" "$P" "$P" "$P" \
        | kcat -b "$BOOTSTRAP" -t "$TOPIC" -p "$P" -P
done

echo "==> [4] Consume and verify 3 messages per partition"
for P in 0 1 2; do
    COUNT=$(kcat -b "$BOOTSTRAP" -t "$TOPIC" -p "$P" -C -e -q 2>/dev/null | wc -l | tr -d ' ')
    if [[ "$COUNT" -ne 3 ]]; then
        echo "FAIL: partition $P returned $COUNT messages, expected 3"
        exit 1
    fi
    echo "    partition $P: $COUNT messages OK"
done

echo "==> [5] Schema Registry ping"
curl -sf "$SR/subjects" > /dev/null
echo "    OK"

echo "==> [6] Register Avro schema"
SCHEMA_JSON='{"type":"record","name":"Smoke","fields":[{"name":"id","type":"string"}]}'
PAYLOAD="{\"schema\":\"$(echo "$SCHEMA_JSON" | sed 's/"/\\"/g')\"}"
RESPONSE=$(curl -sf -X POST "$SR/subjects/smoke-value/versions" \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d "$PAYLOAD")
SCHEMA_ID=$(echo "$RESPONSE" | grep -o '"id":[0-9]*' | grep -o '[0-9]*')
if [[ -z "$SCHEMA_ID" ]]; then
    echo "FAIL: schema registration response had no 'id': $RESPONSE"
    exit 1
fi
echo "    registered schema id=$SCHEMA_ID"

echo "==> [7] Fetch schema by id"
FETCH=$(curl -sf "$SR/schemas/ids/$SCHEMA_ID")
if ! echo "$FETCH" | grep -q '"schema"'; then
    echo "FAIL: GET /schemas/ids/$SCHEMA_ID did not contain 'schema' key: $FETCH"
    exit 1
fi
echo "    OK"

echo "==> [8] Cross-broker metadata (tablet1 / tablet2)"
for PORT in 9093 9094; do
    COUNT=$(kcat -b "localhost:$PORT" -L 2>/dev/null | grep -c "broker " || true)
    if [[ "$COUNT" -lt 3 ]]; then
        echo "FAIL: only $COUNT broker(s) visible from localhost:$PORT"
        exit 1
    fi
    echo "    localhost:$PORT sees $COUNT brokers OK"
done

echo "==> smoke test passed"
