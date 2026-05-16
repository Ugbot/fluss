# Test plan — Kafka API FIP

Reproducible, top-to-bottom validation that the Fluss Kafka bolt-on speaks
**Kafka 3.9 wire protocol** as a drop-in replacement for stock clients. Every
step has a single concrete pass/fail criterion. Run from the repo root.

Per project convention `podman` is the default engine; pass
`--engine docker` to any `cluster.sh` call to use Docker instead.

## Pre-flight

```bash
# A) Build the fluss-kafka jar + the dist tarball needed for the image.
JAVA_HOME=11 mvn -pl fluss-dist -am -DskipTests -Drat.skip=true install

# B) Build the podman image (bundles fluss-kafka + kafka-clients + deps).
docker/kafka-fip/cluster.sh build

# C) Bring the 5-container cluster up.
docker/kafka-fip/cluster.sh up

# D) Confirm five containers are healthy (ZK + coordinator + 3 TS).
docker/kafka-fip/cluster.sh status
```

Pass: all 5 containers in `Up` state. Kafka bootstrap from the host:
`localhost:19092,localhost:29092,localhost:39092`.

## §1. In-JVM Java integration tests

Java `kafka-clients` 3.9.2 talking to an in-process `FlussClusterExtension`.
These are the strongest guarantee that the bolt-on speaks the protocol
correctly because they use the same client library that production
deployments will. No external network involved.

```bash
JAVA_HOME=11 mvn -pl fluss-kafka \
  -Dtest='KafkaRequestITCase,KafkaArrowProduceITCase,KafkaIndexedLogFormatITCase' \
  test
```

Pass criteria:
- `KafkaRequestITCase` — 1/1.
- `KafkaArrowProduceITCase` — 4/4. Covers topic creation, single-record
  round-trip with byte-identical key/value/headers, multi-partition
  fan-out, compacted-topic PK semantics.
- `KafkaIndexedLogFormatITCase` — 1/1. Rollback path with
  `kafka.log-format=INDEXED`.

If any of these fails, abort the rest of the plan — wider tooling tests
won't tell us more.

## §2. kcat smoke (librdkafka)

`kcat` is the canonical "is this Kafka?" probe; it uses the same
`librdkafka` library Confluent ships. Runs inside the cluster network so
the broker hostnames (`ts0` / `ts1` / `ts2`) resolve.

```bash
# Metadata — must list three brokers and a controller.
docker/kafka-fip/cluster.sh kcat -b ts0:9092 -L

# Create a topic via the admin path (Java cli or python), then:
TOPIC="kcat-$RANDOM"
podman run --rm -i --network fluss-fip-net \
    edenhill/kcat:1.7.1 kcat -P -b ts0:9092 -t "$TOPIC" <<< "hello-fluss"
podman run --rm    --network fluss-fip-net \
    edenhill/kcat:1.7.1 kcat -C -b ts0:9092 -t "$TOPIC" -e
```

Pass: metadata response lists `broker 0/1/2`; produce reports no
delivery error; consume returns the same byte sequence we produced.

## §3. kafka-python-ng end-to-end

The Python harness that's checked in at `docker/kafka-fip/e2e/`. Validates
admin + producer + consumer + consumer-groups all negotiate against the
full ApiVersions surface.

```bash
# A) Run pytest from a Python container inside the cluster network, so
#    hostname resolution of ts0/ts1/ts2 works.
podman run --rm --network fluss-fip-net \
  -v "$(pwd)/docker/kafka-fip/e2e:/e2e:Z" \
  python:3.12-slim sh -c '
    pip install -q -r /e2e/requirements.txt
    cd /e2e && python -m pytest -v --timeout=120 test_kafka_drop_in.py
'
```

Pass: 8/8 pytest cases pass (or 7/8 with the compacted-topic
last-writer-wins case skipped if compaction hasn't kicked in).

The previous round's `UnsupportedVersionError` on `ListOffsets` must be
gone — without that fix, every consumer test fails at
`seek_to_beginning`.

## §4. Confluent CLI tools

The kafka-clients-bundled shell scripts. These are what operators reach
for first; if `kafka-topics.sh` doesn't work, drop-in is dead.

```bash
CLI="podman run --rm --network fluss-fip-net confluentinc/cp-kafka:7.6.0"

# §4.1 — kafka-topics.sh: list / create / describe / delete.
$CLI kafka-topics --bootstrap-server ts0:9092 --list
$CLI kafka-topics --bootstrap-server ts0:9092 --create \
    --topic cli-demo --partitions 3 --replication-factor 1
$CLI kafka-topics --bootstrap-server ts0:9092 --describe --topic cli-demo
$CLI kafka-topics --bootstrap-server ts0:9092 --delete --topic cli-demo

# §4.2 — kafka-consumer-groups.sh: list (must not throw on empty cluster).
$CLI kafka-consumer-groups --bootstrap-server ts0:9092 --list

# §4.3 — kafka-configs.sh: describe broker / topic configs.
$CLI kafka-configs --bootstrap-server ts0:9092 --describe \
    --entity-type brokers --entity-name 0
```

Pass: each command exits 0 and prints sensible output (no
`UnsupportedVersionError`, no `UNKNOWN_SERVER_ERROR`).

## §5. Compacted topic — PK table semantics

```bash
$CLI kafka-topics --bootstrap-server ts0:9092 --create \
    --topic compacted-demo --partitions 1 --replication-factor 1 \
    --config cleanup.policy=compact

# Write same key three times, different values.
for v in v1 v2 v3; do
  echo "k:$v" | podman run --rm -i --network fluss-fip-net \
    edenhill/kcat:1.7.1 kcat -P -b ts0:9092 -t compacted-demo -K:
done

# Read back — at minimum the latest write (v3) must appear.
podman run --rm --network fluss-fip-net \
    edenhill/kcat:1.7.1 kcat -C -b ts0:9092 -t compacted-demo -e
```

Pass: `v3` appears in the consumed output. Compaction may not yet have
purged older versions; either result is acceptable.

## §6. Coordinator failover — group state behaviour

Documents the known limitation that group membership is in-memory.
Offsets should survive; rebalance should fire.

```bash
# A) Create a group, commit some offsets.
TOPIC=failover-$RANDOM
$CLI kafka-topics --bootstrap-server ts0:9092 --create \
    --topic "$TOPIC" --partitions 2 --replication-factor 1
echo -e "msg1\nmsg2\nmsg3" | podman run --rm -i --network fluss-fip-net \
    edenhill/kcat:1.7.1 kcat -P -b ts0:9092 -t "$TOPIC"
podman run --rm --network fluss-fip-net edenhill/kcat:1.7.1 \
    kcat -C -b ts0:9092 -t "$TOPIC" -G failover-grp -e

# B) Restart the coordinator (the lowest-id TS by our resolveControllerId).
podman restart fluss-fip-ts0

# C) After ~10s, list groups + describe offsets — must succeed.
sleep 12
$CLI kafka-consumer-groups --bootstrap-server ts0:9092 --list
$CLI kafka-consumer-groups --bootstrap-server ts0:9092 --describe \
    --group failover-grp
```

Pass: `--describe` shows the committed offsets (from
`__consumer_offsets__` PK table). It is **expected** that members appear
empty / unknown — group state is in-memory, this is the documented
follow-up.

## §7. Tickbench — protocol conformance

If a TickStream checkout is available at `~/tickstream/tickbench` (or
`$TICKBENCH_HOME`), run the functional category.

```bash
TICKBENCH_HOME="${HOME}/tickstream/tickbench" \
    docker/kafka-fip/cluster.sh tickbench functional
```

Pass: functional run completes without raising
`UnsupportedVersionError` on any test. Per-test pass/fail counts logged.

## §8. Out-of-scope APIs return clean errors (negative tests)

Confirm we return *clean* error codes, not `UnsupportedVersionError`,
for the APIs the FIP says are stubbed.

```bash
# Transactional producer should fail cleanly on InitProducerId for
# transactional.id — not crash the client.
podman run --rm --network fluss-fip-net -v "$(pwd)/docker/kafka-fip/e2e:/e2e:Z" \
  python:3.12-slim sh -c '
    pip install -q kafka-python-ng==2.2.3
    python -c "
from kafka import KafkaProducer
try:
    p = KafkaProducer(bootstrap_servers=[\"ts0:9092\"], transactional_id=\"tid-1\")
    p.init_transactions()
    print(\"UNEXPECTED: init_transactions succeeded\")
except Exception as e:
    print(\"OK:\", type(e).__name__, str(e)[:120])
"'
```

Pass: client raises `KafkaError` mentioning `TRANSACTIONAL_ID_NOT_FOUND`
(or similar broker-feature-disabled message), **not**
`UnsupportedVersionError` or a TCP-level error.

## Tear down

```bash
docker/kafka-fip/cluster.sh down
```

## Pass/fail matrix

| Section | Pass criterion | Severity if fails |
|---|---|---|
| Pre-flight | 5 containers healthy | blocker |
| §1 in-JVM ITs | 6/6 tests green | blocker |
| §2 kcat | Metadata + produce + consume work | blocker |
| §3 kafka-python-ng | 8/8 pytest (7/8 with compaction skip) | blocker |
| §4 CLI tools | All commands exit 0 | drop-in-required |
| §5 Compacted | Latest write visible | drop-in-required |
| §6 Failover | Offsets survive; describe-group works | known-limitation OK |
| §7 Tickbench | Functional category clean | informational |
| §8 Negative | Clean error codes, not protocol crashes | drop-in-required |
