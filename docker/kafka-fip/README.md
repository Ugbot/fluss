# Fluss Kafka-API FIP — multi-node test harness

A reproducible **podman** (or docker) cluster + a Python Kafka-SDK conformance
suite that exercises the bar the FIP claims to clear: stock `kafka-clients`-style
producer / consumer / admin operations work as a **drop-in Kafka replacement**
against a three-broker Fluss deployment.

> **Engine**: per project convention this defaults to `podman`. Pass
> `--engine docker` to any `cluster.sh` invocation (or set `ENGINE=docker`)
> to fall back to Docker.

## What it spins up

| Container | Role | Ports |
|-----------|------|-------|
| `fluss-fip-zk` | ZooKeeper 3.9 (standalone) | 2181/tcp (internal only) |
| `fluss-fip-coordinator` | Fluss coordinator server | 9123/tcp (internal only) |
| `fluss-fip-ts0..2` | Fluss tablet servers; each binds `FLUSS://` *and* `KAFKA://` | 19092 / 29092 / 39092 → 9092 |
| `fluss-fip-kcat` (profile `tools`) | kcat 1.7 for manual smoke | n/a |

Kafka bootstrap from the host: `localhost:19092,localhost:29092,localhost:39092`.

Each tablet server enables the bolt-on with the FIP's drop-in defaults:

```
kafka.enabled: true
kafka.listener-names: KAFKA
kafka.database: kafka
kafka.offsets.store: fluss_pk_table     # durable offsets in __consumer_offsets__
kafka.log-format: ARROW                 # passthrough 4-tuple in columnar storage
```

## Quick start

```bash
# 1) Build the fluss/fluss:1.0-SNAPSHOT image from the current source tree.
docker/kafka-fip/cluster.sh build

# 2) Start the 5-container cluster.
docker/kafka-fip/cluster.sh up

# 3) Hit the bolt-on with a Python conformance suite.
pip install -r docker/kafka-fip/e2e/requirements.txt
pytest -v docker/kafka-fip/e2e/

# 4) Tear down.
docker/kafka-fip/cluster.sh down
```

## What the test suite covers

Located in `docker/kafka-fip/e2e/test_kafka_drop_in.py`. Each test cleans up
its own topic; tests are order-independent; per-test timeout is 60 – 120s.

| Test | What it asserts |
|------|-----------------|
| `test_cluster_metadata_lists_three_brokers` | `ApiVersions` + `Metadata` work; three tablet servers visible as Kafka brokers `0,1,2`. |
| `test_create_topic_then_delete` | `kafka-topics.sh` equivalents (`CreateTopics` / `ListTopics` / `DeleteTopics`) round-trip. |
| `test_describe_topic_configs` | `DescribeConfigs` on a topic resource doesn't throw. |
| `test_create_topic_rejects_unknown_cleanup_policy` | Bad `cleanup.policy` values are rejected at create time. |
| `test_single_record_round_trip` | Produce → Fetch with key, value, **headers** (including a null-valued header) byte-identical. |
| `test_multi_partition_fan_out_and_back` | 32 records spread across 3 partitions all delivered to a single consumer. |
| `test_consumer_group_commits_offsets_durably` | Two consumer sessions in the same group; second session resumes at the first session's last commit (offset durability via `__consumer_offsets__`). |
| `test_compacted_topic_is_pk_table_and_last_writer_wins` | `cleanup.policy=compact` topic maps to a Fluss PK table; consumer sees the latest write for a repeated key. |

## Manual smoke

`docker/kafka-fip/cluster.sh smoke` runs a single produce + consume via kcat
inside the cluster network. Useful when the Python harness isn't installed.

```bash
docker/kafka-fip/cluster.sh kcat -L -b ts0:9092          # broker metadata
echo "hi" | docker/kafka-fip/cluster.sh kcat -P -t demo  # produce
docker/kafka-fip/cluster.sh kcat -C -t demo -e           # consume one batch
```

## Out of scope (per FIP-NN)

- Confluent Schema Registry HTTP endpoint (`/subjects`, `/config`, ...)
- Typed Kafka topics (Avro / JSON / Protobuf decoded into typed columns)
- Transactions / EOS: `InitProducerId` returns a stub id, idempotent-only.
  All `Txn*` APIs return `UNSUPPORTED_VERSION`.
- ACL admin APIs (`CreateAcls` / `DeleteAcls` / `DescribeAcls`)
- SCRAM credential admin (`Describe/AlterUserScramCredentials`)
- Client quotas (`Describe/AlterClientQuotas`)
- `CreatePartitions`, `DeleteRecords`, `ElectLeaders`, `OffsetForLeaderEpoch`,
  `DescribeProducers`

The harness does not exercise these; sending them would return
`UNSUPPORTED_VERSION` cleanly.
