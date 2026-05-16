#!/usr/bin/env bash
# Multi-node Fluss + Kafka bolt-on cluster control script. Uses podman by
# default (per project preference) but accepts `--engine docker` to fall back.
#
# Usage:
#     docker/kafka-fip/cluster.sh build        # build the fluss:1.0-SNAPSHOT image from the local repo
#     docker/kafka-fip/cluster.sh up           # start the 5-container cluster
#     docker/kafka-fip/cluster.sh status       # show container + healthcheck state
#     docker/kafka-fip/cluster.sh logs [svc]   # tail logs (svc = ts0|ts1|ts2|coordinator|zookeeper)
#     docker/kafka-fip/cluster.sh kcat ...     # exec kcat with bootstrap pre-wired
#     docker/kafka-fip/cluster.sh smoke        # produce + consume a single record via kcat
#     docker/kafka-fip/cluster.sh down         # stop + remove containers + volumes
#
# Set ENGINE=docker (or pass --engine docker) to use Docker instead of podman.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/docker/kafka-fip/compose.yaml"
IMAGE_TAG="fluss/fluss:1.0-SNAPSHOT"
BOOTSTRAP="localhost:19092,localhost:29092,localhost:39092"

ENGINE="${ENGINE:-podman}"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --engine)
            ENGINE="$2"; shift 2 ;;
        *)
            break ;;
    esac
done

if [[ "${ENGINE}" != "podman" && "${ENGINE}" != "docker" ]]; then
    echo "error: --engine must be podman or docker (got ${ENGINE})" >&2
    exit 2
fi

CMD="${1:-help}"; shift || true

compose() {
    "${ENGINE}" compose -f "${COMPOSE_FILE}" "$@"
}

case "${CMD}" in
    build)
        # Build the Fluss distribution + image. Expects you've already run
        #   mvn -pl fluss-dist -am -DskipTests install
        # so that fluss-dist/target/fluss-1.0-SNAPSHOT-bin/fluss-1.0-SNAPSHOT exists.
        DIST_DIR="${ROOT_DIR}/fluss-dist/target/fluss-1.0-SNAPSHOT-bin/fluss-1.0-SNAPSHOT"
        if [[ ! -d "${DIST_DIR}" ]]; then
            echo "Building fluss-dist (this populates target/fluss-1.0-SNAPSHOT-bin/...)"
            ( cd "${ROOT_DIR}" && JAVA_HOME=${JAVA_HOME:-} mvn -pl fluss-dist -am -DskipTests -Drat.skip=true install )
        fi
        BUILD_CTX="${ROOT_DIR}/docker/fluss"
        rm -rf "${BUILD_CTX}/build-target"
        cp -r "${DIST_DIR}" "${BUILD_CTX}/build-target"
        # Add the Kafka bolt-on. The upstream fluss-dist assembly doesn't include
        # fluss-kafka or its kafka-clients deps; drop them into lib/ so the
        # ServiceLoader picks up KafkaProtocolPlugin and kafka-clients is on the
        # classpath for the request decoder.
        KAFKA_JAR="${ROOT_DIR}/fluss-kafka/target/fluss-kafka-1.0-SNAPSHOT.jar"
        if [[ ! -f "${KAFKA_JAR}" ]]; then
            ( cd "${ROOT_DIR}" && JAVA_HOME=${JAVA_HOME:-} mvn -pl fluss-kafka -am -DskipTests -Drat.skip=true package )
        fi
        cp "${KAFKA_JAR}" "${BUILD_CTX}/build-target/lib/"
        cp "${HOME}/.m2/repository/org/apache/kafka/kafka-clients/3.9.2/kafka-clients-3.9.2.jar" "${BUILD_CTX}/build-target/lib/"
        cp "${HOME}/.m2/repository/com/github/luben/zstd-jni/1.5.6-4/zstd-jni-1.5.6-4.jar" "${BUILD_CTX}/build-target/lib/"
        cp "${HOME}/.m2/repository/at/yawk/lz4/lz4-java/1.10.1/lz4-java-1.10.1.jar" "${BUILD_CTX}/build-target/lib/"
        cp "${HOME}/.m2/repository/org/xerial/snappy/snappy-java/1.1.10.4/snappy-java-1.1.10.4.jar" "${BUILD_CTX}/build-target/lib/"
        # fluss-kafka also references CatalogService + ConnectionFactory — bundle the
        # fluss-catalog and fluss-client jars so all referenced types resolve.
        cp "${HOME}/.m2/repository/org/apache/fluss/fluss-catalog/1.0-SNAPSHOT/fluss-catalog-1.0-SNAPSHOT.jar" "${BUILD_CTX}/build-target/lib/"
        cp "${HOME}/.m2/repository/org/apache/fluss/fluss-client/1.0-SNAPSHOT/fluss-client-1.0-SNAPSHOT.jar" "${BUILD_CTX}/build-target/lib/"
        ( cd "${BUILD_CTX}" && "${ENGINE}" build -t "${IMAGE_TAG}" . )
        rm -rf "${BUILD_CTX}/build-target"
        ;;
    up)
        compose up -d
        echo
        echo "Cluster up. Kafka bootstrap: ${BOOTSTRAP}"
        echo "Tail logs:    $0 logs ts0"
        echo "Smoke test:   $0 smoke"
        ;;
    down)
        compose down -v
        ;;
    status)
        compose ps
        ;;
    logs)
        svc="${1:-}"
        if [[ -z "${svc}" ]]; then
            compose logs --tail=200
        else
            compose logs --tail=500 -f "${svc}"
        fi
        ;;
    kcat)
        compose run --rm --profile tools kcat kcat -b ts0:9092 "$@"
        ;;
    smoke)
        topic="fip-smoke-$RANDOM"
        echo "[smoke] create topic ${topic}"
        compose run --rm --profile tools kcat \
            kcat -b ts0:9092 -L -t "${topic}" >/dev/null 2>&1 || true
        echo "hello-from-podman" | compose run --rm -T --profile tools kcat \
            kcat -P -b ts0:9092 -t "${topic}"
        echo "[smoke] consume:"
        compose run --rm --profile tools kcat \
            kcat -C -b ts0:9092 -t "${topic}" -e
        ;;
    bootstrap)
        # Print the bootstrap servers; convenient for tooling scripts.
        echo "${BOOTSTRAP}"
        ;;
    tickbench)
        # Drive TickStream's tickbench protocol-conformance suite against the running
        # Fluss cluster. Requires a tickbench checkout at $TICKBENCH_HOME (defaults to
        # ~/tickstream/tickbench).
        TICKBENCH_HOME="${TICKBENCH_HOME:-${HOME}/tickstream/tickbench}"
        if [[ ! -x "${TICKBENCH_HOME}/bin/test_runner" ]]; then
            echo "error: tickbench/bin/test_runner not found at ${TICKBENCH_HOME}" >&2
            echo "Set TICKBENCH_HOME or clone TickStream first." >&2
            exit 2
        fi
        SERVER="${BOOTSTRAP%%,*}"
        # Default to 'functional': tickbench's Kafka 'compliance' selector covers EOS +
        # ACL/SCRAM/quotas admin which this FIP deliberately doesn't ship. 'functional'
        # exercises the drop-in surface (Produce/Fetch/groups/admin).
        CATEGORY="${1:-functional}"; shift || true
        echo "Running tickbench: protocol=kafka category=${CATEGORY} server=${SERVER}"
        "${TICKBENCH_HOME}/bin/test_runner" \
            -protocol kafka \
            -category "${CATEGORY}" \
            -server "${SERVER}" \
            "$@"
        ;;
    help|--help|-h|"")
        cat <<EOF
Fluss + Kafka FIP cluster control (engine=${ENGINE})

Commands:
  build               build the fluss/fluss:1.0-SNAPSHOT image from local repo
  up                  start the 5-container cluster
  down                stop and remove containers + volumes
  status              show container + health
  logs [svc]          tail logs (svc=ts0|ts1|ts2|coordinator|zookeeper)
  kcat <args...>      run kcat inside the cluster network with bootstrap pre-wired
  smoke               produce + consume a single record via kcat
  tickbench [cat]     run TickStream's tickbench protocol-conformance suite
                      (category defaults to 'compliance'; needs TICKBENCH_HOME)
  bootstrap           print the Kafka bootstrap servers string for the host
  help                this help

Engine: prefix with ENGINE=docker or pass --engine docker to use Docker.
EOF
        ;;
    *)
        echo "Unknown command: ${CMD}" >&2
        exec "$0" help
        ;;
esac
