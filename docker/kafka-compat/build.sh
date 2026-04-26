#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
BUILD_TARGET="${SCRIPT_DIR}/build-target"

echo "==> Building Fluss dist (skipping tests)..."
cd "${REPO_ROOT}"
JAVA_HOME="${JAVA_HOME:-${HOME}/.sdkman/candidates/java/11.0.25-tem}" \
  mvn clean install -DskipTests -pl fluss-dist --also-make -T 1C -q

echo "==> Extracting dist to ${BUILD_TARGET}..."
rm -rf "${BUILD_TARGET}"
mkdir -p "${BUILD_TARGET}"
TARBALL="$(ls "${REPO_ROOT}/fluss-dist/target/fluss-"*"-bin.tgz" | head -1)"
tar -xzf "${TARBALL}" -C "${BUILD_TARGET}" --strip-components=1

echo "==> Building Podman image fluss-kafka:latest..."
cd "${SCRIPT_DIR}"
podman build -t fluss-kafka:latest -f Containerfile .

echo ""
echo "Done. Run the stack with:"
echo "  podman compose up -d"
echo ""
echo "Kafka bootstrap: localhost:9092"
echo "Schema Registry: http://localhost:8081"
