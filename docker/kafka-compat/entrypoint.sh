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

set -e

CONF_FILE="${FLUSS_HOME}/conf/server.yaml"

# Append any extra properties injected via FLUSS_PROPERTIES env var.
if [ -n "${FLUSS_PROPERTIES}" ]; then
    echo "${FLUSS_PROPERTIES}" >> "${CONF_FILE}"
fi

# Substitute environment variables (e.g. KAFKA_ADVERTISED_HOST) into the config.
envsubst < "${CONF_FILE}" > "${CONF_FILE}.tmp" && mv "${CONF_FILE}.tmp" "${CONF_FILE}"

case "$1" in
  coordinatorServer)
    shift
    echo "Starting Coordinator Server"
    exec "$FLUSS_HOME/bin/coordinator-server.sh" start-foreground "$@"
    ;;
  tabletServer)
    shift
    echo "Starting Tablet Server"
    exec "$FLUSS_HOME/bin/tablet-server.sh" start-foreground "$@"
    ;;
  help)
    echo "Usage: $(basename "$0") (coordinatorServer|tabletServer)"
    exit 0
    ;;
  *)
    exec "$@"
    ;;
esac
