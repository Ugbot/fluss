#!/usr/bin/env bash

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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


# Start/stop the standalone Fluss tiering service daemon.
#
# Unlike the coordinator-server / tablet-server, the tiering service is not
# configured from conf/server.yaml. Its configuration is supplied as
# "--key value" (or "key=value") command-line parameters that are forwarded
# verbatim to TieringServiceMain, for example:
#
#   ./bin/tiering-service.sh start \
#       --fluss.bootstrap.servers localhost:9123 \
#       --datalake.format iceberg \
#       --datalake.iceberg.warehouse /tmp/iceberg-warehouse
#
USAGE="Usage: $0 ((start|start-foreground) [args])|stop|stop-all"

STARTSTOP=$1

if [ -z $2 ] || [[ $2 == -* ]]; then
    # start|start-foreground [--key value ...]
    args=("${@:2}")
fi

if [[ $STARTSTOP != "start" ]] && [[ $STARTSTOP != "start-foreground" ]] && [[ $STARTSTOP != "stop" ]] && [[ $STARTSTOP != "stop-all" ]]; then
  echo $USAGE
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

SERVICE=tiering-service

if [[ $STARTSTOP == "start" ]] || [[ $STARTSTOP == "start-foreground" ]]; then
    # If the operator has not supplied their own JVM options (neither the shared
    # env.java.opts.all nor the tiering-service specific
    # env.java.opts.tiering-service), apply the Fluss defaults: a version-appropriate
    # default GC. To switch back to G1 (or any other GC), set env.java.opts.all /
    # env.java.opts.tiering-service in conf/server.yaml; those options take full
    # precedence over these defaults.
    if [ "${FLUSS_ENV_JAVA_OPTS_USER_SET}" != "true" ] && [ -z "${FLUSS_ENV_JAVA_OPTS_TIERING}" ]; then
        export JVM_ARGS="$JVM_ARGS $(constructDefaultJavaOpts "${FLUSS_ENV_JAVA_OPTS_TIERING}")"
    fi

    # Add tiering-service specific JVM options
    export FLUSS_ENV_JAVA_OPTS="${FLUSS_ENV_JAVA_OPTS} ${FLUSS_ENV_JAVA_OPTS_TIERING}"

    if [ ! -z "${DYNAMIC_PARAMETERS}" ]; then
        args=(${DYNAMIC_PARAMETERS[@]} "${args[@]}")
    fi
fi

if [[ $STARTSTOP == "start-foreground" ]]; then
    exec "${FLUSS_BIN_DIR}"/fluss-console.sh $SERVICE "${args[@]}"
else
    "${FLUSS_BIN_DIR}"/fluss-daemon.sh $STARTSTOP $SERVICE "${args[@]}"
fi
