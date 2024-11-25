#!/usr/bin/env bash
#
# Copyright (c) 2024 Alibaba Group Holding Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

if [ -z $1 ] || [[ $1 == "-D" ]]; then
    # [-D ...]
    args=("${@:1}")
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

FLUSS_LOG_PREFIX="${FLUSS_LOG_DIR}/fluss-lakehouse-${HOSTNAME}"
log="${FLUSS_LOG_PREFIX}.log"
out="${FLUSS_LOG_PREFIX}.out"
err="${FLUSS_LOG_PREFIX}.err"

log_setting=("-Dlog.file=${log}" "-Dlog4j.configuration=file:${FLUSS_CONF_DIR}/log4j-console.properties" "-Dlog4j.configurationFile=file:${FLUSS_CONF_DIR}/log4j-console.properties" "-Dlogback.configurationFile=file:${FLUSS_CONF_DIR}/logback-console.xml")

echo "Starting lakehouse tiering service"

LAKEHOUSE_SERVICE_CLASS_TO_RUN=com.alibaba.fluss.lakehouse.cli.FlussLakehouseCli
LAKEHOUSE_SERVICE_CLASSPATH=`findLakehouseCliJar`

LAKEHOUSE_PAIMON_JAR=`findLakehousePaimonJar`

args=("run" "${LAKEHOUSE_PAIMON_JAR}" "--configDir" "${FLUSS_CONF_DIR}" "${args[@]}")

FLUSS_LOG_CLASSPATH=`constructLogClassClassPath`

"$JAVA_RUN" "${log_setting[@]}" -classpath "`manglePathList "$FLUSS_LOG_CLASSPATH:$LAKEHOUSE_SERVICE_CLASSPATH"`" ${LAKEHOUSE_SERVICE_CLASS_TO_RUN} "${args[@]}"
