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


constructFlussClassPath() {
    local FLUSS_SERVER
    local FLUSS_CLASSPATH

    while read -d '' -r jarfile ; do
        if [[ "$jarfile" =~ .*/fluss-server[^/]*.jar$ ]]; then
            FLUSS_SERVER="$FLUSS_SERVER":"$jarfile"
        elif [[ "$FLUSS_CLASSPATH" == "" ]]; then
            FLUSS_CLASSPATH="$jarfile";
        else
            FLUSS_CLASSPATH="$FLUSS_CLASSPATH":"$jarfile"
        fi
    done < <(find "$FLUSS_LIB_DIR" ! -type d -name '*.jar' -print0 | sort -z)

    # Add Hadoop dependencies from environment variables HADOOP_CLASSPATH
    if [ -n "${HADOOP_CLASSPATH}" ]; then
        FLUSS_CLASSPATH="$FLUSS_CLASSPATH":"$HADOOP_CLASSPATH"
    fi

    local FLUSS_SERVER_COUNT
    FLUSS_SERVER_COUNT="$(echo "$FLUSS_SERVER" | tr -s ':' '\n' | grep -v '^$' | wc -l)"

    # If fluss-server*.jar cannot be resolved write error messages to stderr since stdout is stored
    # as the classpath and exit function with empty classpath to force process failure
    if [[ "$FLUSS_SERVER" == "" ]]; then
        (>&2 echo "[ERROR] Fluss server jar not found in $FLUSS_LIB_DIR.")
        exit 1
    elif [[ "$FLUSS_SERVER_COUNT" -gt 1 ]]; then
        (>&2 echo "[ERROR] Multiple fluss-server*.jar found in $FLUSS_LIB_DIR. Please resolve.")
        exit 1
    fi

    echo "$FLUSS_CLASSPATH""$FLUSS_SERVER"
}

# These are used to mangle paths that are passed to java when using
# cygwin. Cygwin paths are like linux paths, i.e. /path/to/somewhere
# but the windows java version expects them in Windows Format, i.e. C:\bla\blub.
# "cygpath" can do the conversion.
manglePath() {
    UNAME=$(uname -s)
    if [ "${UNAME:0:6}" == "CYGWIN" ]; then
        echo `cygpath -w "$1"`
    else
        echo $1
    fi
}

manglePathList() {
    UNAME=$(uname -s)
    # a path list, for example a java classpath
    if [ "${UNAME:0:6}" == "CYGWIN" ]; then
        echo `cygpath -wp "$1"`
    else
        echo $1
    fi
}


# Looks up a config value by key from a simple YAML-style key-value map.
# $1: key to look up
# $2: default value to return if key does not exist
# $3: config file to read from
readFromConfig() {
    local key=$1
    local defaultValue=$2
    local configFile=$3

    # first extract the value with the given key (1st sed), then trim the result (2nd sed)
    # if a key exists multiple times, take the "last" one (tail)
    local value=`sed -n "s/^[ ]*${key}[ ]*: \([^#]*\).*$/\1/p" "${configFile}" | sed "s/^ *//;s/ *$//" | tail -n 1`

    [ -z "$value" ] && echo "$defaultValue" || echo "$value"
}

is_jdk_version_ge_17() {
  java_command=$1

  # get the java version using the java command
  java_version=$($java_command -version 2>&1 | grep 'version' | cut -d '"' -f 2)
  major_version=$(echo "$java_version" | cut -d. -f1)

  if [ "$major_version" -ge 17 ]; then
      return 0 # for true
  else
      return 1 # for false
  fi
}


# WARNING !!! , these values are only used if there is nothing else is specified in
# conf/server.yaml

DEFAULT_ENV_PID_DIR="/tmp"                          # Directory to store *.pid files to
DEFAULT_ENV_LOG_MAX=10                              # Maximum number of old log files to keep
DEFAULT_ENV_LOG_LEVEL="INFO"                        # Level of the root logger

DEFAULT_ENV_JAVA_OPTS=""                            # Optional JVM args
DEFAULT_ENV_JAVA_OPTS_CS=""                         # Optional JVM args (CoordinatorServer)
DEFAULT_ENV_JAVA_OPTS_TS=""                         # Optional JVM args (TabletServer)
DEFAULT_ENV_SSH_OPTS=""                             # Optional SSH parameters running in cluster mode


########################################################################################################################
# CONFIG KEYS: The default values can be overwritten by the following keys in conf/server.yaml
########################################################################################################################
KEY_ENV_PID_DIR="env.pid.dir"
KEY_ENV_LOG_DIR="env.log.dir"
KEY_ENV_LOG_MAX="env.log.max"
KEY_ENV_LOG_LEVEL="env.log.level"
KEY_ENV_STD_REDIRECT_TO_FILE="env.stdout-err.redirect-to-file"

KEY_ENV_JAVA_HOME="env.java.home"
KEY_ENV_JAVA_OPTS="env.java.opts.all"
KEY_ENV_JAVA_OPTS_CS="env.java.opts.coordinator-server"
KEY_ENV_JAVA_OPTS_TS="env.java.opts.tablet-server"
KEY_ENV_SSH_OPTS="env.ssh.opts"
KEY_ZK_HEAP_MB="zookeeper.heap.mb"

KEY_REMOTE_DATA_DIR="remote.data.dir"
KEY_SERVER_BUFFER_MEMORY_SIZE="server.buffer.memory-size"

########################################################################################################################
# PATHS AND CONFIG
########################################################################################################################

target="$0"
# For the case, the executable has been directly symlinked, figure out
# the correct bin path by following its symlink up to an upper bound.
# Note: we can't use the readlink utility here if we want to be POSIX
# compatible.
iteration=0
while [ -L "$target" ]; do
    if [ "$iteration" -gt 100 ]; then
        echo "Cannot resolve path: You have a cyclic symlink in $target."
        break
    fi
    ls=`ls -ld -- "$target"`
    target=`expr "$ls" : '.* -> \(.*\)$'`
    iteration=$((iteration + 1))
done

# Convert relative path to absolute path and resolve directory symlinks
bin=`dirname "$target"`
SYMLINK_RESOLVED_BIN=`cd "$bin"; pwd -P`

# Define the main directory of the fluss installation
if [ -z "$_FLUSS_HOME_DETERMINED" ]; then
    FLUSS_HOME=`dirname "$SYMLINK_RESOLVED_BIN"`
fi
if [ -z "$FLUSS_LIB_DIR" ]; then FLUSS_LIB_DIR=$FLUSS_HOME/lib; fi
if [ -z "$FLUSS_PLUGINS_DIR" ]; then FLUSS_PLUGINS_DIR=$FLUSS_HOME/plugins; fi


# These need to be mangled because they are directly passed to java.
# The above lib path is used by the shell script to retrieve jars in a
# directory, so it needs to be unmangled.
FLUSS_HOME_DIR_MANGLED=`manglePath "$FLUSS_HOME"`
if [ -z "$FLUSS_CONF_DIR" ]; then FLUSS_CONF_DIR=$FLUSS_HOME_DIR_MANGLED/conf; fi
FLUSS_BIN_DIR=$FLUSS_HOME_DIR_MANGLED/bin
FLUSS_OPT_DIR=$FLUSS_HOME_DIR_MANGLED/opt
DEFAULT_FLUSS_LOG_DIR=$FLUSS_HOME_DIR_MANGLED/log
FLUSS_CONF_FILE="server.yaml"
YAML_CONF=${FLUSS_CONF_DIR}/${FLUSS_CONF_FILE}


### Exported environment variables ###
export FLUSS_CONF_DIR
export FLUSS_BIN_DIR
export FLUSS_PLUGINS_DIR
export FLUSS_LIB_DIR
export FLUSS_OPT_DIR

########################################################################################################################
# ENVIRONMENT VARIABLES
########################################################################################################################

# read JAVA_HOME from config with no default value
MY_JAVA_HOME=$(readFromConfig ${KEY_ENV_JAVA_HOME} "" "${YAML_CONF}")
# check if config specified JAVA_HOME
if [ -z "${MY_JAVA_HOME}" ]; then
    # config did not specify JAVA_HOME. Use system JAVA_HOME
    MY_JAVA_HOME="${JAVA_HOME}"
fi
# check if we have a valid JAVA_HOME and if java is not available
if [ -z "${MY_JAVA_HOME}" ] && ! type java > /dev/null 2> /dev/null; then
    echo "Please specify JAVA_HOME. Either in Fluss config ./conf/server.yaml or as system-wide JAVA_HOME."
    exit 1
else
    JAVA_HOME="${MY_JAVA_HOME}"
fi


UNAME=$(uname -s)
if [ "${UNAME:0:6}" == "CYGWIN" ]; then
    JAVA_RUN=java
else
    if [[ -d "$JAVA_HOME" ]]; then
        JAVA_RUN="$JAVA_HOME"/bin/java
    else
        JAVA_RUN=java
    fi
fi


# Define HOSTNAME if it is not already set
if [ -z "${HOSTNAME}" ]; then
    HOSTNAME=`hostname`
fi

IS_NUMBER="^[0-9]+$"


if [ -z "${MAX_LOG_FILE_NUMBER}" ]; then
    MAX_LOG_FILE_NUMBER=$(readFromConfig ${KEY_ENV_LOG_MAX} ${DEFAULT_ENV_LOG_MAX} "${YAML_CONF}")
    export MAX_LOG_FILE_NUMBER
fi


if [ -z "${ROOT_LOG_LEVEL}" ]; then
    ROOT_LOG_LEVEL=$(readFromConfig ${KEY_ENV_LOG_LEVEL} "${DEFAULT_ENV_LOG_LEVEL}" "${YAML_CONF}")
    export ROOT_LOG_LEVEL
fi

if [ -z "${STD_REDIRECT_TO_FILE}" ]; then
    STD_REDIRECT_TO_FILE=$(readFromConfig ${KEY_ENV_STD_REDIRECT_TO_FILE} "false" "${YAML_CONF}")
fi

if [ -z "${FLUSS_LOG_DIR}" ]; then
    FLUSS_LOG_DIR=$(readFromConfig ${KEY_ENV_LOG_DIR} "${DEFAULT_FLUSS_LOG_DIR}" "${YAML_CONF}")
fi

if [ -z "${FLUSS_PID_DIR}" ]; then
    FLUSS_PID_DIR=$(readFromConfig ${KEY_ENV_PID_DIR} "${DEFAULT_ENV_PID_DIR}" "${YAML_CONF}")
fi

if [ -z "${FLUSS_ENV_JAVA_OPTS}" ]; then
    FLUSS_ENV_JAVA_OPTS=$(readFromConfig ${KEY_ENV_JAVA_OPTS} "" "${YAML_CONF}")

    # Record whether the operator actually supplied shared JVM options via
    # env.java.opts.all. We capture this BEFORE seeding the default flags below,
    # because the seeded "-XX:+IgnoreUnrecognizedVMOptions" would otherwise make
    # FLUSS_ENV_JAVA_OPTS look non-empty to the GC-default detection in the
    # server start scripts.
    if [ -n "$( echo "${FLUSS_ENV_JAVA_OPTS}" | sed -e 's/^"//' -e 's/"$//' )" ]; then
        FLUSS_ENV_JAVA_OPTS_USER_SET="true"
    else
        FLUSS_ENV_JAVA_OPTS_USER_SET="false"
    fi

    # Remove leading and ending double quotes (if present) of value
    FLUSS_ENV_JAVA_OPTS="-XX:+IgnoreUnrecognizedVMOptions $( echo "${FLUSS_ENV_JAVA_OPTS}" | sed -e 's/^"//'  -e 's/"$//' )"

    JAVA_SPEC_VERSION=`${JAVA_RUN} -XshowSettings:properties 2>&1 | grep "java.specification.version" | cut -d "=" -f 2 | tr -d '[:space:]' | rev | cut -d "." -f 1 | rev`
    if [[ $(( $JAVA_SPEC_VERSION > 17 )) == 1 ]]; then
      # set security manager property to allow calls to System.setSecurityManager() at runtime
      FLUSS_ENV_JAVA_OPTS="$FLUSS_ENV_JAVA_OPTS -Djava.security.manager=allow"
    fi
fi

if [ -z "${FLUSS_ENV_JAVA_OPTS_CS}" ]; then
    FLUSS_ENV_JAVA_OPTS_CS=$(readFromConfig ${KEY_ENV_JAVA_OPTS_CS} "${DEFAULT_ENV_JAVA_OPTS_CS}" "${YAML_CONF}")
    # Remove leading and ending double quotes (if present) of value
    FLUSS_ENV_JAVA_OPTS_CS="$( echo "${FLUSS_ENV_JAVA_OPTS_CS}" | sed -e 's/^"//'  -e 's/"$//' )"
fi

if [ -z "${FLUSS_ENV_JAVA_OPTS_TS}" ]; then
    FLUSS_ENV_JAVA_OPTS_TS=$(readFromConfig ${KEY_ENV_JAVA_OPTS_TS} "${DEFAULT_ENV_JAVA_OPTS_TS}" "${YAML_CONF}")
    # Remove leading and ending double quotes (if present) of value
    FLUSS_ENV_JAVA_OPTS_TS="$( echo "${FLUSS_ENV_JAVA_OPTS_TS}" | sed -e 's/^"//'  -e 's/"$//' )"
fi


if [ -z "${FLUSS_SSH_OPTS}" ]; then
    FLUSS_SSH_OPTS=$(readFromConfig ${KEY_ENV_SSH_OPTS} "${DEFAULT_ENV_SSH_OPTS}" "${YAML_CONF}")
fi

# Define ZK_HEAP if it is not already set
if [ -z "${ZK_HEAP}" ]; then
    ZK_HEAP=$(readFromConfig ${KEY_ZK_HEAP_MB} 0 "${YAML_CONF}")
fi

if [ -z "${REMOTE_DATA_DIR}" ]; then
    REMOTE_DATA_DIR=$(readFromConfig ${KEY_REMOTE_DATA_DIR} "" "${YAML_CONF}")
fi

# Arguments for the JVM. Used for Coordinator server and Tablet server JVMs.
if [ -z "${JVM_ARGS}" ]; then
    JVM_ARGS=""
fi

########################################################################################################################
# DEFAULT GARBAGE COLLECTOR
########################################################################################################################
#
# Generational ZGC is the default garbage collector for the Fluss servers. It is
# a low-latency, region-based collector that scales well with large heaps and the
# off-heap / direct-buffer heavy workloads (Arrow, write-ahead-log) that Fluss
# servers run. These defaults are ONLY applied when the operator has not supplied
# their own JVM options via the env.java.opts.* keys (or the corresponding
# FLUSS_ENV_JAVA_OPTS* environment variables); any user-provided options take full
# precedence so the GC can always be overridden.
#
# To switch back to the G1 collector, set env.java.opts.all (or the per-role
# env.java.opts.coordinator-server / env.java.opts.tablet-server) in
# conf/server.yaml, e.g.:
#
#     env.java.opts.all: "-XX:+UseG1GC"
#
# The commented-out line below is the previous G1 default, kept for reference:
#
#     DEFAULT_GC_OPTS="-XX:+UseG1GC"
#
DEFAULT_GC_OPTS="-XX:+UseZGC -XX:+ZGenerational"

# If FLUSS_ENV_JAVA_OPTS was already exported in the environment (so the
# env.java.opts.all read above was skipped), treat that as operator-supplied JVM
# options and do NOT apply the Fluss GC defaults.
if [ -z "${FLUSS_ENV_JAVA_OPTS_USER_SET}" ]; then
    FLUSS_ENV_JAVA_OPTS_USER_SET="true"
fi

# Returns the default JVM options Fluss applies when the operator has NOT set any
# of their own JVM options. This selects Generational ZGC and, when the
# configured server buffer memory size is discoverable from conf/server.yaml,
# wires a sensible -XX:MaxDirectMemorySize so the direct-buffer pool can hold the
# server's write buffers (Arrow / write-ahead-log) plus headroom.
#
# $1: optional, the value of the role-specific FLUSS_ENV_JAVA_OPTS (CS or TS).
#     The caller passes this so we only apply defaults when BOTH the shared and
#     the role-specific opts are empty.
constructDefaultJavaOpts() {
    local role_opts="$1"

    # Only apply defaults when the operator has supplied no JVM options at all.
    # FLUSS_ENV_JAVA_OPTS is seeded with "-XX:+IgnoreUnrecognizedVMOptions" (and
    # possibly a security-manager flag) even when env.java.opts.all is empty, so
    # we cannot test it directly here; the caller is responsible for detecting
    # whether the user actually configured env.java.opts.all and only invoking
    # this helper when they did not.
    local default_opts="${DEFAULT_GC_OPTS}"

    # Derive a default -XX:MaxDirectMemorySize from server.buffer.memory-size when
    # it is configured as a plain "<number><unit>" value (e.g. "256mb", "2gb").
    # We give the direct-buffer pool the configured buffer size plus 64mb of
    # headroom for transient Arrow / Netty allocations. If the value is missing or
    # not in a form we can safely parse, we leave -XX:MaxDirectMemorySize unset and
    # let the JVM default apply.
    local buffer_size
    buffer_size=$(readFromConfig ${KEY_SERVER_BUFFER_MEMORY_SIZE} "" "${YAML_CONF}")
    local direct_mem_mb
    direct_mem_mb=$(directMemoryMbFromBufferSize "${buffer_size}")
    if [ -n "${direct_mem_mb}" ]; then
        default_opts="${default_opts} -XX:MaxDirectMemorySize=${direct_mem_mb}m"
    fi

    echo "${default_opts}"
}

# Converts a Fluss MemorySize string (e.g. "256mb", "2 gb", "1073741824b",
# "512kb") into an integer number of megabytes plus 64mb of headroom, suitable for
# -XX:MaxDirectMemorySize. Echoes nothing when the input is empty or cannot be
# parsed, so callers can simply test for an empty result.
directMemoryMbFromBufferSize() {
    local raw="$1"

    # Strip whitespace and lower-case the unit so "256 MB" and "256mb" both work.
    raw=$(echo "${raw}" | tr '[:upper:]' '[:lower:]' | tr -d '[:space:]')
    if [ -z "${raw}" ]; then
        return 0
    fi

    # Split into the numeric part and the (optional) unit suffix.
    local num
    local unit
    num=$(echo "${raw}" | sed -n 's/^\([0-9][0-9]*\).*$/\1/p')
    unit=$(echo "${raw}" | sed -n 's/^[0-9][0-9]*\(.*\)$/\1/p')

    # Bail out if there was no leading number (unparseable value).
    if [ -z "${num}" ]; then
        return 0
    fi

    local bytes
    case "${unit}" in
        ""|"b"|"bytes")
            bytes=${num}
            ;;
        "k"|"kb"|"kibibytes")
            bytes=$((num * 1024))
            ;;
        "m"|"mb"|"mebibytes")
            bytes=$((num * 1024 * 1024))
            ;;
        "g"|"gb"|"gibibytes")
            bytes=$((num * 1024 * 1024 * 1024))
            ;;
        "t"|"tb"|"tebibytes")
            bytes=$((num * 1024 * 1024 * 1024 * 1024))
            ;;
        *)
            # Unknown unit; do not guess.
            return 0
            ;;
    esac

    # Convert to MB (rounding up) and add 64mb of headroom.
    local mb
    mb=$(((bytes + 1024 * 1024 - 1) / (1024 * 1024)))
    echo $((mb + 64))
}
