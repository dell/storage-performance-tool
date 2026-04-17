#!/bin/sh
#
# Spt Bundle Docker Entrypoint
#
# This script serves as the entrypoint for the Spt Docker container.
# It sets up the environment and launches Spt with the provided arguments.
#

# Strict error handling:
# -e: Exit on any error
# -u: Treat unset variables as errors
# -o pipefail is not available in /bin/sh, but we avoid pipes in critical paths
set -eu

# Set Java options with safe defaults if not already set
# Default Java options for Spt (JDK 25 LTS Docker image)
# - ZGC: sub-millisecond GC pauses; avoids GC jitter in latency measurements
# - Xmx 4g: bounded heap for Java objects (GC works best with a defined ceiling)
# - MaxDirectMemorySize 64g: separate ceiling for off-heap ByteBuffers (RDMA/NIO);
#   allocated on demand so this limit is safe even on smaller machines
# - AlwaysPreTouch: pre-fault heap pages at startup to eliminate page faults during runs
# - UseNUMA: NUMA-aware allocation for multi-socket servers
# - Xshare:off: disable CDS to prevent SIGBUS when JDK/JAR resides on NFS
# - UseAVX=2: avoid AVX-512 EVEX glibc code paths with known SIGBUS crashes
# - DoJVMTIVirtualThreadTransitions: skip JVMTI mount/unmount notifications on every
#   VirtualThread context switch; reduces per-op overhead when no debugging agent is attached
# - UseCompactObjectHeaders (JEP 519): 64-bit object headers instead of 128-bit;
#   reduces heap footprint and GC pressure for high-rate short-lived objects
DEFAULT_JAVA_OPTS="-XX:+UseZGC -Xms1g -Xmx4g -XX:MaxDirectMemorySize=64g -XX:+AlwaysPreTouch -XX:+UseNUMA -Xshare:off -XX:UseAVX=2 -XX:+UnlockExperimentalVMOptions -XX:-DoJVMTIVirtualThreadTransitions -XX:+UseCompactObjectHeaders"

# Append any user-provided JAVA_OPTS (like RMI hostname) to the defaults.
# If a user provides conflicting flags (like -Xmx8g), Java will use the last one specified.
export JAVA_OPTS="$DEFAULT_JAVA_OPTS ${JAVA_OPTS:-}"

# Additional Java options that can be set via environment variables
# Use parameter expansion for safe handling of unset variables
SPT_JAVA_OPTS="${SPT_JAVA_OPTS:-}"
if [ -n "$SPT_JAVA_OPTS" ]; then
    export JAVA_OPTS="$JAVA_OPTS $SPT_JAVA_OPTS"
fi

# If no arguments provided, show help
if [ $# -eq 0 ]; then
    set -- --help
fi

# Check if user wants to run a shell instead of Spt
if [ "$1" = "sh" ] || [ "$1" = "bash" ] || [ "$1" = "/bin/sh" ] || [ "$1" = "/bin/bash" ]; then
    exec "$@"
fi

# Launch Spt with all provided arguments
# The default storage driver is set to s3 (instead of dummy-mock) as it's more commonly used
# Users can override this with --storage-driver-type parameter
exec java $JAVA_OPTS -jar /opt/spt/spt.jar "$@"