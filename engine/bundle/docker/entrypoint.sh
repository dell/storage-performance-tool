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
# Use parameter expansion to handle unset JAVA_OPTS safely
JAVA_OPTS="${JAVA_OPTS:-}"
if [ -z "$JAVA_OPTS" ]; then
    # Default Java options for Spt
    # - Use G1GC for better latency
    # - Set reasonable heap size
    # - Enable detailed GC logging for performance analysis
    export JAVA_OPTS="-XX:+UseG1GC -Xms1g -Xmx4g -XX:MaxGCPauseMillis=100"
fi

# Additional Java options that can be set via environment variables
# Use parameter expansion for safe handling of unset variables
SPT_JAVA_OPTS="${SPT_JAVA_OPTS:-}"
if [ -n "$SPT_JAVA_OPTS" ]; then
    export JAVA_OPTS="$JAVA_OPTS $SPT_JAVA_OPTS"
fi

# Ensure the spt user owns the home directory
# This helps with scenarios where volumes are mounted
if [ -w "/home/spt" ]; then
    # Only try to change ownership if we have write permission
    # This will fail if running as non-root, which is fine
    chown -R spt:spt /home/spt/.spt 2>/dev/null || true
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