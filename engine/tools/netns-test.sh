#!/bin/bash
# Runs the s3 driver's network-namespace tests (tag netns) on this host without root. An
# unprivileged user namespace gives the tests their own network namespace with NET_ADMIN and
# SYS_ADMIN over it and a sysfs that shows it; CAP_NET_RAW is dropped, as a non-root process
# lacks it. Needs unprivileged user namespaces, iproute2, util-linux (unshare, nsenter, setpriv)
# and JDK 21. CI runs the netnsTest task instead, as root in a container with both capabilities.
#
# Gradle stays outside the namespace, since Gradle processes negotiate their shared cache locks
# over the host's loopback: it compiles the tests and prints their class path, and one JVM runs
# them inside through NetnsTestMain.
#
# Usage: tools/netns-test.sh [test method names of S3NicSetNamespaceTest]
set -euo pipefail

cd "$(dirname "$0")/.."
classpath=$(./gradlew -q :extensions:storage-drivers:implementations:s3:printNetnsTestClasspath | tail -n 1)
java=${JAVA_HOME:+$JAVA_HOME/bin/}java

exec unshare --user --map-root-user --net --mount --propagation private sh -c '
	mount -t sysfs sysfs /sys
	ip link set lo up
	java=$0
	classpath=$1
	shift
	exec setpriv --bounding-set=-net_raw --inh-caps=-net_raw "$java" -Xmx2g -XX:MaxDirectMemorySize=2g \
		-cp "$classpath" com.dell.spt.storage.driver.coop.netty.http.s3.NetnsTestMain "$@"
' "$java" "$classpath" "$@"
