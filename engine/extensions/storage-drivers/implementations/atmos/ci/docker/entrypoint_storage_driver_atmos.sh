#!/bin/sh
umask 0000
export JAVA_HOME=/opt/spt
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:${JAVA_HOME}/bin
java -jar /opt/spt/spt.jar --storage-driver-type=atmos --storage-net-node-port=9020 "$@"
