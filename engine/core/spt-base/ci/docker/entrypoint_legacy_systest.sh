#!/bin/sh
umask 0000
./gradlew clean systemTest --tests com.dell.spt.system.${TEST}
