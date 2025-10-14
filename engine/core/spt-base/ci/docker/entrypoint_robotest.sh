#!/bin/sh
umask 0000
robot --outputdir /root/spt/build/robotest --suite ${SUITE} --include ${TEST} /root/spt/src/test/robot
rebot /root/spt/base/robotest/output.xml
