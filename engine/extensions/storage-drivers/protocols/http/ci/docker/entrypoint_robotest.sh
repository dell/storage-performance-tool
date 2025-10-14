#!/bin/sh
umask 0000
ls -l /root/spt/src/test/robot/api/storage
robot --outputdir /root/spt/build/robotest --suite ${SUITE} --include ${TEST} /root/spt/src/test/robot
rebot /root/spt/build/robotest/output.xml
