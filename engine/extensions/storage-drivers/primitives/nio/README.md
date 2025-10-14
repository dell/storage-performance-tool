[![Gitter chat](https://badges.gitter.im/dell-spt.png)](https://gitter.im/dell-spt)
[![Issue Tracker](https://img.shields.io/badge/Issue-Tracker-red.svg)](https://spt-issues.atlassian.net/projects/GOOSE)
[![CI status](https://gitlab.com/dell-spt/spt-storage-driver-nio/badges/master/pipeline.svg)](https://gitlab.com/dell-spt/spt-storage-driver-nio/commits/master)
[![Tag](https://img.shields.io/github/tag/dell-spt/spt-storage-driver-nio.svg)](https://github.com/dell-spt/spt-storage-driver-nio/tags)
[![Maven metadata URL](https://img.shields.io/maven-metadata/v/http/central.maven.org/maven2/com/github/dell-spt/spt-storage-driver-nio/maven-metadata.xml.svg)](http://central.maven.org/maven2/com/github/dell-spt/spt-storage-driver-nio)
[![Sonatype Nexus (Releases)](https://img.shields.io/nexus/r/http/oss.sonatype.org/com.github.dell-spt/spt-storage-driver-nio.svg)](http://oss.sonatype.org/com.github.dell-spt/spt-storage-driver-nio)
[![Docker Pulls](https://img.shields.io/docker/pulls/dellspt/spt-storage-driver-nio.svg)](https://hub.docker.com/r/dellspt/spt-storage-driver-nio/)

# NIO Storage Driver

The count of the concurrent load operations at is limited also by the `storage-driver-threads` configuration option
which is CPU core count by default. This means that operations will occupy no more I/O worker threads than configured.
However the operations are reentrant and the count of the active (started but not completed yet) operations may be much
more(may be limited by `storage-driver-limit-concurrency` configuration option).
