[![Gitter chat](https://badges.gitter.im/dell-spt.png)](https://gitter.im/dell-spt)
[![Issue Tracker](https://img.shields.io/badge/Issue-Tracker-red.svg)](https://spt-issues.atlassian.net/projects/GOOSE)
[![CI status](https://gitlab.com/dell-spt/spt-storage-driver-fs/badges/master/pipeline.svg)](https://gitlab.com/dell-spt/spt-storage-driver-fs/commits/master)
[![Maven metadata URL](https://img.shields.io/maven-metadata/v/http/central.maven.org/maven2/com/github/dell-spt/spt-storage-driver-fs/maven-metadata.xml.svg)](http://central.maven.org/maven2/com/github/dell-spt/spt-storage-driver-fs)
[![Docker Pulls](https://img.shields.io/docker/pulls/dellspt/spt-storage-driver-fs.svg)](https://hub.docker.com/r/dellspt/spt-storage-driver-fs/)

# Introduction

The storage driver implementation may be used to perform file non-blocking I/O. Typically used totest CIFS/HDFS/NFS
shares mounted locally. However, testing distributed filesystems via the mounted shares may be not accurate due to
additional VFS layer. The measured rates may be:
* Inadequately low due to frequent system calls
* Higher than network bandwidth due to local caching by VFS

# Features

* Authentification: N/A
* Item types: `data` only (--> "file")
* Path listing input
* Automatic destination path creation on demand
* Data item operation types:
    * `create`, additional modes:
        * [copy](../../../../../doc/design/copy_mode/README.md)
    * `read`
        * full
        * random byte ranges
        * fixed byte ranges
        * content verification
    * `update`
        * full (overwrite)
        * random byte ranges
        * fixed byte ranges (with append mode)
    * `delete`
    * `noop`

# Usage

Get the latest pre-built jar file which is available at:
http://repo.maven.apache.org/maven2/com/github/dell-spt/spt-storage-driver-fs/
The jar file may be downloaded manually and placed into the `<USER_HOME_DIR>/.spt/<VERSION>/ext`
directory of Spt to be automatically loaded into the runtime.

```bash
java -jar spt-<VERSION>.jar \
    --storage-driver-type=fs \
    --storage-net-node-addrs=<NODE_IP_ADDRS> \
    --storage-net-node-port=<NODE_PORT> \
    ...
```

## Standalone

```bash
java -jar spt-<VERSION>.jar \
    --storage-driver-type=fs \
    ...
```
