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
http://repo.maven.apache.org/maven2/io/github/dell/spt/spt-storage-driver-fs/
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
