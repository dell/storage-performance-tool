# S3 Storage Driver

Spt storage driver extention for testing of **S3 type storages**. The repo contains only the extension source code, the source code of the spt core and the full spt documentation is contained in the [`spt-base` repository](https://github.com/dell/storage-performance-tool).

# Content

1. [Features](#1-features)<br/>
2. [Deployment](#2-deployment)<br/>
        2.1. [Jar](#21-jar)<br/>
        2.2. [Docker](#22-docker)<br/>
3. [Configuration Reference](#3-configuration-reference)<br/>
        3.1. [S3 Specific Options](#31-s3-specific-options)<br/>
        3.2. [Other Options](#32-other-options)<br/>
4. [Usage](#4-usage)<br/>
    4.1. [Main functionality](#41-main-functionality)<br/>
    4.1. [HTTP functionality](#41-http-functionality)<br/>
    4.2. [Object Tagging](#42-object-tagging)<br/>
    4.3.  [Versioning](#43-versioning)>/br>
5. [Minio S3 server](#5-minio-s3-server)<br/>

## 1. Features

* API version: 2006-03-01
* Authentification:
    * [v2](https://docs.aws.amazon.com/general/latest/gr/signature-version-2.html) (by default)
    * [v4](https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html)
* SSL/TLS
* Item types:
    * `data` (--> "object")
    * `path` (--> "bucket")
* Automatic destination path creation on demand
* Path listing input (with XML response payload)
* Data item operation types:
    * `create`
        * [copy](https://github.com/dell/storage-performance-tool/tree/master/doc/usage/load/operations/types#12-copy-mode)
        * [Multipart Upload](https://github.com/dell/storage-performance-tool/tree/master/doc/usage/load/operations/composite)
    * `read`
        * full
        * random byte ranges
        * fixed byte ranges
        * content verification
        * [object tagging](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html)
    * `update`
        * full (overwrite)
        * random byte ranges
        * fixed byte ranges (with append mode)
        * [object tagging](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectTagging.html)
    * `delete`
        * full
        * [object tagging](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjectTagging.html)
    * `noop`
* Path item operation types:
    * `create`
    * `read`
    * `delete`
    * `noop`

## 2. Deployment

## 2.1. Jar

Java 21+ is required to build/run.

1. Get the latest `spt-base` jar from the 
[maven repo](https://repo.maven.apache.org/maven2/io/github/dell/spt/spt-base/)
and put it to your working directory. Note the particular version, which is referred as *BASE_VERSION* below.

2. Get the latest `spt-storage-driver-coop` jar from the
[maven repo](https://repo.maven.apache.org/maven2/io/github/dell/spt/spt-storage-driver-coop/)
and put it to the `~/.spt/<BASE_VERSION>/ext` directory.

3. Get the latest `spt-storage-driver-netty` jar from the
[maven repo](https://repo.maven.apache.org/maven2/io/github/dell/spt/spt-storage-driver-netty/)
and put it to the `~/.spt/<BASE_VERSION>/ext` directory.

4. Get the latest `spt-storage-driver-http` jar from the
[maven repo](https://repo.maven.apache.org/maven2/io/github/dell/spt/spt-storage-driver-http/)
and put it to the `~/.spt/<BASE_VERSION>/ext` directory.

5. Get the latest `spt-storage-driver-s3` jar from the
[maven repo](https://repo.maven.apache.org/maven2/io/github/dell/spt/spt-storage-driver-s3/)
and put it to the `~/.spt/<BASE_VERSION>/ext` directory.

```bash
java -jar spt-base-<BASE_VERSION>.jar \
    --storage-driver-type=s3 \
    [<SPT CLI ARGS>]
```
## 2.2. Docker

[More deployment examples](https://github.com/dell/storage-performance-tool/tree/master/doc/deployment)

> NOTE: The base image doesn't contain any additonal load step types neither additional storage drivers. 

### 2.2.1. Standalone

Example:
```bash
docker run \
    --network host \
    dcr.octo.dell.com/spt/spt-storage-driver-s3 \
    --storage-net-node-addrs=<NODE_IP_ADDRS> \
    [<SPT CLI ARGS>]
```

### 2.2.2. Distributed

#### 2.2.2.1. Additional Node

Example:
```bash
docker run \
    --network host \
    dcr.octo.dell.com/spt/spt-storage-driver-s3 \
    --run-node
```

> NOTE: Spt uses `1099` port for RMI between spt nodes and `9999` for REST API. If you run several spt nodes on the same host (in different docker containers, for example) or if the ports are used by another service, then ports can be redefined:
> ```bash
> docker run \
>    --network host \
>    dcr.octo.dell.com/spt/spt-storage-driver-s3 \
>    --run-node \
>    --load-step-node-port=<RMI PORT> \
>    --run-port=<REST PORT> 
> ```

#### 2.2.2.2. Entry Node

Example:
```bash
docker run \
    --network host \
    dcr.octo.dell.com/spt/spt-storage-driver-s3 \
    --load-step-node-addrs=<ADDR1,ADDR2,...> \
    --storage-net-node-addrs=<NODE_IP_ADDRS> \
    [<SPT CLI ARGS>]
```

## 3. Configuration Reference

### 3.1. S3 Specific Options

| Name                                           | Type         | Default Value    | Description                                      |
|:-----------------------------------------------|:-------------|:-----------------|:-------------------------------------------------|
| storage-auth-version                           | Int  | 2     | Specifies which auth version to use. Valid values: 2, 4.
| storage-object-fsAccess                        | Flag | false | Specifies whether filesystem access is enabled or not
| storage-object-tagging-enabled                 | Flag | false | Work (PUT/GET/DELETE) with object tagging or not (default)
| storage-object-tagging-tags                    | Map  | {}    | Map of name-value tags, effective only for the `UPDATE` operation when tagging is enabled
| storage-object-versioning                      | Flag | false | Specifies whether the versioning storage feature is used or not
| storage-checksum-enabled                       | Flag | false | Pass checksum to server on PUT?
| storage-checksum-algorithm                     | String | md5 | S3 checksum algorithm: [md5, crc32, crc32c, sha1, sha256]

### 3.2. Other Options

* A **bucket** may be specified with either `item-input-path` or `item-output-path` configuration option
* Multipart upload should be enabled using the `item-data-ranges-threshold` configuration option
* The default storage port is set to 9020 for the docker image

## 4. Usage

### 4.1. Main functionality

[Examples of spt core usage](https://github.com/dell/storage-performance-tool/tree/master/doc/getstarted)

### 4.1. HTTP functionality

> NOTE: Spt S3 SD depends on Spt HTTP SD, and the S3 bundle includes all the features of HTTP SD, so all http-specific parameters can be also used with this S3 driver.

[Examples of HTTP headers usage](https://github.com/dell/storage-performance-tool)

### 4.2. Object Tagging

https://docs.aws.amazon.com/AmazonS3/latest/dev/object-tagging.html

#### 4.2.1. Put Object Tags

https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectTagging.html

Put (create or replace) the tags on the existing objects. The `update` load operation should be used for this. 
The objects should be specified by an 
[item input](https://github.com/dell/storage-performance-tool/tree/master/doc/usage/item/input#items-input) 
(the bucket listing or the items input CSV file). 

Scenario example:
```javascript
var updateTaggingConfig = {
    "storage" : {
        "object" : {
            "tagging" : {
                "enabled" : true,
                "tags" : {
                    "tag0" : "value_0",
                    "tag1" : "value_1",
                    // ...
                    "tagN" : "value_N"
                }
            }
        }
    }
};

UpdateLoad
    .config(updateTaggingConfig)
    .run();
```

Command line example:
```bash
docker run \
    --network host \
    dcr.octo.dell.com/spt/spt-storage-driver-s3 \
    --storage-auth-uid=user1 \ 
    --storage-auth-secret=**************************************** \
    --item-input-file=objects_to_update_tagging.csv \
    --item-output-path=/bucket1 \
    --storage-net-transport=nio \
    --run-scenario=tagging.js
```

***Note***:
> It's not possible to use the command line to specify the tag set, a user should use the scenario file for this

##### 4.2.1.1. Tags Expressions

Both tag names and values support the 
[expression language](https://github.com/dell/storage-performance-tool/blob/master/src/main/java/com/dell/spt/base/config/el/README.md):

Example:
```javascript
var updateTaggingConfig = {
    "storage" : {
        "object" : {
            "tagging" : {
                "enabled" : true,
                "tags" : {
                    "foo${rnd.nextInt()}" : "bar${time:millisSinceEpoch()}",
                    "key1" : "${date:formatNowIso8601()}",
                    "${e}" : "${pi}"
                }
            }
        }
    }
};

UpdateLoad
    .config(updateTaggingConfig)
    .run();
```

#### 4.2.2. Get Object Tags

https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html

Example:
```bash
docker run \
    --network host \
    dcr.octo.dell.com/spt/spt-storage-driver-s3 \
    --storage-net-node-addrs=<NODE_IP_ADDRS> \
    --read \
    --item-input-path=/bucket1 \
    --storage-object-tagging-enabled \
    [<SPT CLI ARGS>]
```

#### 4.2.3. Delete Object Tags

https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjectTagging.html

```bash
docker run \
    --network host \
    dcr.octo.dell.com/spt/spt-storage-driver-s3 \
    --storage-net-node-addrs=<NODE_IP_ADDRS> \
    --delete \
    --item-input-file=objects_to_delete_tagging.csv \
    --storage-object-tagging-enabled \
    [<SPT CLI ARGS>]
```

### 4.3. Versioning

[What versioning is?](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html)

Create request is the only versioning operation that doesn't require version-id by default. It just creates a new
version for the same object. We can only retrieve the version-ids by specifying `item-input-file`. To create such file 
make sure to enable `--storage-object-versioning` flag and specify `item-output-file` path. 

#### 4.3.1. PUT versions

There are two approaches to do load testing with versioning. But first stage is common:

```bash
docker run \
    --network host \
    dcr.octo.dell.com/spt/spt-storage-driver-s3 \
    --storage-auth-uid=user1 \ 
    --storage-auth-secret=**************************************** \
    --storage-net-node-addrs=<NODE_IP_ADDRS> \
    --item-output-file=itemsInitialList.csv \
    --storage-object-versioning=true \
    --item-output-path=/bucket \
    --load-op-limit-count=<N>
```

We create an `itemsInitialList.csv` that has objects that we are going to version. Next step is different for the 
two approaches.

#### 4.3.1.1. Recycle mode

First approach is to use `--load-op-recycle` mode. We pass the list of objects to version and specify the 
`limit-count=<N*M>` where `N` - is the length of the intial list and `M` is the amount of versions per object. Be aware
that `recycle-mode` doesn't guarantee the exact amount of versions per object. But the average amount will be `M`.

```bash
docker run \
    --network host \
    dcr.octo.dell.com/spt/spt-storage-driver-s3 \
    --storage-auth-uid=user1 \ 
    --storage-auth-secret=**************************************** \
    --storage-net-node-addrs=<NODE_IP_ADDRS> \
    --item-input-file=itemsInitialList.csv \
    --storage-object-versioning=true \
    --load-op-limit-count=<N*M>
```

There is a constraint. You need to have a large enough input-file so that Spt always has work to do in order to get
max throughput. Because it can't send a new request on the same item until it's acked. And if we consider let's say 100ms
latency for a small object then Spt should have enough objects to process during those 100ms to not stay idle. 

To determine that amount check your latency and throughput when doing regular s3 PUTs. If you do 50000 op/s and your 
latency is 100ms then the initial list must be 5000 objects.

As a side-note: if you want to get a list of items for this test (e.g. to pass it to read test) make sure to 
enable `load-op-output-duplicates` flag as by default spt doesn't print the duplicates created by recycle mode.

#### 4.3.1.2. Long input file

Another approach requires using command line tools after the common step but guarantees the exact amount of versions per 
object. Instead of recycling object we can provide Spt a list of objects which would already have copies of the 
same object. This can be achieved for example via: 

```bash
for i in {1..1000}; do cat itemsInitialList.csv; done > itemsWithVersionsList.csv
```

```bash
docker run \
    --network host \
    dcr.octo.dell.com/spt/spt-storage-driver-s3 \
    --storage-auth-uid=user1 \ 
    --storage-auth-secret=**************************************** \
    --storage-net-node-addrs=<NODE_IP_ADDRS> \
    --item-input-file=itemsWithVersionsList.csv \
    --storage-object-versioning=true 
```


#### 4.3.2. GET versions

Unlike PUT operations, GETs are simple. You need to have an `input-file` with versions like this generated by 
PUT load:

```
/bucket/97bdgavrlkp0~161458,10cf8e8ba060d304,100,0/0
/bucket/gx8zqoy6fvtd~161494,1ee9b7ffddbddcd1,100,0/0
/bucket/vs71f8k3cnx9~161504,3a0e47edc025f71d,100,0/0
/bucket/hg9ai03dthjm~161516,1fe09f7b49e09712,100,0/0
/bucket/m3stkc24nmdp~161517,2860e144acfb5d0d,100,0/0
```

```bash
docker run \
    --network host \
    dcr.octo.dell.com/spt/spt-storage-driver-s3 \
    --storage-auth-uid=user1 \ 
    --load-op-type=read \
    --storage-auth-secret=**************************************** \
    --storage-net-node-addrs=<NODE_IP_ADDRS> \
    --item-input-file=itemsWithVersionsList.csv \
    --storage-object-versioning=true
```

#### 4.3.3. DELETE versions

DELETEs are also simple. An `input-file` is again required.

```bash
docker run \
    --network host \
    dcr.octo.dell.com/spt/spt-storage-driver-s3 \
    --storage-auth-uid=user1 \ 
    --load-op-type=delete \
    --storage-auth-secret=**************************************** \
    --storage-net-node-addrs=<NODE_IP_ADDRS> \
    --item-input-file=itemsWithVersionsList.csv \
    --storage-object-versioning=true
```

## 5. Minio S3 server

For tests, a [`minio/minio`](https://github.com/minio/minio) S3 server is used. 
It can be deployed to test the spt commands and S3-specific scenarios if there is no access to real S3 storage.

Example:
```
docker run -d --name s3_server \
        -p 9000:9000 \
        --env MINIO_ACCESS_KEY=user1 \
        --env MINIO_SECRET_KEY=secretKey1  \
        minio/minio:latest \
        server /data
```

Spt run:
```
docker run --network host \
        dcr.octo.dell.com/spt/spt-storage-driver-s3  \
        --storage-net-node-port=9000 \
        --storage-auth-uid=user1 \
        --storage-auth-secret=secretKey1 \
        --storage-net-node-addrs=localhost 
```
