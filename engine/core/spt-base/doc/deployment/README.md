# Deployment

1. [Environment Requirements](#environment-requirements)<br/>
2. [Jar](#jar)<br/>
3. [Docker](#docker)<br/>
    3.1. [Standalone](#standalone)<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;3.1.1 [Mount files](#mount-files)<br/>
    3.2. [Distributed Mode](#distributed-mode)<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;3.2.1 [Custom ports](#custom-ports)<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;3.2.2 [One log directory per engine](#one-log-directory-per-engine)<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;3.2.3 [2 docker containers on 1 machine](#2-docker-containers-on-1-machine)<br/>
    3.3 [Additional Notes](#additional-notes)<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;3.3.1 [Logs Sharing](#logs-sharing)<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;3.3.2 [Debugging](#debugging)<br/>
    3.4. [Docker-compose](#docker-compose)<br/>
    3.5. [Docker-swarm](#docker-swarm)<br/>
4. [Kubernetes](#kubernetes)<br/>
  1. [Helm](#helm)<br/>

---

# Environment Requirements

* Java 21+ or Docker
* OS open files limit is at least a bit higher than specified concurrency level
* Few gigabytes of free memory.

High-load tests may allocate up to few GBs of the memory depending on the scenario.
* (Remote Storage) Connectivity with the endpoint nodes via the ports used
* (Distributed Mode) Connectivity with the additional/remote nodes via port #1099 (RMI)
* (Remote Monitoring) Connectivity with the nodes via port #9010 (JMX)

---

# Jar

Spt is distributed as a single jar file from:
http://central.maven.org/maven2/io/github/dell/spt/spt-base/

[About bundle jars](https://github.com/dell/storage-performance-tool#backward-compatibility-notes)

---

# Docker

Spt images are stored in [GitHub Container Registry](https://ghcr.io/dell/storage-performance-tool)

## Base image

**Note**
> The base image doesn't contain any additonal load step types neither additional storage drivers. Please use one of the
> specific images either consider using the [backward compatibility bundle](https://github.com/dell/storage-performance-tool)

## Standalone

The image may be used in the standalone mode:
```bash
docker run \
    --network host \
    ghcr.io/dell/storage-performance-tool \
    [<SPT CLI ARGS>]
```

### Mount files

An example for mounting and using a scenario. Thus, files for input/output, configurations, logs and metrics can be mounted.

```
docker run -d --network host  \
    -v /path/to/scenario.js:/opt/scenario.js \
    ghcr.io/dell/storage-performance-tool \
    --run-scenario=/opt/scenario.js
``` 

**Mounting** a volume **is the only right way** to save Spt logs in docker. Don't get confused by [`--item-output-file` option](../usage/output#127-item-list-files), it's not about docker.

## Distributed Mode

#### Node

First, it's necessary to start some node/peer services.

Additional node run command:
```bash
docker run \
    --network host \
    ghcr.io/dell/storage-performance-tool \
    --run-node 
```

#### Run

To invoke the run in the distributed mode it's necessary to specify the additional node/peer addresses.

Entry node run command:
```bash
docker run \
    --network host \
    ghcr.io/dell/storage-performance-tool \
    --load-step-node-addrs=<ADDR1,ADDR2:PORT,ADDR3...> \
    [<SPT CLI ARGS>]
```

#### Custom ports

**NOTE** 
> Spt uses `1099` port for RMI between spt nodes and `9999` for REST API. If you run several spt nodes on the same host (in different docker containers, for example) or if the ports are used by another service, then ports can be redefined:

**Additional node:**
```bash
docker run \
    --network host \
    ghcr.io/dell/storage-performance-tool \
    --run-node \
    --load-step-node-port=<CUSTOM RMI PORT> \
    --run-port=<CUSTOM HTTP PORT> 
 ```

**Entry node:**
```bash
docker run \
    --network host \
    ghcr.io/dell/storage-performance-tool \
    --load-step-node-addrs=ADDR:<CUSTOM RMI PORT> \
    [<SPT CLI ARGS>]
```

**NOTE** 
> If port didn't specified, then `1099` will be used by default.

#### One log directory per engine

An engine writes its logs and step artifacts (metrics, operation lifecycle files) under `<DIR>/log/<STEP ID>/`,
where `<DIR>` is, in this order:

1. the `SPT_LOG_DIR` environment variable, if it is set and not empty;
2. else the `spt.log.dir` Java system property, if it is set and not empty;
3. else the engine's working directory.

In distributed mode every node runs a step under the same step id as the Entry Node. So **each engine process on a
host, node or Entry Node, needs its own log directory**: its own working directory, or its own `SPT_LOG_DIR` or
`-Dspt.log.dir`. Two engines started from the same directory with neither set share one `log/` directory, and then:

* every CREATE or READ step fails, and the run exits 1;
* the node may keep the failed step's resources: its queued operations, a thread and its periodic metrics output;
* an operation lifecycle file of another run's step can be merged into the run's `operation.lifecycle.csv`.

With `java -jar` (or `run.sh`), start each engine from its own working directory:

```bash
# shell 1: the node
mkdir -p ~/spt-node && cd ~/spt-node
java -jar /path/to/spt.jar --run-node --load-step-node-port=1100 --run-port=9998

# shell 2: the Entry Node
mkdir -p ~/spt-entry && cd ~/spt-entry
java -jar /path/to/spt.jar --load-step-node-addrs=localhost:1100 [<SPT CLI ARGS>]
```

or give each its own `SPT_LOG_DIR` (or `-Dspt.log.dir`) from the same working directory:

```bash
SPT_LOG_DIR="$PWD/node" java -jar /path/to/spt.jar --run-node --load-step-node-port=1100 --run-port=9998
SPT_LOG_DIR="$PWD/entry" java -jar /path/to/spt.jar --load-step-node-addrs=localhost:1100 [<SPT CLI ARGS>]
```

The same holds for a node and an Entry Node started on one client node with `run.sh`: start them from different
directories, or set a different `SPT_LOG_DIR` for each.

Each container has its own file system (the image's working directory is `/opt/spt`), so engines in different
containers keep their logs apart, unless they bind-mount the same host directory as their log directory: see
[Logs Sharing](#logs-sharing).

#### 2 docker containers on 1 machine
> Note 1: E = Entry node, A = Additional node, D = Address used in defaults.yaml

> Note 2: To get Internal IP of container: `docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' [DOCKER ID/NAME]`

> Note 3: Each container writes its logs in its own file system. Don't bind-mount the same host directory as the log
> directory of both containers: see [One log directory per engine](#one-log-directory-per-engine).

| It works | It doesn't work |
| --- | --- |
| E: `--network host` <br></br> A: `-p rest2:rest -p rmi2:rmi` <br></br> D: `localhost:rmi2`| E: `--network host` <br></br> A: `--network host` <br></br> D: `localhost:rmi`|
| E: `--network host` <br></br> A: `None (used docker bridge)` <br></br> D: `Internal IP`| E: `-p rest1:rest -p rmi1:rmi` <br></br> A: `-p rest2:rest -p rmi2:rmi` <br></br> D: `localhost:rmi2`|
| E: `-p rest1:rest` <br></br> A: `None (used docker bridge)` <br></br> D: `Internal IP`| |
| E: `-p rest1:rest` <br></br> A: `-p rest2:rest` <br></br> D: `Internal IP`| |

## Additional Notes

#### Logs Sharing

The example below mounts the host's directory `./log` to the container's
`/opt/spt/log` (where spt holds its log files).

```bash
docker run \
    --network host \
    --mount type=bind,source="$(pwd)"/log,target=/opt/spt/log
    ghcr.io/dell/storage-performance-tool \
    [<SPT CLI ARGS>]
```

**Caution**
> Give each container on a host its own host directory. Two engines, such as a node and an Entry Node, whose
> containers mount the same host directory at `/opt/spt/log` share one log directory, which breaks their steps:
> see [One log directory per engine](#one-log-directory-per-engine). For example, mount `"$(pwd)"/log-node` for the
> node and `"$(pwd)"/log-entry` for the Entry Node.

#### Debugging

The example below starts the Spt in the container with remote
debugging capability via the port #5005.

At first it's need to build new docker image for debug:


```bash
docker build --build-arg SPT_VERSION=latest -f ci/docker/Dockerfile.debug -t ghcr.io/dell/storage-performance-tool:debug .
```
or with `SPT_VERSION` value bt default (latest):
```bash
docker build -f ci/docker/Dockerfile.debug -t ghcr.io/dell/storage-performance-tool:debug .
```

and run:

```bash
docker run \
    --network host \
    ghcr.io/dell/storage-performance-tool:debug \
    [<SPT CLI ARGS>]
```

## Docker-compose 

> *Checked with Docker version: 19.03.8*

#### Deploy only spt nodes
Change `.env` file to configure image and project name.
```bash
docker-compose up -d --scale spt-node=3
```
Check:
```bash
# docker ps
CONTAINER ID        IMAGE                                      COMMAND                  CREATED             STATUS              PORTS                                            NAMES
6e3ec1f837c8        ghcr.io/dell/storage-performance-tool:latest           "/opt/spt/entry…"   14 seconds ago      Up 12 seconds       0.0.0.0:1091->1099/tcp, 0.0.0.0:9991->9999/tcp   spt_spt-node_3
f671b77ffd27        ghcr.io/dell/storage-performance-tool:latest           "/opt/spt/entry…"   14 seconds ago      Up 12 seconds       0.0.0.0:1093->1099/tcp, 0.0.0.0:9993->9999/tcp   spt_spt-node_2
40255c0a91d9        ghcr.io/dell/storage-performance-tool:latest           "/opt/spt/entry…"   14 seconds ago      Up 12 seconds       0.0.0.0:1092->1099/tcp, 0.0.0.0:9992->9999/tcp   spt_spt-node_1
...
```

#### Start entry-node (or with [REST API](../usage/api/remote/README.md)):
```bash
docker run -d --name spt \
              --network host \
              ghcr.io/dell/storage-performance-tool:latest \
            --load-step-node-addrs=localhost:1091,localhost:1092,localhost:1093
```

or with created network `spt_default`:
```bash
docker run -d --name spt \
              --network spt_default \
              ghcr.io/dell/storage-performance-tool:latest \
            --load-step-node-addrs=spt_spt-node_1,spt_spt-node_2,spt_spt-node_3
```

## Docker-swarm

> *Checked with Docker version: 19.03.8*

#### Create docker swarm cluster

*prerequisites*: node1(ip1), node2(ip2), node3(ip3)

ssh to node1:
```
docker swarm init
### to display token
docker swarm join-token -q worker
```
ssh to node2, node2
```
docker swarm join --token <some token> <ip1>:2377
```

#### Deploy spt nodes
Change `.env` file to configure image and project name.
```
docker stack deploy --compose-file docker-swarm.yaml spt-nodes
```
```
$ docker stack ps spt-nodes
 ID                  NAME                       IMAGE                              NODE                DESIRED STATE       CURRENT STATE           ERROR               PORTS
 sy6krxo9vnj3        spt-nodes_spt-node.1   ghcr.io/dell/storage-performance-tool:latest   node5               Running             Running 1 second ago

$ curl -I node5:9999/run
HTTP/1.1 204 No Content
...
```
change spt replicas count:
```
export REPLICAS=3; docker stack deploy --compose-file docker-swarm.yaml spt-nodes
```
```
$ docker stack ps spt-nodes
ID                  NAME                       IMAGE                              NODE                DESIRED STATE       CURRENT STATE           ERROR               PORTS
sy6krxo9vnj3        spt-nodes_spt-node.1   ghcr.io/dell/storage-performance-tool:latest   node5               Running             Running 1 second ago
6m9d04e75ybd        spt-nodes_spt-node.2   ghcr.io/dell/storage-performance-tool:latest   node4               Running             Running 3 seconds ago
x7euup6ihumb        spt-nodes_spt-node.3   ghcr.io/dell/storage-performance-tool:latest   node6               Running             Running 2 seconds ago
```

also you can specify `IMAGE` and `TAG` to use custom spt docker image:tag

#### Destroy spt nodes

```bash
docker stack rm spt-nodes
```
---

# Kubernetes

Spt can be deployed in a [kubernetes](https://kubernetes.io/) cluster manually or with Helm. 

## Helm

The only officially supported way to deploy Spt in Kubernetes is through Helm charts.

[Spt Helm chart doc](https://github.com/emc-mongoose/mongoose-helm-charts)

## Logs

With command `kubectl logs -n spt <resource name>` you can see logs into container. For example:

```bash
$ kubectl logs -n spt spt
################################################### spt v 4.2.7 ###################################################
2019-04-08T09:41:52,777 I                                main                           Available/installed extensions:
	Load --------------------------> com.dell.spt.base.load.step.linear.LinearLoadStepExtension
	dummy-mock --------------------> com.dell.spt.base.storage.driver.mock.DummyStorageDriverMockExtension
	http --------------------------> com.dell.spt.storage.driver.coop.netty.http.HttpStorageDriverExtension
	s3 ----------------------------> com.dell.spt.storage.driver.coop.netty.http.s3.S3StorageDriverExtension
	WeightedLoad ------------------> com.dell.spt.load.step.weighted.WeightedLoadStepExtension
	PipelineLoad ------------------> com.dell.spt.load.step.pipeline.PipelineLoadStepExtension
	atmos -------------------------> com.dell.spt.storage.driver.coop.netty.http.atmos.AtmosStorageDriverExtension
...
```

## Deleting kubernetes resources

There are several ways to delete kubernetes resources:
* delete helm release `helm uninstall spt`
* delete by configuration `kubectl delete -f <filename>.yaml`
* manual removal `kubectl delete -n spt pod NAME`
* removal of all resources in namespace `kubectl delete -n spt pod --all`
* removal of namespace (including resources) `kubectl delete namespace spt`
