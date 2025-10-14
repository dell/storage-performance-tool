# FAQ

## What is *latency* and *duration*?

![](spt-lat-dur.png)

A = spt start formulate a request

B = spt sent the first byte to storage

C = spt get the first byte of response from storage

D = spt get the last byte of response from storage; operation finish

**Latency = C-B** : this value is measured in nanoseconds and shows how much time passed since spt sent first byte to storage till first byte  from storage was returned. This value depends on object size in case of Create or Update, and doesn't in case of Read and Delete.

**Duration = D-A** : total operation time (this value is measured in nanoseconds).

## What is *concurrency*?

**Concurrency level** = count of concurrently executed load operations/ the number of open connections at any moment of time ( in terms of the [netty driver](https://github.com/dell-spt/spt-storage-driver-netty) and its ["child" drivers](https://github.com/dell-spt/spt#dependency)).

## How to deploy with docker or jar?

We recommend deploying the spt with **docker**, because:

* no need to have specific version of java on your machine;
* no need to search for the latest jar file and download it from maven;
* no need to search and download jar with extensions for each storage driver;
* and all the other [advantages of docker](https://dzone.com/articles/top-10-benefits-of-using-docker)

## Does docker affect tool performance?

3 tests were performed for each item-size (10B-100MB):
  * spt-storage-driver-s3 + minio server
  * standalone mode
  * CREATE operations

AVG Bandwith:

![](../images/docker_vs_jar_bw.png)

AVG Latency:

![](../images/docker_vs_jar_lat.png)

## Does spt use 10^3 or 2^10 multiplier for input and output data sizes?

For data sizes spt only uses 2^10 (1024) multiplier. So 1MB is 1_048_576 bytes.
