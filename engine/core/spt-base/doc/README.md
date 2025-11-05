# Documentation

1. [Get started](getstarted)<br/>

2. [Comparison With Similar Tools](comparision)<br/>

3. [Design](design) <br/>
    **3.1. [Architecture](design/architecture)<br/>**
    &nbsp;&nbsp;&nbsp;&nbsp;3.1.1 [Basic Terms](design//architecture#1-basic-terms)<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;3.1.2 [Components](design//architecture#2-components)<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;3.1.3 [Concurrency](design//architecture#3-concurrency)<br/>
    3.2. [Modes](design/modes)<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;3.2.1 [Distributed Mode](design/modes/distributed_mode)<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;3.2.2 [Recycle Mode](design/modes/recycle_mode)<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;3.2.3 [Copy Mode](design/modes/copy_mode)<br/>
    3.3. [Installer](design/installer)<br/>
    3.4. [Data Reentrancy](design/data_reentrancy)<br/>
    3.5. [Byte Range Operations](usage/load/operations/byte_ranges)<br/>
    

4. [Deployment](deployment)<br/>
    4.1. [Environment Requirements](deployment#environment-requirements)<br/>
    4.2. [Jar](deployment#jar)<br/>
    **4.3. [Docker](deployment#docker)<br/>**
    &nbsp;&nbsp;&nbsp;&nbsp;4.3.1 [Mount files](deployment#mount-files)<br/>
    **4.4. [Kubernetes](deployment#kubernetes)<br/>**
    &nbsp;&nbsp;&nbsp;&nbsp;4.4.1 [Helm](https://github.com/emc-mongoose/mongoose-helm-charts)<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;4.4.2 [Manual](deployment#manual-deployment)<br/>

5. [Usage](usage)<br/>
    **5.1. [Input](usage/input)<br/>**
    &nbsp;&nbsp;&nbsp;&nbsp;5.1.1. [CLI](usage/input/cli)<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;5.1.2. [Configuration](usage/input/configuration)<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;5.1.3. [Scenarios](usage/input/scenarios)<br/>
    **5.2. [Output](usage/output)<br/>**
    &nbsp;&nbsp;&nbsp;&nbsp;5.2.1. [General Output](usage/output#1-general)<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;5.2.2. [Metrics Output](usage/output#2-metrics)<br/>
    **5.3. [Remote API](usage/api/remote)<br/>**
    &nbsp;&nbsp;&nbsp;&nbsp;5.3.1 [Config API](usage/api/remote#config)<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;5.3.2 [Runs API](usage/api/remote#run)<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;5.3.3 [Logs API](usage/api/remote#logs)<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;5.3.4 [Metrics API](usage/api/remote#metrics)<br/>
    **5.4. Load Generation<br/>**
    &nbsp;&nbsp;&nbsp;&nbsp;5.4.1. [Items](usage/item) <br/>
    &nbsp;&nbsp;&nbsp;&nbsp;5.4.2. [Load Operations](usage/load/operations) <br/>
    &nbsp;&nbsp;&nbsp;&nbsp;5.4.3. [Load Steps](usage/load/steps)<br/>
    **5.5. [Load Scaling](usage/scaling)<br/>**
    &nbsp;&nbsp;&nbsp;&nbsp;5.5.1. [Rate](usage/scaling#1-rate)<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;5.5.2. [Concurrency](usage/scaling#2-concurrency)<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;5.5.3. [Distributed Mode](usage/scaling#3-distributed-mode)<br/>

6. [Troubleshooting](troubleshooting)<br/>

7. [Extentions](https://github.com/dell/storage-performance-tool)<br/>
    **7.1. Storage Drivers<br/>**
    &nbsp;&nbsp;&nbsp;&nbsp;7.1.1  [S3](https://github.com/dell/storage-performance-tool/tree/main/engine/extensions/storage-drivers/implementations/s3)<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;7.1.2. [Atmos](https://github.com/dell/storage-performance-tool/tree/main/engine/extensions/storage-drivers/implementations/atmos)<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;7.1.3. [Swift](https://github.com/dell/storage-performance-tool/tree/main/engine/extensions/storage-drivers/implementations/swift)<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;7.1.4. [FS](https://github.com/dell/storage-performance-tool/tree/main/engine/extensions/storage-drivers/protocols/fs)<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;7.1.5. [HDFS](https://github.com/emc-mongoose/mongoose-storage-driver-hdfs)<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;7.1.6. [Pravega](https://github.com/emc-mongoose/mongoose-storage-driver-pravega)<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;7.1.7. [Kafka](https://github.com/emc-mongoose/mongoose-storage-driver-kafka)<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;7.1.8. [Pulsar](https://github.com/emc-mongoose/mongoose-storage-driver-pulsar)<br/>
    **7.2. Load Steps<br/>**
    &nbsp;&nbsp;&nbsp;&nbsp;7.2.1  [Pipeline](https://github.com/dell/storage-performance-tool/tree/main/engine/extensions/load-steps/pipeline)<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;7.2.2. [Weighted](https://github.com/dell/storage-performance-tool/tree/main/engine/extensions/load-steps/weighted)<br/>
    7.3. [Auxiliary Tools](https://github.com/dell/storage-performance-tool#auxiliary-tools)

8. [Dependencies](dependencies)<br/>

9. [Contributing](contributing)<br/>

10. [Changelog](changelog)<br/>
