# NIO Storage Driver

The count of the concurrent load operations at is limited also by the `storage-driver-threads` configuration option
which is CPU core count by default. This means that operations will occupy no more I/O worker threads than configured.
However the operations are reentrant and the count of the active (started but not completed yet) operations may be much
more(may be limited by `storage-driver-limit-concurrency` configuration option).
