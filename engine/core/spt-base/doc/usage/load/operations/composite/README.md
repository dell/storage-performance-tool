# Composite Load Operations

## 1. Storage Side Concatenation

Some cloud storages support the concatenation of the data item parts written independently.
Storage-side concatenation (further - ***SSC***).

### 1.1. Limitations

1. The storage API supporting the SSC is used. These are `s3`, `dells3` and `swift` currently.
2. In the distributed mode, all data item parts are processed by the single storage driver.
3. "Create" load type is used to split the *large* data items into the parts to write them
 separately.
4. `load-batch-size` **must** be set to `1` for SSC operations. The CLI does this automatically
 when `--part-size` is set. Using the default of 4096 causes the internal child-operation queue
 to overflow, silently dropping part uploads and degrading throughput.

### 1.2. Approach

Spt has the so called *load operation* abstraction. Load operations are executed by the specific storage
drivers. The storage driver may be able to detect the special *composite* load operations and execute the
corresponding sequence of the *partial sub-operations*:

1. Initiate the SSC for the given data item.
2. Create the data item parts on the storage (in parallel).
3. Commit the data item SSC on the storage.
4. Abort the SSC on part failure (after retries are exhausted), cleaning up incomplete uploads.

### 1.3. Storage Drivers Support

#### 1.3.1. S3 Multipart Upload

https://docs.aws.amazon.com/AmazonS3/latest/dev/uploadobjusingmpu.html

#### 1.3.2. Swift Dynamic Large Objects

https://docs.openstack.org/swift/latest/overview_large_objects.html#direct-api

### 1.4. Configuration

The `item-data-ranges-threshold` configuration parameter controls the SSC behavior (exposed by
the CLI as `--part-size`). The value is the [size in bytes](../../../input/configuration#122-size).
Any new generated object is treated as "*large*" if its size is more or equal than the configured
threshold. *Large* objects are being split into the 2 or more parts with the size not more than the
configured value above.

### 1.5. Part-Level Retry

Individual parts are automatically retried up to 3 times on failure. If all retries for a part are
exhausted, the engine aborts the entire composite operation (e.g., sends `AbortMultipartUpload` for
S3). This prevents a single transient error from wasting all successfully uploaded parts while still
ensuring failed uploads are cleaned up.

### 1.6. Per-Part Checksums

When `storage-checksum-enabled=true`, the configured checksum algorithm is applied to each
individual part upload. The checksum is computed over the part's data slice and included in the
request headers. Supported algorithms: `md5`, `crc32`, `crc32c`, `sha1`, `sha256`.

### 1.7. Reporting

#### 1.7.1. Parts List Output

The record containing the object name and the corresponding upload id is written to the
`parts.upload.csv` file if SSC operation is finished. The upload completion response latency is also
persisted in the 3rd column. Aborted uploads are logged with an `abort` prefix.

### 1.8. Future Enhancements

* Support Read for the segmented objects (parallel range-read)
* Support Update for the segmented objects
* Support Copy for the segmented objects
