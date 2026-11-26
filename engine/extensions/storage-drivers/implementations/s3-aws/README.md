# SPT S3-AWS Storage Driver

AWS SDK implementation of S3 Storage Driver for SPT with CRT-based async client for high performance.

## Overview

This driver uses the official AWS SDK for Java v2 with the AWS Common Runtime (CRT) native bindings to interact with Amazon S3 and S3-compatible storage services. It provides a clean, maintainable alternative to the manual HTTP implementation in the `s3` driver, with approximately 70% less code while delivering dramatically higher throughput through native C optimizations.

## CRT-Based Performance

The driver uses `S3AsyncClient.crtBuilder()` with aws-crt-java bindings, which provides:

- **Native C performance layer** - Leverages aws-c-s3 C library for core operations
- **Automatic multipart parallelism** - CRT automatically splits large uploads/downloads
- **DNS load balancing** - Automatic distribution across S3 endpoints
- **Connection health management** - Built-in connection pooling and health checks
- **Zero JNI complexity** - Pure Java API with native optimizations transparently

### Performance Tuning

The CRT client is configured with the following performance parameters:

- `storage.crt.targetThroughputGbps: 20.0` - Target network throughput (adjust based on network capacity)
- `storage.crt.minimumPartSizeBytes: 16777216` (16 MiB) - Minimum part size for multipart uploads

Both are configuration settings and can be set like any other to match your network infrastructure.

## Integration

### AWS SDK Version

- **Version:** 2.57.11
- **CRT Version:** 0.50.0
- **Compatibility:** Compatible with Amazon S3 and S3-compatible services (MinIO, SeaweedFS, Ceph, etc.)

### Dependencies

The driver bundles the following AWS SDK components:

- `software.amazon.awssdk:s3` - S3 service client
- `software.amazon.awssdk:auth` - Authentication
- `software.amazon.awssdk:regions` - Region configuration
- `software.amazon.awssdk:aws-core` - Core SDK functionality
- `software.amazon.awssdk:sts` - STS for credential resolution
- `software.amazon.awssdk:http-client-spi` - HTTP client SPI
- `software.amazon.awssdk:apache-client` - Apache HTTP client (runtime-only, fallback)
- `software.amazon.awssdk:sdk-core` - SDK core
- `software.amazon.awssdk:utils` - Utility classes
- `software.amazon.awssdk:profiles` - Profile management
- `software.amazon.awssdk.crt:aws-crt` - CRT native bindings for high-performance S3 operations

### HTTP Client

The driver uses the CRT-based HTTP client for S3 operations. The Apache HTTP client is included as a runtime dependency for compatibility but is not used by default.

## Architecture

### Async Implementation

The driver uses `S3AsyncClient` with CompletableFuture for all S3 operations:
- `putObject()` - Uses `AsyncRequestBody` for uploads
- `readObject()` - Uses `AsyncResponseTransformer.toBytes()` for downloads
- `deleteObject()` - Async legacy single-object delete operation
- standalone DELETE requests - `DeleteObject` for one target and one non-quiet
  `DeleteObjects` call for 2 through 1,000 same-bucket targets
- `listObjects()` - Async list operation with pagination and first response body byte timing for LIST TTFB metrics

Standalone targets with a version ID use exact-version deletion; targets without one retain
ordinary current-key semantics, including delete-marker behavior on versioned buckets. The SDK
response is reconciled by key and version before the logical request completes, so partial and
malformed batch responses cannot be reported as full success. SDK-managed retries remain inside
the single logical request future and timing sample (see [Client Retries](#client-retries)).

The standalone DELETE timing client records native CRT HTTP-stream `send-start` and `receive-start`
timestamps for both header-only `DeleteObject` and body-bearing `DeleteObjects`. The SDK wrapper
observes received headers and response-publisher completion for duration. Publisher subscription,
request-body delivery, signed-request handoff, and SDK-future completion are not byte markers.
Transparent retries retain the logical request's first send marker but replace earlier response timing,
so a terminal failure
without a completed response cannot reuse stale latency or duration samples.

### Multipart Upload Timing

A multipart upload records one `Duration` and `Latency` sample per part and one for the upload
itself, as on the `s3` driver. The upload's sample is its completion request alone, including any
retries and back-off the client makes for it, as for a standalone DELETE (see
[Client Retries](#client-retries)); a completion that fails adds no sample. The op trace's row for
the upload's parent and the per-item timing output cover the completion too; for a failed upload they cover its completion and the abort sent after it, or the abort alone when parts failed. The whole upload's
time is the op trace's initiation row's start to its parent row's end. Through 5.17.0 the upload's
sample ran from its initiation to its completion.

### Reads Split by the Part Size

A READ of an object larger than `item.data.ranges.threshold` (`--part-size` on the CLI) is split
into parts, as on the `s3` driver: each part is a ranged GET of its bytes in the object, and the
parts are read in parallel. The read sends no request of its own. It counts once, when its last
part ends, with the object's size in bytes and a duration from its start to its last part's end;
each part also records its own duration sample. A failed part is retried up to 3 times; if it still
fails, the other parts finish and the read fails with that part's status. The bytes of the parts
that succeeded still count toward the byte total, of a read that fails, of a read that is retried
whole, and of a read left `unresolved` because a time limit stopped it with a part unsent. A split
read ignores a requested version ID, as on `s3`: each part reads the current
object. Through 5.17.0 the driver failed such a read without a request.

### Requests the Client Adds

The AWS CRT client sends requests and bytes that SPT does not report, so the target sees more from
`s3-aws` than SPT counts, and more than it sees from the Netty `s3` driver for the same workload:

- **A HEAD before each whole-object GET.** The client validates response checksums by default. A
  READ of a whole object is a HEAD with `x-amz-checksum-mode: enabled`, then a ranged GET
  (`Range: bytes=0-<size - 1>`, answered 206) with `x-amz-checksum-mode` and `If-Match` set to the
  HEAD's ETag. A GET that already carries a range has no HEAD, so each part of a
  [split read](#reads-split-by-the-part-size) is one request. A READ of an exact version ID goes
  through the ordinary SDK client and has no HEAD.
- **A HEAD before each copy.** The client looks up the copy source with a HEAD before it sends the
  copy. No client option removes it.
- **A CRC trailer on each PUT.** The client sends a PUT body with `aws-chunked` framing and a
  checksum trailer, even with `checksumEnabled` off (the default): 45 bytes more per one-part object.

Neither HEAD counts as an SPT operation, but the HEAD before a GET is part of that GET's reported
latency. Compare `s3-aws` GET latency with `s3` with the extra round trip in mind.

Measured on SPT 5.17.0 (AWS SDK 2.57.11, CRT 0.50.0) against a loopback fake S3 target, not a real
one:

- 20 READs reached the target as 20 HEADs and 20 ranged GETs on `s3-aws`, and as 20 plain GETs on
  `s3`. 20 copies reached it as 20 HEADs and 20 copies.
- With the fake delaying each HEAD by 20 ms, 200 READs of 64 KiB objects at concurrency 1: the
  `s3-aws` READ p50 latency went from 3.0 ms to 26.4 ms; the `s3` READ p50 went from about 0.4 ms to
  0.5 ms.
- PUTs carried 45 bytes more per one-part object.

### UPDATE

An UPDATE rewrites the whole object with one PutObject of the item's current content, and counts
the item's size as its bytes done. This matches the `s3` driver's default UPDATE, which with no
ranges configured sends the whole object with its current content (with a `Range` header that
`s3-aws` does not send). Configured ranges (`item.data.ranges.random` or `item.data.ranges.fixed`)
are ignored: the whole object is written whatever ranges are set. An UPDATE is never a copy; only a
CREATE with a source path is (copy mode).

With object tagging on, the two drivers differ: the `s3` driver sends an UPDATE as `PUT ?tagging`
and does not rewrite the object, while `s3-aws` rewrites the whole object with the tags on the PUT.

Before 5.9.0 an UPDATE took the same route. From 5.9.0 to 5.17.0 an UPDATE of an item whose name
or input path carries a bucket was sent as a copy from a missing object and failed.

### Blocking Compatibility

To maintain compatibility with the existing `NioStorageDriverBase` threading model, async operations are blocked using `.join()` in `invokeNio()`. This preserves the existing driver architecture while leveraging CRT's native performance benefits.

## Usage

### Basic Usage

```java
// Create configuration
S3StorageDriverConfig config = new S3StorageDriverConfig(
    "your-access-key", 
    "your-secret-key", 
    "us-east-1", 
    "your-bucket-name"
);

// Create driver
S3StorageDriver driver = S3StorageDriverFactory.create(config);

// Store an object
String data = "Hello, S3!";
InputStream inputStream = new ByteArrayInputStream(data.getBytes());
Map<String, String> metadata = new HashMap<>();
metadata.put("custom-field", "custom-value");

driver.storeObject("test-key.txt", inputStream, data.length(), metadata);

// Retrieve an object
InputStream retrievedData = driver.getObject("test-key.txt");

// Check if object exists
boolean exists = driver.objectExists("test-key.txt");

// List objects
List<StorageMetadata> objects = driver.listObjects("test-");

// Delete an object
driver.deleteObject("test-key.txt");

// Close the driver
driver.close();
```

### Using with S3-Compatible Services (MinIO, Ceph, etc.)

```java
S3StorageDriverConfig config = new S3StorageDriverConfig(
    "your-access-key", 
    "your-secret-key", 
    "us-east-1", 
    "your-bucket-name"
);

// Configure for S3-compatible service
config.setEndpointOverride("http://localhost:9000");
config.setPathStyleAccess(true);

S3StorageDriver driver = S3StorageDriverFactory.create(config);
```

### Advanced Configuration

```java
S3StorageDriverConfig config = new S3StorageDriverConfig(
    "your-access-key", 
    "your-secret-key", 
    "us-east-1", 
    "your-bucket-name"
);

// Performance tuning
config.setMaxConnections(100);
config.setSocketTimeout(60000);
config.setConnectionTimeout(20000);

// Enable metrics
config.setEnableRequestMetrics(true);

S3StorageDriver driver = S3StorageDriverFactory.create(config);
```

## Configuration Options

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| accessKey | String | Required | AWS access key ID |
| secretKey | String | Required | AWS secret access key |
| region | String | `us-east-1` fallback | AWS region from `storage.region` in YAML or `--storage-region` in the engine; exposed by `spt run --region`. Empty or omitted uses the region of a standard AWS endpoint (`s3.<region>.amazonaws.com`), else `us-east-1`. Before 5.16.2 the fallback was `eu-west-2`. |
| bucketName | String | Required | S3 bucket name |
| endpointOverride | String | null | Override S3 endpoint (for S3-compatible services) |
| pathStyleAccess | boolean | false | Use path-style access (required for some S3-compatible services) |
| maxConnections | int | 50 | Maximum number of concurrent connections |
| storage.crt.socketTimeoutMs | int | 30000 | Milliseconds a connection with a request in flight may move no data before it is closed and the request fails as a timeout; `0` turns the detection off, otherwise at least `2000`, counted in whole seconds rounded down (see [AWS SDK driver timeouts](../../../../README.md#aws-sdk-driver-timeouts)) |
| storage.crt.connectionTimeoutMs | int | 5000 | Milliseconds allowed to establish a connection |
| storage.crt.requestTimeoutMs | int | 300000 | Milliseconds a request may take from the moment the driver makes it until its response has been read in full, a GET's body included; a request that outlives this is cancelled and fails as a timeout. `0` waits without bound (see [AWS SDK driver timeouts](../../../../README.md#aws-sdk-driver-timeouts)) |
| storage.crt.stopTimeoutMs | int | 5000 | Milliseconds the requests in flight get once the step stops before the driver cancels them; `0` leaves them to the step's wait and the stop of the workers |
| storage.driver.threads | int | 0 | I/O workers. `0` runs one worker per concurrency permit, since every request blocks its worker (see [AWS SDK driver workers](../../../../README.md#aws-sdk-driver-workers)) |
| enableRequestMetrics | boolean | false | Enable AWS SDK request metrics |
| checksumEnabled | boolean | false | Compute and send a checksum on write requests |
| checksumAlgorithm | String | null | Checksum algorithm: `crc32`, `crc32c`, `sha1`, `sha256`, `crc64-nvme`. MD5 is not supported as a flexible checksum by the AWS SDK |

## API Methods

The S3StorageDriver implements the standard StorageDriver interface:

- `storeObject(String key, InputStream data, long contentLength, Map<String, String> metadata)`
- `getObject(String key)` - Returns InputStream
- `getObjectWithMetadata(String key)` - Returns StorageObject with metadata
- `deleteObject(String key)`
- `objectExists(String key)` - Returns boolean
- `listObjects(String prefix)` - Returns List<StorageMetadata>
- `copyObject(String sourceKey, String destinationKey)`
- `getObjectMetadata(String key)` - Returns StorageMetadata

The method list above describes the legacy single-object API. Batched deletion is exposed only
through the engine's first-class standalone `DeleteRequestOperation`; the driver does not provide
a separate public `deleteObjects()` utility method.

## Error Handling

All methods throw `IOException` with detailed error messages. Common error scenarios:

- Authentication failures
- Network connectivity issues
- Bucket/object not found
- Permission denied
- Service throttling (see [Client Retries](#client-retries))

### Client Retries

The AWS clients retry a 5xx answer themselves, before SPT sees the result. The driver builds its CRT
transfer client with no retry configuration, so the SDK's default retries apply to every request
type it sends: PUT, multipart part, GET, HEAD, DELETE and copy. The ordinary SDK client, which
sends standalone DELETE requests and requests for an exact version ID, retries too.

Measured with AWS SDK 2.57.11 and CRT 0.50.0, the versions SPT 5.17.0 ships, against a loopback
fake S3 target:

- A CRT client request answered 500, 503 or 504 every time reached the target up to 6 times before
  SPT saw one failure.
- That holds while the client's retry budget lasts, about 100 retries per client. Of 60 concurrent
  PUTs answered 503 on one client, the earliest reached the target up to 6 times each; once the
  budget ran out, the last 38 reached it once each.
- An ordinary SDK client call answered with a 5xx every time reached the target 4 times.
- A transient 5xx shows as a slower success. A PUT answered 503 once and then 200 was a success on
  `s3-aws`, after 2 requests at the target and 545 ms; on `s3` it was one failure after 1 request.
- A 502 answer was not measured.

SPT's own retries (`load.op.retry`, off by default) multiply these: each retry SPT sends goes
through the client's retries again. The target therefore receives more requests than SPT reports,
and on a throttling target `s3-aws` reports fewer failures than `s3` and counts the client's
back-off as latency. Compare `s3-aws` with `s3` only on a target that does not throttle.

## Testing

The module includes comprehensive unit tests using JUnit and Mockito:

```bash
./gradlew test
```

The default suite includes a fast canary, `S3AwsTimeoutsCanaryTest`, which checks that each S3 client the driver builds receives `storage.crt.connectionTimeoutMs` and `storage.crt.socketTimeoutMs`. The slower tests, tagged `slow`, let requests fail after every retry of their client: stalls in each phase of a request, a transfer that keeps moving data, stall detection turned off, and connections that cannot be made. They take about a minute and run only on request, from `engine/`:

```bash
./gradlew :extensions:storage-drivers:implementations:s3-aws:slowTest
```

Run them after upgrading the AWS SDK or the AWS CRT, and after changing how the driver builds its S3 clients or classifies timeouts.

## Migration from Legacy REST Implementation

This AWS SDK implementation provides the same interface as the legacy REST implementation (`com.dell.spt.storage.driver.coop.netty.http.s3.S3StorageDriver`). Migration steps:

1. Update dependency from REST driver to AWS SDK driver
2. Update configuration to use AWS SDK configuration format
3. Replace driver instantiation with factory methods
4. Update any custom error handling if needed

## Performance Considerations

- Use appropriate connection pool sizes based on expected load
- Configure timeouts based on network conditions and object sizes
- Enable path-style access for S3-compatible services
- Consider using multipart upload for large objects (>5MB)

## Security

- Store credentials securely (environment variables, secret management)
- Use IAM roles when running on AWS infrastructure
- Enable encryption at rest and in transit
- Use least-privilege access policies

## Compatibility

- Java 8+
- AWS SDK for Java v2.21.29+
- Compatible with Amazon S3 and S3-compatible storage services
