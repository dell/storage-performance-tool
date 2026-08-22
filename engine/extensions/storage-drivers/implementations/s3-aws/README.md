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

- `targetThroughputInGbps: 20.0` - Target network throughput (adjust based on network capacity)
- `minimumPartSizeInBytes: 8 MB` - Minimum part size for multipart uploads

These parameters can be adjusted in `S3AwsStorageDriverFactory.java` to match your network infrastructure.

## Integration

### AWS SDK Version

- **Version:** 2.42.28
- **CRT Version:** 0.44.0
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
the single logical request future and timing sample.

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
| region | String | Required | AWS region |
| bucketName | String | Required | S3 bucket name |
| endpointOverride | String | null | Override S3 endpoint (for S3-compatible services) |
| pathStyleAccess | boolean | false | Use path-style access (required for some S3-compatible services) |
| maxConnections | int | 50 | Maximum number of concurrent connections |
| socketTimeout | int | 30000 | Socket timeout in milliseconds |
| connectionTimeout | int | 10000 | Connection timeout in milliseconds |
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
- Service throttling

## Testing

The module includes comprehensive unit tests using JUnit and Mockito:

```bash
./gradlew test
```

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
