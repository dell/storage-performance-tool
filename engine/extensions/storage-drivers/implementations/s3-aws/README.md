# S3 AWS Storage Driver

This module provides an AWS SDK implementation of the S3 Storage Driver for SPT (Storage Platform Technology). It offers a modern, efficient alternative to the legacy REST-based S3 implementation.

## Features

- **AWS SDK Integration**: Uses the official AWS SDK for Java v2
- **Full S3 Compatibility**: Supports all standard S3 operations
- **S3-Compatible Services**: Compatible with MinIO, Ceph, and other S3-compatible storage
- **Configuration Flexibility**: Extensive configuration options for performance and compatibility
- **Error Handling**: Comprehensive error handling with proper exception translation
- **Metadata Support**: Full support for custom metadata
- **Performance Optimized**: Configurable connection pooling and timeouts
- **Checksum Validation**: Per-object and per-part checksum support (CRC32, CRC32C, SHA1, SHA256) via AWS SDK flexible checksums

## Dependencies

- AWS SDK for Java v2 (2.21.29)
- SPT Storage Driver API
- SLF4J for logging

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
| checksumAlgorithm | String | null | Checksum algorithm: `crc32`, `crc32c`, `sha1`, `sha256`. MD5 is not supported as a flexible checksum by the AWS SDK |

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
