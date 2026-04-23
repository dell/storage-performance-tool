# S3-AWS Driver CRT Migration Analysis

## Executive Summary

This document analyzes the current s3-aws driver implementation and provides a detailed migration path to upgrade it to use `S3AsyncClient.crtBuilder()` with aws-crt-java bindings, as recommended in the SPT_AWS_SDK_Investigation_Report.docx.

**Current State:** Synchronous S3Client with Apache HTTP client
**Target State:** S3AsyncClient with CRT-based HTTP client (native C performance layer)
**Expected Benefit:** Dramatically higher throughput through native C optimizations, automatic multipart parallelism, and connection health management

---

## 1. Current Implementation Analysis

### 1.1 Current Architecture

**File:** `S3AwsStorageDriverFactory.java` (lines 113-137)

Current client creation:
```java
final var httpClient = ApacheHttpClient.builder()
    .maxConnections(maxConnections)
    .connectionTimeout(Duration.ofMillis(connTimeout))
    .socketTimeout(Duration.ofMillis(socketTimeout))
    .connectionAcquisitionTimeout(Duration.ofSeconds(3))
    .connectionMaxIdleTime(Duration.ofSeconds(30))
    .connectionTimeToLive(Duration.ofMinutes(2))
    .tcpKeepAlive(true)
    .build();

S3Client s3Client = S3Client.builder()
    .region(Region.of(region))
    .credentialsProvider(StaticCredentialsProvider.create(creds))
    .httpClient(httpClient)
    .endpointOverride(URI.create(endpoint))
    .forcePathStyle(pathStyle)
    .serviceConfiguration(S3Configuration.builder()
        .chunkedEncodingEnabled(false)
        .dualstackEnabled(false)
        .accelerateModeEnabled(false)
        .useArnRegionEnabled(true)
        .build())
    .build();
```

**Key Characteristics:**
- **Synchronous blocking I/O** - limits throughput under high concurrency
- **Apache HTTP client** - pure Java implementation
- **Manual connection tuning** - maxConnections, timeouts, keep-alive
- **No automatic multipart** - uploads use single stream
- **No native optimizations** - all processing in JVM

### 1.2 Driver Usage Pattern

**File:** `S3AwsStorageDriver.java`

The driver extends `NioStorageDriverBase` and executes operations synchronously within `invokeNio()`:
- Line 172: `execute(op)` - synchronous AWS SDK call
- Line 336-361: `execute()` method dispatches to putObject, readObject, deleteObject, listObjects
- All operations are blocking calls on the synchronous S3Client

**Current Operations:**
- `putObject()` (lines 363-387): Uses `RequestBody.fromInputStream()`
- `readObject()` (lines 389-410): Uses `s3Client.getObject()` with try-with-resources
- `deleteObject()` (lines 412-420): Simple synchronous delete
- `listObjects()` (lines 423-504): Synchronous ListObjectsV2

### 1.3 Current Dependencies

**File:** `build.gradle` (lines 20-31)
```gradle
dependencies {
    implementation libs.aws.s3
    implementation libs.aws.auth
    implementation libs.aws.regions
    implementation libs.aws.core
    implementation libs.aws.sts
    implementation libs.aws.http.client.spi
    implementation libs.aws.apache.client  // ← Apache HTTP client
    implementation libs.aws.sdk.core
    implementation libs.aws.utils
    implementation libs.aws.profiles
}
```

**File:** `libs.versions.toml` (line 47)
```toml
awsSdk = "2.42.28"
```

**Exclusions** (build.gradle lines 12-18):
```gradle
configurations.implementation {
    exclude group: 'software.amazon.awssdk', module: 'netty-nio-client'
    exclude group: 'io.netty'
}
```

---

## 2. CRT-Based Client Benefits

### 2.1 Performance Advantages

From the investigative report and AWS documentation:

1. **Native C Performance Layer** - Leverages aws-c-s3 C library for core operations
2. **Automatic Multipart Parallelism** - CRT automatically splits large uploads/downloads
3. **DNS Load Balancing** - Automatic distribution across S3 endpoints
4. **Connection Health Management** - Built-in connection pooling and health checks
5. **Zero Language Change** - Pure Java API, no JNI required

### 2.2 Throughput Improvements

The report indicates:
- Current synchronous Apache client leaves "significant performance on the table"
- CRT-based client can deliver "dramatically higher throughput"
- Same native C libraries that power the AWS C SDK (highest-performance option)

### 2.3 Compatibility

- Preserves existing Java codebase
- Preserves build system (Gradle)
- Preserves extension architecture
- Compatible with S3-compatible services (MinIO, Ceph, etc.)

---

## 3. Required Dependency Changes

### 3.1 Add CRT Dependency

**File:** `gradle/libs.versions.toml`

Add aws-crt version:
```toml
[versions]
awsSdk = "2.42.28"
awsCrt = "0.31.32"  # Add this - compatible with awsSdk 2.42.28
```

Add library definition:
```toml
[libraries]
aws-crt = { module = "software.amazon.awssdk.crt:aws-crt", version.ref = "awsCrt" }
```

### 3.2 Update build.gradle

**File:** `engine/extensions/storage-drivers/implementations/s3-aws/build.gradle`

Replace Apache client dependency with CRT:
```gradle
dependencies {
    // AWS SDK – extension-private, safe to bundle.
    implementation libs.aws.s3
    implementation libs.aws.auth
    implementation libs.aws.regions
    implementation libs.aws.core
    implementation libs.aws.sts
    implementation libs.aws.http.client.spi
    implementation libs.aws.sdk.core
    implementation libs.aws.utils
    implementation libs.aws.profiles
    
    // CRT native bindings for S3 performance
    implementation libs.aws.crt  // ← Add this
    
    // Remove: implementation libs.aws.apache.client
    // Keep Apache client as optional fallback if needed
    runtimeOnly libs.aws.apache.client
    
    // ... rest of dependencies
}
```

**Remove exclusions** (lines 12-18) - CRT may need Netty:
```gradle
// Remove these exclusions - CRT uses Netty internally
// configurations.implementation {
//     exclude group: 'software.amazon.awssdk', module: 'netty-nio-client'
//     exclude group: 'io.netty'
// }
```

**Update shadowJar exclusions** (lines 65-68):
```gradle
// Keep Netty exclusions for now, but may need adjustment
exclude('io/netty/**')  // May need to allow specific Netty versions used by CRT
```

---

## 4. Code Changes Required

### 4.1 S3AwsStorageDriverFactory.java

**Current:** Lines 113-137 create synchronous S3Client with ApacheHttpClient

**Target:** Create S3AsyncClient with CRT builder

```java
package com.dell.spt.storage.driver.coop.aws.s3;

import com.dell.spt.base.Constants;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.ExtensionBase;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.storage.driver.StorageDriverFactory;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.io.yaml.YamlSchemaProviderBase;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;  // ← Change from S3Client
import software.amazon.awssdk.services.s3.S3Configuration;

import java.io.InputStream;
import java.net.URI;
import java.util.List;

/**
 * ServiceLoader entry point for the AWS SDK based S3 storage driver.
 */
public final class S3AwsStorageDriverFactory<I extends Item, O extends Operation<I>>
        extends ExtensionBase
        implements StorageDriverFactory<I, O, S3AwsStorageDriver<I, O>> {

    private static final String NAME = "s3-aws";
    private static final String DEFAULTS_FILE_NAME = "defaults-storage-s3-aws.yaml";

    @Override
    public String id() {
        return NAME;
    }

    @Override
    public S3AwsStorageDriver<I, O> create(
            final String stepId,
            final DataInput dataInput,
            final Config storageConfig,
            final boolean verifyFlag,
            final int batchSize)
            throws IllegalConfigurationException, InterruptedException {

        return createInternal(stepId, dataInput, storageConfig, verifyFlag, batchSize);
    }

    private S3AwsStorageDriver<I, O> createInternal(
            final String stepId,
            final DataInput dataInput,
            final Config storageConfig,
            final boolean verifyFlag,
            final int batchSize)
            throws IllegalConfigurationException, InterruptedException {

        // ---------------------------
        // Authentication
        // ---------------------------
        final String accessKey;
        final String secretKey;
        try {
            accessKey = storageConfig.stringVal("auth-uid");
        } catch (Exception e) {
            throw new IllegalConfigurationException(
                    "Missing required config: storage.auth.uid (access key)");
        }
        try {
            secretKey = storageConfig.stringVal("auth-secret");
        } catch (Exception e) {
            throw new IllegalConfigurationException(
                    "Missing required config: storage.auth.secret (secret key)");
        }

        // Region — optional for S3-compatible services
        String region;
        try {
            region = storageConfig.stringVal("region");
        } catch (Exception e) {
            region = null;
        }
        if (region == null || region.isEmpty()) {
            region = "eu-west-2";
        }

        // Endpoint
        final String endpoint = resolveEndpoint(storageConfig);

        // Path-style access — S3-compatible stores require this
        final boolean pathStyle = true;

        // ---------------------------
        // CRT Performance Tuning
        // ---------------------------
        // Target throughput in Gbps - adjust based on network capacity
        final double targetThroughputInGbps = 20.0;  // 20 Gbps target
        // Minimum part size for multipart uploads (8 MB default)
        final long minimumPartSizeInBytes = 8 * 1024 * 1024L;
        // Maximum concurrent connections
        final int maxConnections = 128;

        // ---------------------------
        // Build AWS S3 Async Client with CRT
        // ---------------------------
        final var creds = AwsBasicCredentials.create(accessKey, secretKey);

        S3AsyncClient s3AsyncClient = S3AsyncClient.crtBuilder()
                .credentialsProvider(StaticCredentialsProvider.create(creds))
                .region(Region.of(region))
                .endpointOverride(URI.create(endpoint))
                .forcePathStyle(pathStyle)
                .targetThroughputInGbps(targetThroughputInGbps)
                .minimumPartSizeInBytes(minimumPartSizeInBytes)
                .serviceConfiguration(S3Configuration.builder()
                        .chunkedEncodingEnabled(false)
                        .dualstackEnabled(false)
                        .accelerateModeEnabled(false)
                        .useArnRegionEnabled(true)
                        .build())
                .build();

        return new S3AwsStorageDriver<>(
                stepId,
                dataInput,
                storageConfig,
                verifyFlag,
                batchSize,
                s3AsyncClient);  // ← Pass async client
    }

    /**
     * Resolve the S3 endpoint URL from storage config
     */
    static String resolveEndpoint(final Config storageConfig)
            throws IllegalConfigurationException {
        String endpoint = null;
        boolean sslEnabled = false;

        try {
            final Config netConfig = storageConfig.configVal("net");
            final Config nodeConfig = netConfig.configVal("node");
            final List<String> addrs = nodeConfig.listVal("addrs");

            int port = 0;
            try {
                port = nodeConfig.intVal("port");
            } catch (Exception ignored) {}

            try {
                sslEnabled = netConfig.configVal("ssl").boolVal("enabled");
            } catch (Exception ignored) {}

            if (addrs != null && !addrs.isEmpty()) {
                final String addr = addrs.get(0);
                final String scheme = sslEnabled ? "https" : "http";

                if (addr.startsWith("http://") || addr.startsWith("https://")) {
                    endpoint = addr;
                } else if (!addr.contains(":") && port > 0) {
                    endpoint = scheme + "://" + addr + ":" + port;
                } else {
                    endpoint = scheme + "://" + addr;
                }
            }
        } catch (Exception ignored) {}

        if (endpoint == null) {
            throw new IllegalConfigurationException(
                    "Missing required config: storage.net.node.addrs " +
                            "(S3 endpoint address)");
        }
        return endpoint;
    }

    @Override
    public SchemaProvider schemaProvider() {
        return new YamlSchemaProviderBase() {
            @Override
            protected InputStream schemaInputStream() {
                return getClass().getResourceAsStream(
                        "/config-schema-storage-s3-aws.yaml");
            }

            @Override
            public String id() {
                return Constants.APP_NAME;
            }
        };
    }

    @Override
    protected String defaultsFileName() {
        return DEFAULTS_FILE_NAME;
    }

    @Override
    protected List<String> resourceFilesToInstall() {
        return List.of("config/" + DEFAULTS_FILE_NAME);
    }
}
```

### 4.2 S3AwsStorageDriver.java

**Key Changes Required:**

1. **Change client type** from `S3Client` to `S3AsyncClient`
2. **Make operations async** using CompletableFuture
3. **Use async request/response transformers**
4. **Handle async exceptions**

```java
package com.dell.spt.storage.driver.coop.aws.s3;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.PathItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.item.op.list.ListOperation;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.storage.driver.ListDiscoveryProbe;
import com.dell.spt.base.storage.driver.ListOptions;
import com.dell.spt.storage.driver.coop.nio.NioStorageDriverBase;
import com.github.akurilov.confuse.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.io.IOException;
import com.dell.spt.base.item.io.DataItemInputStream;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;  // ← Changed from S3Client
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * AWS SDK implementation of S3 Storage Driver for SPT using CRT-based async client
 */
public class S3AwsStorageDriver<I extends Item, O extends Operation<I>> extends NioStorageDriverBase<I, O>
        implements ListDiscoveryProbe {

    private static final Logger LOG = LoggerFactory.getLogger(S3AwsStorageDriver.class);

    private final S3AsyncClient s3AsyncClient;  // ← Changed from S3Client
    private final String bucketName;
    private final boolean checksumEnabled;
    private final ChecksumAlgorithm checksumAlgorithm;

    public S3AwsStorageDriver(
                    final String stepId,
                    final DataInput dataInput,
                    final Config config,
                    final boolean verifyFlag,
                    final int batchSize,
                    final S3AsyncClient s3AsyncClient)  // ← Changed from S3Client
                    throws IllegalConfigurationException {
        super(stepId, dataInput, config, verifyFlag, batchSize);
        if (verifyFlag) {
            LOG.warn(
                            "S3-AWS driver does not support --item-data-verify; reads will report SUCC without integrity checks");
        }
        this.s3AsyncClient = s3AsyncClient;

        this.bucketName = resolveBucketName(config);

        boolean ckEnabled = false;
        ChecksumAlgorithm ckAlgo = null;
        try {
            ckEnabled = config.boolVal("checksum-enabled");
        } catch (Exception e) {
            LOG.debug("Could not read checksum-enabled from config: {}", e.toString());
        }
        if (ckEnabled) {
            try {
                final String algo = config.stringVal("checksum-algorithm");
                ckAlgo = resolveChecksumAlgorithm(algo);
                if (ckAlgo == null) {
                    LOG.warn("Unsupported checksum algorithm '{}' for s3-aws driver; checksums will not be applied", algo);
                    ckEnabled = false;
                }
            } catch (Exception e) {
                LOG.warn("Could not read checksum-algorithm from config: {}", e.toString());
                ckEnabled = false;
            }
        }
        this.checksumEnabled = ckEnabled;
        this.checksumAlgorithm = ckAlgo;
    }

    // ... [resolveChecksumAlgorithm, resolveBucketName methods unchanged] ...

    @Override
    protected void invokeNio(final O op) {
        try {
            // Execute AWS SDK operation asynchronously and block for result
            // This maintains the existing NioStorageDriverBase threading model
            execute(op).join();  // ← Block on async result

            // Set bytes transferred for metrics (skip READs — readObject sets actual bytes)
            if (op.type() != OpType.READ && op.item() instanceof DataItem) {
                DataItem dataItem = (DataItem) op.item();
                if (op instanceof DataOperation) {
                    ((DataOperation) op).countBytesDone(dataItem.size());
                }
            }

            // Use finishOperation helper like FileStorageDriver does
            finishOperation(op);

        } catch (Exception e) {
            op.status(classifyFailure(e));
            LOG.debug("{} {} failed: {}", op.type(), op.item().name(), e.toString());
            LOG.trace("{} {} stack trace", op.type(), op.item().name(), e);
            try {
                op.startResponse();
                op.finishResponse();
            } catch (Exception ignored) {}
        }
    }

    // ... [classifyFailure method unchanged] ...

    @Override
    protected String requestNewPath(final String path) {
        final String relPath = path.startsWith("/") ? path.substring(1) : path;
        final int slashPos = relPath.indexOf('/');
        final String targetBucket = slashPos > 0 ? relPath.substring(0, slashPos) : relPath;
        final String bucketPath = "/" + targetBucket;

        try {
            // Validate that the target bucket exists
            s3AsyncClient.headBucket(
                HeadBucketRequest.builder().bucket(targetBucket).build()
            ).join();
        } catch (Exception e) {
            if (isNoSuchBucket(e)) {
                // Bucket doesn't exist — create it
                try {
                    s3AsyncClient.createBucket(
                        CreateBucketRequest.builder().bucket(targetBucket).build()
                    ).join();
                } catch (Exception createEx) {
                    throw new RuntimeException("Failed to create bucket: " + targetBucket, createEx);
                }
            } else {
                throw new RuntimeException("Failed to validate bucket: " + targetBucket, e);
            }
        }

        return bucketPath;
    }

    private boolean isNoSuchBucket(Exception e) {
        if (e.getCause() instanceof S3Exception) {
            return ((S3Exception) e.getCause()).statusCode() == 404;
        }
        return false;
    }

    @Override
    protected String requestNewAuthToken(final Credential credential) {
        return null;
    }

    @Override
    public void adjustIoBuffers(final long avgTransferSize, final OpType opType) {
        // No-op, CRT manages its own buffers
    }

    // ... [resolveBucketAndKey, parseBucketAndKey methods unchanged] ...

    CompletableFuture<Void> execute(final O op) {
        switch (op.type()) {
        case NOOP:
            return CompletableFuture.completedFuture(null);

        case CREATE:
        case UPDATE:
            return putObject(op);

        case READ:
            return readObject(op);

        case DELETE:
            return deleteObject(op);

        case LIST:
            return listObjects(op);

        default:
            return CompletableFuture.failedFuture(new UnsupportedOperationException(op.type().toString()));
        }
    }

    private CompletableFuture<Void> putObject(final O op) {
        final var bk = resolveBucketAndKey(op);
        var reqBuilder = PutObjectRequest.builder()
                        .bucket(bk[0])
                        .key(bk[1]);
        if (checksumEnabled && checksumAlgorithm != null) {
            reqBuilder.checksumAlgorithm(checksumAlgorithm);
        }

        if (op.item() instanceof DataItem) {
            DataItem dataItem = (DataItem) op.item();
            dataItem.position(0);
            return s3AsyncClient.putObject(
                            reqBuilder.build(),
                            AsyncRequestBody.fromInputStream(new DataItemInputStream(dataItem), dataItem.size()))
                    .thenApply(response -> null);
        } else if (op.item() instanceof PathItem) {
            PathItem pathItem = (PathItem) op.item();
            Path path = Path.of(pathItem.name());
            return s3AsyncClient.putObject(
                            reqBuilder.build(),
                            AsyncRequestBody.fromFile(path))
                    .thenApply(response -> null);
        } else {
            return CompletableFuture.failedFuture(
                    new UnsupportedOperationException("s3-aws PUT requires DataItem or PathItem"));
        }
    }

    private CompletableFuture<Void> readObject(final O op) {
        final var bk = resolveBucketAndKey(op);

        return s3AsyncClient.getObject(
                        GetObjectRequest.builder()
                                        .bucket(bk[0])
                                        .key(bk[1])
                                        .build(),
                        AsyncResponseTransformer.toBytes())
                .thenAccept(response -> {
                    long bytesRead = response.response().contentLength();
                    if (op instanceof DataOperation) {
                        ((DataOperation) op).countBytesDone(bytesRead);
                    }
                });
    }

    private CompletableFuture<Void> deleteObject(final O op) {
        final var bk = resolveBucketAndKey(op);

        return s3AsyncClient.deleteObject(
                        DeleteObjectRequest.builder()
                                        .bucket(bk[0])
                                        .key(bk[1])
                                        .build())
                .thenApply(response -> null);
    }

    @SuppressWarnings("unchecked")
    private CompletableFuture<Void> listObjects(final O op) {
        final var listOp = (ListOperation<? extends PathItem>) op;
        final var options = listOp.options();

        String targetBucket = bucketName;
        final var srcPath = op.srcPath();
        if (srcPath != null && !srcPath.isEmpty()) {
            final var rel = srcPath.startsWith("/") ? srcPath.substring(1) : srcPath;
            final var slash = rel.indexOf('/');
            targetBucket = slash > 0 ? rel.substring(0, slash) : rel;
        }

        final var maxKeys = options.maxKeys() > 0 ? Math.min(options.maxKeys(), 1000) : 1000;

        ListObjectsV2Request.Builder reqBuilder = ListObjectsV2Request.builder()
                        .bucket(targetBucket)
                        .maxKeys(maxKeys);

        final var prefix = op.item().name();
        if (prefix != null && !prefix.isEmpty()) {
            reqBuilder.prefix(prefix.startsWith("/") ? prefix.substring(1) : prefix);
        }

        final var delimiter = options.delimiter();
        if (delimiter != null && !delimiter.isEmpty()) {
            reqBuilder.delimiter(delimiter);
        }

        final var contToken = options.continuationToken();
        if (contToken != null && !contToken.isEmpty()) {
            reqBuilder.continuationToken(contToken);
        } else {
            final var startAfter = listOp.startAfter();
            if (startAfter != null && !startAfter.isEmpty()) {
                reqBuilder.startAfter(startAfter);
            }
        }

        return s3AsyncClient.listObjectsV2(reqBuilder.build())
                .thenAccept(resp -> {
                    int objectCount = 0;
                    long bytesTotal = 0;
                    String firstKey = null;
                    String lastKey = null;

                    for (S3Object s3obj : resp.contents()) {
                        objectCount++;
                        if (options.fetchMetadata()) {
                            bytesTotal += s3obj.size();
                        }
                        final var key = s3obj.key();
                        if (firstKey == null) {
                            firstKey = key;
                        }
                        lastKey = key;
                    }

                    listOp.objectsListed(objectCount);
                    listOp.bytesListed(options.fetchMetadata() ? bytesTotal : 0);
                    listOp.truncated(Boolean.TRUE.equals(resp.isTruncated()));

                    if (firstKey != null) {
                        listOp.pageFirstKey(firstKey);
                    }
                    listOp.continuationToken(resp.nextContinuationToken());

                    listOp.options(
                                    options.toBuilder()
                                                    .continuationToken(resp.nextContinuationToken())
                                                    .build());

                    if (lastKey != null) {
                        listOp.startAfter(lastKey);
                    }
                    listOp.countBytesDone(listOp.bytesListed());
                });
    }

    // ... [list methods unchanged - they already use synchronous calls, can remain synchronous] ...
    // Note: The list() methods are called by the framework directly, not through invokeNio(),
    // so they can remain synchronous using s3AsyncClient.listObjectsV2().join()

    @Override
    public com.dell.spt.base.storage.driver.ListDiscoveryProbe.DiscoverResult probeCommonPrefixes(
                    final String bucketPath,
                    final String prefix,
                    final String delimiter,
                    final int maxKeys) throws IOException {
        try {
            String targetBucket = bucketName;
            if (bucketPath != null && !bucketPath.isEmpty()) {
                if (bucketPath.startsWith("/")) {
                    targetBucket = bucketPath.substring(1);
                } else {
                    targetBucket = bucketPath;
                }
            }

            ListObjectsV2Request.Builder reqBuilder = ListObjectsV2Request.builder()
                            .bucket(targetBucket)
                            .maxKeys(Math.min(Math.max(maxKeys, 1), 1000));

            if (prefix != null && !prefix.isEmpty()) {
                reqBuilder.prefix(prefix);
            }
            if (delimiter != null && !delimiter.isEmpty()) {
                reqBuilder.delimiter(delimiter);
            }

            ListObjectsV2Response resp = s3AsyncClient.listObjectsV2(reqBuilder.build()).join();

            List<String> commonPrefixes = resp.commonPrefixes().stream()
                            .map(CommonPrefix::prefix)
                            .collect(Collectors.toList());

            boolean truncated = Boolean.TRUE.equals(resp.isTruncated());
            boolean hasContents = resp.contents() != null && !resp.contents().isEmpty();
            return new com.dell.spt.base.storage.driver.ListDiscoveryProbe.DiscoverResult(
                            commonPrefixes, hasContents, truncated);
        } catch (Exception e) {
            throw new IOException("Failed to probe common prefixes", e);
        }
    }

    public String getBucketName() {
        return bucketName;
    }
}
```

---

## 5. Migration Strategy

### 5.1 Implementation Phases

**Phase 1: Dependency Updates**
1. Update `gradle/libs.versions.toml` with aws-crt version
2. Update `build.gradle` to add aws-crt dependency
3. Remove Apache client exclusions if CRT needs Netty
4. Test build compilation

**Phase 2: Factory Migration**
1. Update `S3AwsStorageDriverFactory.java` to use `S3AsyncClient.crtBuilder()`
2. Add CRT performance tuning parameters
3. Test client creation and configuration

**Phase 3: Driver Migration**
1. Update `S3AwsStorageDriver.java` to accept `S3AsyncClient`
2. Convert operations to async with CompletableFuture
3. Maintain blocking behavior in `invokeNio()` for compatibility
4. Test individual operations (PUT, GET, DELETE, LIST)

**Phase 4: Testing**
1. Unit tests with mock S3AsyncClient
2. Integration tests with real S3 endpoint
3. Performance benchmarking vs. current implementation
4. Validation with S3-compatible services (MinIO, Ceph)

**Phase 5: Documentation**
1. Update README.md with CRT configuration options
2. Document performance tuning parameters
3. Update migration guide if needed

### 5.2 Risk Mitigation

**Potential Issues:**
1. **Native library loading** - aws-crt requires platform-specific native libraries
   - **Mitigation:** Test on all target platforms (Linux x86_64, ARM64)
   - **Fallback:** Keep Apache client as runtime dependency for compatibility

2. **Async complexity** - Converting synchronous operations to async
   - **Mitigation:** Use `.join()` to block on async results, preserving existing threading model
   - **Fallback:** Implement synchronous wrapper if async proves problematic

3. **Configuration compatibility** - Some S3Configuration options may not be supported by CRT builder
   - **Mitigation:** Test all current configuration options
   - **Fallback:** Document unsupported options and provide alternatives

4. **S3-compatible service compatibility** - CRT may have different behavior with non-AWS endpoints
   - **Mitigation:** Extensive testing with MinIO, Ceph, SeaweedFS
   - **Fallback:** Configuration option to disable CRT for specific endpoints

### 5.3 Rollback Plan

If CRT migration encounters issues:
1. Keep Apache client as runtime dependency
2. Add configuration flag to switch between CRT and Apache client
3. Implement factory method to create either client type based on config
4. Maintain current synchronous implementation as fallback

---

## 6. Performance Tuning Parameters

### 6.1 CRT-Specific Settings

```java
.targetThroughputInGbps(20.0)           // Target network throughput
.minimumPartSizeInBytes(8 * 1024 * 1024L)  // 8 MB minimum part size for multipart
```

**Recommendations:**
- `targetThroughputInGbps`: Set based on network capacity (10-100 Gbps typical)
- `minimumPartSizeInBytes`: 8 MB default, increase for very large objects (>1 GB)

### 6.2 Connection Management

CRT handles connection management automatically, but can be tuned:
```java
.maxConcurrency(128)  // Maximum concurrent operations
```

### 6.3 Monitoring

CRT provides built-in metrics. Consider adding:
- Upload/download throughput
- Multipart operation statistics
- Connection pool utilization
- Native library health

---

## 7. Testing Checklist

- [ ] Build compiles with new dependencies
- [ ] Unit tests pass with mock S3AsyncClient
- [ ] Integration tests pass with AWS S3
- [ ] Integration tests pass with MinIO
- [ ] Integration tests pass with Ceph
- [ ] Performance benchmark shows improvement
- [ ] Error handling works correctly for all operations
- [ ] Checksum validation still works
- [ ] List operations work correctly
- [ ] Path-style access works for S3-compatible services
- [ ] Native libraries load correctly on target platforms
- [ ] No memory leaks under sustained load
- [ ] Graceful degradation if CRT fails to initialize

---

## 8. References

- [AWS CRT-based S3 Client Documentation](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/crt-based-s3-client.html)
- [AWS CRT HTTP Client Configuration](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/http-configuration-crt.html)
- [aws-crt-java GitHub Repository](https://github.com/awslabs/aws-crt-java)
- [SPT AWS SDK Investigation Report](SPT_AWS_SDK_Investigation_Report.docx)

---

## 9. Next Steps

1. **Review this analysis** with the team
2. **Approve migration strategy** and timeline
3. **Begin Phase 1** (dependency updates)
4. **Implement changes incrementally** following the phases
5. **Test thoroughly** at each phase
6. **Document lessons learned** during migration

