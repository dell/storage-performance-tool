package com.dell.spt.storage.driver.coop.aws.s3;

// Removed StorageDriver import; now using generic S3StorageDriver
import java.io.IOException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.time.Duration;

/**
 * Factory class for creating AWS S3 Storage Driver instances
 */
public class S3StorageDriverFactory {

    /**
     * Create an S3StorageDriver instance from configuration
     */
    public static <I extends com.dell.spt.base.item.Item, O extends com.dell.spt.base.item.op.Operation<I>> S3StorageDriver<I, O> create(S3StorageDriverConfig config) throws IOException {
        try {
            AwsCredentials credentials = AwsBasicCredentials.create(config.getAccessKey(), config.getSecretKey());
            
            S3ClientBuilder builder = S3Client.builder()
                    .credentialsProvider(StaticCredentialsProvider.create(credentials))
                    .region(Region.of(config.getRegion()));

            // Configure HTTP client if needed
            ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder()
                    .connectionTimeout(Duration.ofMillis(config.getConnectionTimeout()))
                    .socketTimeout(Duration.ofMillis(config.getSocketTimeout()))
                    .maxConnections(config.getMaxConnections());

            builder.httpClientBuilder(httpClientBuilder);

            // Set endpoint override for S3-compatible services
            if (config.getEndpointOverride() != null && !config.getEndpointOverride().isEmpty()) {
                builder.endpointOverride(java.net.URI.create(config.getEndpointOverride()));
            }

            // Configure path-style access if needed
            if (config.isPathStyleAccess()) {
                builder.forcePathStyle(true);
            }

            S3Client s3Client = builder.build();
            return new S3StorageDriver<>(s3Client, config.getBucketName());

        } catch (Exception e) {
            throw new IOException("Failed to create S3StorageDriver", e);
        }
    }

    /**
     * Create an S3StorageDriver instance with basic parameters
     */
    public static <I extends com.dell.spt.base.item.Item, O extends com.dell.spt.base.item.op.Operation<I>> S3StorageDriver<I, O> create(String accessKey, String secretKey, String region, String bucketName) throws IOException {
        S3StorageDriverConfig config = new S3StorageDriverConfig(accessKey, secretKey, region, bucketName);
        return create(config);
    }

    /**
     * Create an S3StorageDriver instance with endpoint override (for S3-compatible services)
     */
    public static <I extends com.dell.spt.base.item.Item, O extends com.dell.spt.base.item.op.Operation<I>> S3StorageDriver<I, O> create(String accessKey, String secretKey, String region, String bucketName, String endpointOverride) throws IOException {
        S3StorageDriverConfig config = new S3StorageDriverConfig(accessKey, secretKey, region, bucketName);
        config.setEndpointOverride(endpointOverride);
        return create(config);
    }

    /**
     * Create an S3StorageDriver instance with custom configuration
     */
    public static <I extends com.dell.spt.base.item.Item, O extends com.dell.spt.base.item.op.Operation<I>> S3StorageDriver<I, O> create(String accessKey, String secretKey, String region, String bucketName, 
                                       String endpointOverride, boolean pathStyleAccess, 
                                       int maxConnections, int socketTimeout, int connectionTimeout) throws IOException {
        S3StorageDriverConfig config = new S3StorageDriverConfig(accessKey, secretKey, region, bucketName);
        config.setEndpointOverride(endpointOverride);
        config.setPathStyleAccess(pathStyleAccess);
        config.setMaxConnections(maxConnections);
        config.setSocketTimeout(socketTimeout);
        config.setConnectionTimeout(connectionTimeout);
        return create(config);
    }
}
