package com.dell.spt.storage.driver.coop.aws.s3;

/**
 * Configuration class for AWS S3 Storage Driver
 */
public class S3StorageDriverConfig {
    
    private String accessKey;
    private String secretKey;
    private String region;
    private String bucketName;
    private String endpointOverride;
    private boolean pathStyleAccess;
    private int maxConnections = 50;
    private int socketTimeout = 30000;
    private int connectionTimeout = 10000;
    private boolean enableRequestMetrics = false;

    public S3StorageDriverConfig() {
    }

    public S3StorageDriverConfig(String accessKey, String secretKey, String region, String bucketName) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.region = region;
        this.bucketName = bucketName;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getEndpointOverride() {
        return endpointOverride;
    }

    public void setEndpointOverride(String endpointOverride) {
        this.endpointOverride = endpointOverride;
    }

    public boolean isPathStyleAccess() {
        return pathStyleAccess;
    }

    public void setPathStyleAccess(boolean pathStyleAccess) {
        this.pathStyleAccess = pathStyleAccess;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public boolean isEnableRequestMetrics() {
        return enableRequestMetrics;
    }

    public void setEnableRequestMetrics(boolean enableRequestMetrics) {
        this.enableRequestMetrics = enableRequestMetrics;
    }

    @Override
    public String toString() {
        return "S3StorageDriverConfig{" +
                "accessKey='" + (accessKey != null ? "***" : "null") + '\'' +
                ", secretKey='" + (secretKey != null ? "***" : "null") + '\'' +
                ", region='" + region + '\'' +
                ", bucketName='" + bucketName + '\'' +
                ", endpointOverride='" + endpointOverride + '\'' +
                ", pathStyleAccess=" + pathStyleAccess +
                ", maxConnections=" + maxConnections +
                ", socketTimeout=" + socketTimeout +
                ", connectionTimeout=" + connectionTimeout +
                ", enableRequestMetrics=" + enableRequestMetrics +
                '}';
    }
}
