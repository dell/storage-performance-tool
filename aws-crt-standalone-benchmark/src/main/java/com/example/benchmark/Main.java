package com.example.benchmark;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;

import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final String ENDPOINT = "http://10.246.190.64:8333";
    private static final String ACCESS_KEY = "admin";
    private static final String SECRET_KEY = "admin123";
    private static final String BUCKET = "admin";
    private static final String REGION = "us-east-1";
    private static final int WARMUP_ITERATIONS = 5;
    private static final int MEASURED_ITERATIONS = 30;
    private static int CONCURRENCY = 256;

    public static void main(String[] args) throws Exception {
        // Parse command line arguments
        if (args.length > 0) {
            try {
                CONCURRENCY = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid thread count: " + args[0]);
                System.err.println("Usage: java -jar benchmark.jar [thread_count]");
                System.exit(1);
            }
        }

        System.out.println("AWS CRT Standalone Benchmark");
        System.out.println("================================");
        System.out.println("Endpoint: " + ENDPOINT);
        System.out.println("Bucket: " + BUCKET);
        System.out.println("Concurrency: " + CONCURRENCY);
        System.out.println("Warmup iterations: " + WARMUP_ITERATIONS);
        System.out.println("Measured iterations: " + MEASURED_ITERATIONS);
        System.out.println();

        // Create S3 client with CRT
        S3AsyncClient s3Client = S3AsyncClient.crtBuilder()
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .region(Region.of(REGION))
                .endpointOverride(URI.create(ENDPOINT))
                .forcePathStyle(true)
                .targetThroughputInGbps(10.0)
                .minimumPartSizeInBytes(8 * 1024 * 1024L)
                .maxConcurrency(CONCURRENCY)
                .build();

        // Test different object sizes
        int[] sizes = {1024, 102400, 1048576}; // 1KB, 100KB, 1MB
        String[] sizeNames = {"1KB", "100KB", "1MB"};

        for (int i = 0; i < sizes.length; i++) {
            int size = sizes[i];
            String sizeName = sizeNames[i];

            System.out.println("\n=== Testing " + sizeName + " Objects ===");

            // Upload benchmark
            runUploadBenchmark(s3Client, size, sizeName);

            // Download benchmark
            runDownloadBenchmark(s3Client, size, sizeName);

            // Cleanup
            cleanupObject(s3Client, size, sizeName);
        }

        s3Client.close();
        System.out.println("\nBenchmark completed successfully!");
    }

    private static void runUploadBenchmark(S3AsyncClient s3Client, int size, String sizeName) {
        System.out.println("\n--- Upload Benchmark (" + sizeName + ") ---");
        System.out.println("Mode: Sequential (single-threaded, CRT maxConcurrency=" + CONCURRENCY + ")");

        String keyPrefix = "benchmark-upload-" + sizeName + "-" + System.currentTimeMillis();
        byte[] data = generateRandomData(size);

        // Warmup
        System.out.println("Warming up...");
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            uploadObject(s3Client, keyPrefix + "-warmup-" + i, data).join();
        }

        // Measured runs - execute sequentially to get baseline
        System.out.println("Running measured iterations...");
        List<Long> latencies = new ArrayList<>();
        List<String> keysToDelete = new ArrayList<>();
        Instant start = Instant.now();

        for (int i = 0; i < MEASURED_ITERATIONS; i++) {
            String iterationKey = keyPrefix + "-" + i;
            keysToDelete.add(iterationKey);
            
            // Execute upload and measure latency
            Instant iterStart = Instant.now();
            uploadObject(s3Client, iterationKey, data).join();
            Instant iterEnd = Instant.now();
            long latency = Duration.between(iterStart, iterEnd).toMillis();
            latencies.add(latency);
        }

        Instant end = Instant.now();
        double totalDurationSec = Duration.between(start, end).toMillis() / 1000.0;
        double totalBytes = (double) size * MEASURED_ITERATIONS;
        double throughputMbps = (totalBytes * 8) / (totalDurationSec * 1_000_000);
        double opsPerSecond = MEASURED_ITERATIONS / totalDurationSec;

        // Cleanup
        System.out.println("Cleaning up...");
        for (String key : keysToDelete) {
            deleteObject(s3Client, key).join();
        }

        // Calculate statistics
        double avgLatency = latencies.stream().mapToLong(Long::longValue).average().orElse(0);
        latencies.sort(Long::compareTo);
        double p50Latency = latencies.get(latencies.size() / 2);
        double p95Latency = latencies.get((int) (latencies.size() * 0.95));
        double p99Latency = latencies.get((int) (latencies.size() * 0.99));

        System.out.printf("Upload Results (%s):%n", sizeName);
        System.out.printf("  Total Duration: %.3f s%n", totalDurationSec);
        System.out.printf("  Operations/sec: %.2f ops/s%n", opsPerSecond);
        System.out.printf("  Throughput: %.2f Mbps%n", throughputMbps);
        System.out.printf("  Avg Latency: %.2f ms%n", avgLatency);
        System.out.printf("  P50 Latency: %.2f ms%n", p50Latency);
        System.out.printf("  P95 Latency: %.2f ms%n", p95Latency);
        System.out.printf("  P99 Latency: %.2f ms%n", p99Latency);
    }

    private static void runDownloadBenchmark(S3AsyncClient s3Client, int size, String sizeName) {
        System.out.println("\n--- Download Benchmark (" + sizeName + ") ---");
        System.out.println("Mode: Sequential (single-threaded, CRT maxConcurrency=" + CONCURRENCY + ")");

        String key = "benchmark-download-" + sizeName + "-" + System.currentTimeMillis();
        byte[] data = generateRandomData(size);

        // Upload the object first
        uploadObject(s3Client, key, data).join();

        // Warmup
        System.out.println("Warming up...");
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            downloadObject(s3Client, key).join();
        }

        // Measured runs - execute sequentially to get baseline
        System.out.println("Running measured iterations...");
        List<Long> latencies = new ArrayList<>();
        Instant start = Instant.now();

        for (int i = 0; i < MEASURED_ITERATIONS; i++) {
            Instant iterStart = Instant.now();
            downloadObject(s3Client, key).join();
            Instant iterEnd = Instant.now();
            long latency = Duration.between(iterStart, iterEnd).toMillis();
            latencies.add(latency);
        }

        Instant end = Instant.now();
        double totalDurationSec = Duration.between(start, end).toMillis() / 1000.0;
        double totalBytes = (double) size * MEASURED_ITERATIONS;
        double throughputMbps = (totalBytes * 8) / (totalDurationSec * 1_000_000);
        double opsPerSecond = MEASURED_ITERATIONS / totalDurationSec;

        // Calculate statistics
        double avgLatency = latencies.stream().mapToLong(Long::longValue).average().orElse(0);
        latencies.sort(Long::compareTo);
        double p50Latency = latencies.get(latencies.size() / 2);
        double p95Latency = latencies.get((int) (latencies.size() * 0.95));
        double p99Latency = latencies.get((int) (latencies.size() * 0.99));

        System.out.printf("Download Results (%s):%n", sizeName);
        System.out.printf("  Total Duration: %.3f s%n", totalDurationSec);
        System.out.printf("  Operations/sec: %.2f ops/s%n", opsPerSecond);
        System.out.printf("  Throughput: %.2f Mbps%n", throughputMbps);
        System.out.printf("  Avg Latency: %.2f ms%n", avgLatency);
        System.out.printf("  P50 Latency: %.2f ms%n", p50Latency);
        System.out.printf("  P95 Latency: %.2f ms%n", p95Latency);
        System.out.printf("  P99 Latency: %.2f ms%n", p99Latency);

        // Cleanup
        deleteObject(s3Client, key).join();
    }

    private static CompletableFuture<Void> uploadObject(S3AsyncClient s3Client, String key, byte[] data) {
        return s3Client.putObject(
                builder -> builder.bucket(BUCKET).key(key),
                AsyncRequestBody.fromByteBuffer(ByteBuffer.wrap(data))
        ).thenApply(response -> null);
    }

    private static CompletableFuture<byte[]> downloadObject(S3AsyncClient s3Client, String key) {
        return s3Client.getObject(
                builder -> builder.bucket(BUCKET).key(key),
                AsyncResponseTransformer.toBytes()
        ).thenApply(response -> response.asByteArray());
    }

    private static CompletableFuture<Void> deleteObject(S3AsyncClient s3Client, String key) {
        return s3Client.deleteObject(
                DeleteObjectRequest.builder().bucket(BUCKET).key(key).build()
        ).thenApply(response -> null);
    }

    private static void cleanupObject(S3AsyncClient s3Client, int size, String sizeName) {
        String key = "benchmark-upload-" + sizeName;
        // Try to delete any remaining objects from previous runs
        try {
            deleteObject(s3Client, key).join();
        } catch (Exception e) {
            // Ignore if object doesn't exist
        }
    }

    private static byte[] generateRandomData(int size) {
        byte[] data = new byte[size];
        new Random().nextBytes(data);
        return data;
    }
}
