package com.dell.spt.storage.driver.coop.netty.http.s3.tables;

import com.dell.spt.base.logging.Loggers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.function.LongSupplier;

/**
 * Implements the {@code tableCompactionPoll} opMode:
 *
 * <ol>
 *   <li>Trigger compaction via PutTableMaintenanceConfiguration.</li>
 *   <li>Poll GetTableMetadataLocation → fetch metadata JSON → read snapshot summary
 *       {@code total-data-files} field.</li>
 *   <li>Converge when the file count is ≤ {@code targetFileCount} for
 *       {@code stabilizationPolls} consecutive polls, or until {@code timeoutMs} elapses.</li>
 *   <li>Record compaction stats into the driver via {@link S3TablesStorageDriver#recordCompactionStats}.</li>
 * </ol>
 *
 * <p>File count convergence target: {@code ceil(totalIngestBytes / targetFileSizeBytes) * toleranceFactor}.
 *
 * <p>Thread-safety: one instance is created per {@code tableCompactionPoll} step and
 * {@link #poll()} is called once from the single dispatch thread.
 */
final class CompactionPoller {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private final S3TablesStorageDriver<?, ?> driver;
	private final long targetFileCount;
	private final int stabilizationPolls;
	private final long pollIntervalMs;
	private final long timeoutMs;
	private final LongSupplier clockMillis;
	private final Sleeper sleeper;

	@FunctionalInterface
	interface Sleeper {
		void sleep(long millis) throws Exception;
	}

	CompactionPoller(
					final S3TablesStorageDriver<?, ?> driver,
					final long totalIngestBytes,
					final long targetFileSizeBytes,
					final double toleranceFactor,
					final int stabilizationPolls,
					final long pollIntervalMs,
					final long timeoutMs) {
		this(driver, totalIngestBytes, targetFileSizeBytes, toleranceFactor, stabilizationPolls,
						pollIntervalMs, timeoutMs, System::currentTimeMillis, Thread::sleep);
	}

	CompactionPoller(
					final S3TablesStorageDriver<?, ?> driver,
					final long totalIngestBytes,
					final long targetFileSizeBytes,
					final double toleranceFactor,
					final int stabilizationPolls,
					final long pollIntervalMs,
					final long timeoutMs,
					final LongSupplier clockMillis,
					final Sleeper sleeper) {
		this.driver = driver;
		this.targetFileCount = (long) Math.ceil((double) totalIngestBytes / targetFileSizeBytes * toleranceFactor);
		this.stabilizationPolls = stabilizationPolls;
		this.pollIntervalMs = pollIntervalMs;
		this.timeoutMs = timeoutMs;
		this.clockMillis = clockMillis;
		this.sleeper = sleeper;
	}

	/**
	 * Triggers compaction and polls until convergence or timeout.
	 * Returns {@code true} on convergence, {@code false} on timeout.
	 */
	boolean poll() throws Exception {
		final long startMs = clockMillis.getAsLong();

		driver.getControlPlane().putTableMaintenanceConfiguration();
		Loggers.MSG.info("{}: compaction triggered, polling for convergence (targetFileCount={}, stabilizationPolls={})",
						driver.getStepId(), targetFileCount, stabilizationPolls);

		long filesBefore = -1;
		int stableCount = 0;
		long lastFileCount = Long.MAX_VALUE;

		while (true) {
			final long elapsed = clockMillis.getAsLong() - startMs;
			if (elapsed >= timeoutMs) {
				Loggers.MSG.warn("{}: compaction poll timed out after {}ms, last fileCount={}",
								driver.getStepId(), elapsed, lastFileCount);
				driver.recordCompactionStats(startMs, 0, filesBefore, lastFileCount);
				return false;
			}

			sleeper.sleep(pollIntervalMs);

			final long fileCount;
			try {
				fileCount = fetchCurrentFileCount();
			} catch (final Exception e) {
				Loggers.MSG.warn("{}: compaction poll error fetching file count: {}", driver.getStepId(), e.getMessage());
				continue;
			}

			if (filesBefore < 0) {
				filesBefore = fileCount;
				Loggers.MSG.info("{}: compaction poll started, initial fileCount={}", driver.getStepId(), filesBefore);
			}
			lastFileCount = fileCount;

			if (fileCount <= targetFileCount) {
				stableCount++;
				Loggers.MSG.debug("{}: compaction converging: fileCount={} ≤ target={}, stable={}/{}",
								driver.getStepId(), fileCount, targetFileCount, stableCount, stabilizationPolls);
				if (stableCount >= stabilizationPolls) {
					final long completeMs = clockMillis.getAsLong();
					Loggers.MSG.info("{}: compaction converged in {}ms, fileCount={} (was {})",
									driver.getStepId(), completeMs - startMs, fileCount, filesBefore);
					driver.recordCompactionStats(startMs, completeMs, filesBefore, fileCount);
					return true;
				}
			} else {
				if (stableCount > 0) {
					Loggers.MSG.debug("{}: compaction not yet stable, resetting count (fileCount={} > target={})",
									driver.getStepId(), fileCount, targetFileCount);
				}
				stableCount = 0;
			}
		}
	}

	/**
	 * Fetches the current snapshot's {@code total-data-files} from the table metadata JSON.
	 * Uses the snapshot summary field to avoid fetching/parsing Avro manifests.
	 */
	private long fetchCurrentFileCount() throws Exception {
		final IcebergCommitter.MetadataLocationResult loc = driver.getControlPlane().getTableMetadataLocation();
		if (loc.metadataLocation == null || loc.metadataLocation.isEmpty()) {
			throw new Exception("GetTableMetadataLocation returned empty metadataLocation");
		}
		final String s3Path = metadataLocationToS3Path(loc.metadataLocation);
		final byte[] metaBytes = driver.executeDataPlaneGet(s3Path);
		final JsonNode meta = MAPPER.readTree(metaBytes);

		// Walk snapshots to find the current one, then read its summary
		final long currentSnapshotId = meta.path("current-snapshot-id").asLong(-1);
		if (currentSnapshotId < 0) {
			return 0;
		}
		final JsonNode snapshots = meta.path("snapshots");
		for (final JsonNode snap : snapshots) {
			if (snap.path("snapshot-id").asLong() == currentSnapshotId) {
				final String totalFiles = snap.path("summary").path("total-data-files").asText("0");
				return Long.parseLong(totalFiles);
			}
		}
		return 0;
	}

	/**
	 * Converts an S3 URI (s3://bucket/key or s3a://bucket/key) to the HTTP path /bucket/key.
	 */
	static String metadataLocationToS3Path(final String s3Uri) {
		if (s3Uri.startsWith("s3://")) {
			return "/" + s3Uri.substring(5);
		}
		if (s3Uri.startsWith("s3a://")) {
			return "/" + s3Uri.substring(6);
		}
		return s3Uri;
	}
}
