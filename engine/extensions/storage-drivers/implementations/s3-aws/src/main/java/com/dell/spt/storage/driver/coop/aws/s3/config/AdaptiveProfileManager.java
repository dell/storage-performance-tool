package com.dell.spt.storage.driver.coop.aws.s3.config;

import com.dell.spt.storage.driver.coop.aws.s3.metrics.WorkloadMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Adaptive profile manager that dynamically adjusts the configuration profile
 * based on workload characteristics collected by WorkloadMetrics.
 */
public class AdaptiveProfileManager {
	private static final Logger LOG = LoggerFactory.getLogger(AdaptiveProfileManager.class);

	private final WorkloadMetrics metrics;
	private final AtomicReference<ObjectSizeProfile> currentProfile;
	private final ScheduledExecutorService tuner;
	private final long tuningIntervalSeconds;
	private volatile boolean enabled = false;
	private volatile boolean shutdown = false;

	/**
	 * Create a new adaptive profile manager.
	 *
	 * @param metrics The workload metrics collector
	 * @param tuningIntervalSeconds The interval between profile adjustments (seconds)
	 * @param initialProfile The initial profile to use
	 */
	public AdaptiveProfileManager(
					final WorkloadMetrics metrics,
					final long tuningIntervalSeconds,
					final ObjectSizeProfile initialProfile) {

		this.metrics = metrics;
		this.tuningIntervalSeconds = tuningIntervalSeconds;
		this.currentProfile = new AtomicReference<>(initialProfile);
		this.tuner = Executors.newSingleThreadScheduledExecutor(r -> {
			Thread t = new Thread(r, "adaptive-profile-tuner");
			t.setDaemon(true);
			return t;
		});

		LOG.info("AdaptiveProfileManager initialized with interval: {}s, initial profile: {}",
						tuningIntervalSeconds, initialProfile);
	}

	/**
	 * Start the adaptive tuning process.
	 */
	public void start() {
		if (enabled || shutdown) {
			return;
		}

		enabled = true;
		tuner.scheduleAtFixedRate(this::adjustProfile, tuningIntervalSeconds, tuningIntervalSeconds, TimeUnit.SECONDS);
		LOG.info("AdaptiveProfileManager started");
	}

	/**
	 * Stop the adaptive tuning process.
	 */
	public void stop() {
		if (!enabled || shutdown) {
			return;
		}

		enabled = false;
		tuner.shutdown();
		try {
			if (!tuner.awaitTermination(5, TimeUnit.SECONDS)) {
				tuner.shutdownNow();
			}
		} catch (InterruptedException e) {
			tuner.shutdownNow();
			Thread.currentThread().interrupt();
		}
		LOG.info("AdaptiveProfileManager stopped");
	}

	/**
	 * Shutdown the profile manager permanently.
	 */
	public void shutdown() {
		shutdown = true;
		stop();
	}

	/**
	 * Get the current profile.
	 *
	 * @return The current object size profile
	 */
	public ObjectSizeProfile getCurrentProfile() {
		return currentProfile.get();
	}

	/**
	 * Manually set the current profile.
	 * This is useful for testing or manual overrides.
	 *
	 * @param profile The profile to set
	 */
	public void setCurrentProfile(final ObjectSizeProfile profile) {
		ObjectSizeProfile oldProfile = currentProfile.getAndSet(profile);
		if (oldProfile != profile) {
			LOG.info("Profile manually changed from {} to {}", oldProfile, profile);
		}
	}

	/**
	 * Adjust the profile based on current workload metrics.
	 */
	private void adjustProfile() {
		if (!enabled || shutdown) {
			return;
		}

		try {
			long totalOps = metrics.getTotalOperations();
			if (totalOps < 100) {
				// Not enough data to make a decision
				LOG.trace("Not enough operations ({}) for profile adjustment", totalOps);
				return;
			}

			double avgSize = metrics.getAverageObjectSize();
			ObjectSizeProfile newProfile = ObjectSizeProfile.fromSize((long) avgSize);
			ObjectSizeProfile oldProfile = currentProfile.get();

			if (newProfile != oldProfile) {
				LOG.info("Adjusting profile from {} to {} based on average object size: {} ({} ops, {} bytes)",
								oldProfile, newProfile, String.format("%.2fKB", avgSize / 1024.0),
								totalOps, metrics.getTotalBytes());
				currentProfile.set(newProfile);
			} else {
				LOG.trace("Profile remains {} (avg size: {}, ops: {})",
								oldProfile, String.format("%.2fKB", avgSize / 1024.0), totalOps);
			}

			// Log summary periodically
			if (totalOps % 1000 == 0) {
				LOG.info("Workload summary: {}", metrics.getSummary());
			}

		} catch (Exception e) {
			LOG.error("Error during profile adjustment", e);
		}
	}

	/**
	 * Check if adaptive tuning is enabled.
	 *
	 * @return true if enabled, false otherwise
	 */
	public boolean isEnabled() {
		return enabled;
	}

	/**
	 * Check if the manager is shutdown.
	 *
	 * @return true if shutdown, false otherwise
	 */
	public boolean isShutdown() {
		return shutdown;
	}
}
