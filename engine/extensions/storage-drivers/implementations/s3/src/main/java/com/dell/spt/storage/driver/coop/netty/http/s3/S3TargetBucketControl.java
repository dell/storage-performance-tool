package com.dell.spt.storage.driver.coop.netty.http.s3;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.storage.driver.bucket.TargetBucketControl;
import com.dell.spt.base.storage.driver.bucket.TargetBucketResponse;
import com.dell.spt.storage.driver.coop.netty.endpoint.EndpointSelectionSettings;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.io.IOException;
import org.apache.logging.log4j.Level;

/**
 * Target-bucket control client of the Netty S3 family, {@code s3} and {@code s3-rdma}.
 *
 * <p>It owns an HTTP S3 driver built from the step's storage configuration as the {@code s3}
 * factory builds it, so it reuses the driver's endpoint, endpoint selection (logical hostname,
 * rotation, TLS SNI and DNS resolver), TLS, signing, region, global headers and URI arguments, and
 * needs no RDMA hardware. The driver is never started and is closed with the client.
 */
public final class S3TargetBucketControl implements TargetBucketControl {

	/** The bound on each request when the step configures no network timeout. */
	static final int UNBOUNDED_TIMEOUT_REPLACEMENT_MILLIS = 30_000;

	private final S3StorageDriver<?, ?> driver;

	S3TargetBucketControl(final S3StorageDriver<?, ?> driver) {
		this.driver = driver;
	}

	/** Opens a control client for the step's storage configuration. */
	public static S3TargetBucketControl open(final String stepId, final Config storageConfig)
					throws IllegalConfigurationException, InterruptedException {
		final var config = controlConfig(storageConfig);
		final var endpointSelection = EndpointSelectionSettings.fromStorage(config);
		final S3StorageDriver<Item, Operation<Item>> driver = endpointSelection.isDefault()
						? new S3StorageDriver<>(stepId, null, config, false, 1)
						: S3EndpointSelectionDriver.create(stepId, null, config, false, 1, endpointSelection);
		return new S3TargetBucketControl(driver);
	}

	/** A copy of the storage configuration whose network timeout is bounded. */
	static Config controlConfig(final Config storageConfig) {
		final var config = new BasicConfig(storageConfig);
		if (config.intVal("net-timeoutMilliSec") <= 0) {
			config.val("net-timeoutMilliSec", UNBOUNDED_TIMEOUT_REPLACEMENT_MILLIS);
		}
		return config;
	}

	@Override
	public TargetBucketResponse check(final String bucket, final Credential credential) throws InterruptedException {
		return driver.checkTargetBucket(bucket, credential);
	}

	@Override
	public TargetBucketResponse create(final String bucket, final Credential credential) throws InterruptedException {
		return driver.createTargetBucket(bucket, credential);
	}

	@Override
	public void close() {
		try {
			driver.close();
		} catch (final IOException | RuntimeException e) {
			LogUtil.exception(Level.DEBUG, e, "failed to close the target-bucket control driver");
		}
	}
}
