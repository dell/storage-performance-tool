package com.dell.spt.storage.driver.coop.netty.http.s3;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.storage.driver.bucket.TargetBucketControl;
import com.dell.spt.base.storage.driver.bucket.TargetBucketResponse;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.io.IOException;
import org.apache.logging.log4j.Level;

/**
 * Target-bucket control client of the Netty S3 family, {@code s3} and {@code s3-rdma}.
 *
 * <p>It owns a plain HTTP S3 driver built from the step's storage configuration, so it reuses the
 * driver's endpoint, TLS, signing, region, global headers and URI arguments, and needs no RDMA
 * hardware. The driver is never started and is closed with the client.
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
		return new S3TargetBucketControl(
						new S3StorageDriver<Item, Operation<Item>>(stepId, null, controlConfig(storageConfig), false, 1));
	}

	/** A copy of the storage configuration with default endpoint selection and a bounded timeout. */
	static Config controlConfig(final Config storageConfig) {
		final var config = new BasicConfig(storageConfig);
		config.val("net-endpoint-selection", "default");
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
