package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.ExtensionBase;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.storage.driver.bucket.TargetBucketControl;
import com.dell.spt.base.storage.driver.bucket.TargetBucketControlFactory;
import com.dell.spt.base.storage.driver.nic.NicSetDriverFactory;
import com.dell.spt.storage.driver.coop.netty.http.s3.S3TargetBucketControl;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetSettings;
import static com.dell.spt.base.Constants.APP_NAME;

import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.io.yaml.YamlSchemaProviderBase;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public final class S3RdmaStorageDriverExtension<I extends Item, O extends Operation<I>, T extends S3RdmaStorageDriver<I, O>>
				extends ExtensionBase
				implements NicSetDriverFactory<I, O, T>, TargetBucketControlFactory {

	static final String NAME = "s3-rdma";
	private static final String DEFAULTS_FILE_NAME = "defaults-storage-s3-rdma.yaml";
	private static final List<String> RES_INSTALL_FILES = Collections.unmodifiableList(
					Arrays.asList(
									"config/" + DEFAULTS_FILE_NAME));

	@Override
	public String id() {
		return NAME;
	}

	@Override
	public T create(
					final String stepId, final DataInput dataInput, final Config storageConfig, final boolean verifyFlag,
					final int batchSize) throws IllegalConfigurationException, InterruptedException {
		final var nicSet = nicSetSettings(storageConfig);
		if (nicSet.isPresent()) {
			return (T) S3RdmaNicSetDriver.create(stepId, dataInput, storageConfig, verifyFlag, batchSize, nicSet.get());
		}
		return (T) new S3RdmaStorageDriver<>(stepId, dataInput, storageConfig, verifyFlag, batchSize);
	}

	/**
	 * The NIC set's settings, with fewer default event loops per NIC while RDMA is enabled (D83),
	 * since payloads then bypass the loops. With {@code storage.rdma.enabled=false} every payload is
	 * HTTP, so the HTTP drivers' default applies.
	 */
	static Optional<NicSetSettings> nicSetSettings(final Config storageConfig) {
		return NicSetSettings.fromStorage(storageConfig, () -> new RdmaConfig(storageConfig.configVal("rdma")).isEnabled()
						? NicSetSettings.defaultRdmaThreadsPerNic()
						: NicSetSettings.defaultThreadsPerNic());
	}

	/** Bucket control requests are plain HTTP, so the controller needs no RDMA device. */
	@Override
	public TargetBucketControl openTargetBucketControl(final String stepId, final Config storageConfig)
					throws IllegalConfigurationException, InterruptedException {
		return S3TargetBucketControl.open(stepId, storageConfig);
	}

	@Override
	public final SchemaProvider schemaProvider() {
		return new YamlSchemaProviderBase() {
			@Override
			protected final InputStream schemaInputStream() {
				return getClass().getResourceAsStream("/config-schema-storage-s3-rdma.yaml");
			}

			@Override
			public final String id() {
				return APP_NAME;
			}
		};
	}

	@Override
	protected final String defaultsFileName() {
		return DEFAULTS_FILE_NAME;
	}

	@Override
	protected final List<String> resourceFilesToInstall() {
		return RES_INSTALL_FILES;
	}
}
