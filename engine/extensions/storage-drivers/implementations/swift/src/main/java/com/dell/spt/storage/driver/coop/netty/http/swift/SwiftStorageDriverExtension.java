package com.dell.spt.storage.driver.coop.netty.http.swift;

import static com.dell.spt.base.Constants.APP_NAME;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.ExtensionBase;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.storage.driver.StorageDriverFactory;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.io.yaml.YamlSchemaProviderBase;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SwiftStorageDriverExtension<I extends Item, O extends Operation<I>, T extends SwiftStorageDriver<I, O>>
				extends ExtensionBase
				implements StorageDriverFactory<I, O, T> {

	private static final String NAME = "swift";
	private static final String DEFAULTS_FILE_NAME = "defaults-storage-swift.yaml";
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
		return (T) new SwiftStorageDriver<>(stepId, dataInput, storageConfig, verifyFlag, batchSize);
	}

	@Override
	public final SchemaProvider schemaProvider() {
		return new YamlSchemaProviderBase() {
			@Override
			protected final InputStream schemaInputStream() {
				return getClass().getResourceAsStream("/config-schema-storage-swift.yaml");
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
