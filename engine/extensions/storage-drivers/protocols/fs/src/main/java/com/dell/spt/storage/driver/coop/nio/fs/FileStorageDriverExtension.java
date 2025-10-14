package com.dell.spt.storage.driver.coop.nio.fs;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.ExtensionBase;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.storage.driver.StorageDriverFactory;

import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class FileStorageDriverExtension<I extends Item, O extends Operation<I>, T extends FileStorageDriver<I, O>>
				extends ExtensionBase
				implements StorageDriverFactory<I, O, T> {

	private static final String NAME = "fs";
	private static final List<String> RES_INSTALL_FILES = Collections.unmodifiableList(
					Arrays.asList());

	@Override
	public String id() {
		return NAME;
	}

	@Override
	@SuppressWarnings("unchecked")
	public T create(
					final String stepId, final DataInput dataInput, final Config storageConfig, final boolean verifyFlag,
					final int batchSize) throws IllegalConfigurationException, InterruptedException {
		return (T) new FileStorageDriver<I, O>(stepId, dataInput, storageConfig, verifyFlag, batchSize);
	}

	@Override
	public final SchemaProvider schemaProvider() {
		return null;
	}

	@Override
	protected final String defaultsFileName() {
		return null;
	}

	@Override
	protected final List<String> resourceFilesToInstall() {
		return RES_INSTALL_FILES;
	}
}
