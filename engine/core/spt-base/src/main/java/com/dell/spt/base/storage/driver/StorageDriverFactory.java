package com.dell.spt.base.storage.driver;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.github.akurilov.confuse.Config;

public interface StorageDriverFactory<I extends Item, O extends Operation<I>, T extends StorageDriver<I, O>>
				extends Extension {

	T create(
					final String stepId,
					final DataInput dataInput,
					final Config storageConfig,
					final boolean verifyFlag,
					final int batchSize)
					throws IllegalConfigurationException, InterruptedException;
}
