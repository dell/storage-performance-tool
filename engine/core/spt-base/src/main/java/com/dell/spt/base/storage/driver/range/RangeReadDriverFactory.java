package com.dell.spt.base.storage.driver.range;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.range.RangeReadPolicy;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.StorageDriverFactory;
import com.github.akurilov.confuse.Config;

/** Explicit construction-time opt-in; existing factories retain their ordinary API. */
public interface RangeReadDriverFactory<I extends Item, O extends Operation<I>, T extends StorageDriver<I, O>>
				extends StorageDriverFactory<I, O, T> {
	/** Create a range-only driver with the supplied immutable policy and matching runtime. */
	T createRangeRead(String stepId, DataInput dataInput, Config storageConfig,
					int batchSize, RangeReadPolicy policy) throws IllegalConfigurationException, InterruptedException;
}
