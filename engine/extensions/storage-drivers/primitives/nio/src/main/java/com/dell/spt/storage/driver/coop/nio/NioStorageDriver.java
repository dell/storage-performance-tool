package com.dell.spt.storage.driver.coop.nio;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.storage.driver.StorageDriver;

/**
Created by andrey on 12.05.17.
*/
public interface NioStorageDriver<I extends Item, O extends Operation<I>>
				extends StorageDriver<I, O> {

	int MIN_TASK_BUFF_CAPACITY = 0x1000;
}
