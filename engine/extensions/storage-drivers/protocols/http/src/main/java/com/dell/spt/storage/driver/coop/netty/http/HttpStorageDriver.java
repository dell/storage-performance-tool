package com.dell.spt.storage.driver.coop.netty.http;

import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.Item;

import com.dell.spt.storage.driver.coop.netty.NettyStorageDriver;

/**
Created by kurila on 30.08.16.
*/
public interface HttpStorageDriver<I extends Item, O extends Operation<I>>
				extends NettyStorageDriver<I, O> {

	int REQ_LINE_LEN = 1024;
	int HEADERS_LEN = 2048;

	String KEY_CONTENT = "content";
}
