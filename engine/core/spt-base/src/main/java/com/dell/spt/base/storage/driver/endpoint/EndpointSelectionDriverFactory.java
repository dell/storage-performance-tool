package com.dell.spt.base.storage.driver.endpoint;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.StorageDriverFactory;

/**
 * Explicit construction-time opt-in for a non-default {@code storage.net.endpoint.selection}.
 * Factories without this marker are rejected before construction when a non-default mode is
 * configured; sharing a schema or a base class does not imply support.
 */
public interface EndpointSelectionDriverFactory<I extends Item, O extends Operation<I>, T extends StorageDriver<I, O>>
				extends StorageDriverFactory<I, O, T> {}
