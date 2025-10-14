package com.dell.spt.storage.driver.coop.netty.http.swift;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SwiftStorageDriverExtensionTest {

	@Test
	public void idIsSwift() {
		final SwiftStorageDriverExtension<Item, Operation<Item>, SwiftStorageDriver<Item, Operation<Item>>> ext = new SwiftStorageDriverExtension<>();
		assertEquals("swift", ext.id());
	}

	@Test
	public void schemaAndDefaultsAndResourcesPresent() {
		final SwiftStorageDriverExtension<Item, Operation<Item>, SwiftStorageDriver<Item, Operation<Item>>> ext = new SwiftStorageDriverExtension<>();
		assertNotNull(ext.schemaProvider(), "Swift extension should provide a schema provider");
		assertEquals("defaults-storage-swift.yaml", ext.defaultsFileName());
		assertFalse(ext.resourceFilesToInstall().isEmpty());
		assertTrue(ext.resourceFilesToInstall().contains("config/defaults-storage-swift.yaml"));
	}
}
