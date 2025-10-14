package com.dell.spt.storage.driver.coop.nio.fs;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class FileStorageDriverExtensionTest {

	@Test
	public void idIsFs() {
		final FileStorageDriverExtension<Item, Operation<Item>, FileStorageDriver<Item, Operation<Item>>> ext = new FileStorageDriverExtension<>();
		assertEquals("fs", ext.id());
	}

	@Test
	public void defaultsAndSchemaAreNullAndResourcesEmpty() {
		final FileStorageDriverExtension<Item, Operation<Item>, FileStorageDriver<Item, Operation<Item>>> ext = new FileStorageDriverExtension<>();
		assertNull(ext.schemaProvider());
		assertNull(ext.defaultsFileName());
		assertTrue(ext.resourceFilesToInstall().isEmpty());
	}
}
