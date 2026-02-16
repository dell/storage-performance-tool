package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class S3RdmaStorageDriverExtensionTest {

	@Test
	void testId() {
		final var ext = new S3RdmaStorageDriverExtension<>();
		assertEquals("s3-rdma", ext.id());
	}

	@Test
	void testSchemaProviderNotNull() {
		final var ext = new S3RdmaStorageDriverExtension<>();
		assertNotNull(ext.schemaProvider());
	}

	@Test
	void testSchemaProviderHasCorrectId() {
		final var ext = new S3RdmaStorageDriverExtension<>();
		final var provider = ext.schemaProvider();
		assertEquals("spt", provider.id());
	}

	@Test
	void testSchemaProviderLoadsSchema() throws Exception {
		final var ext = new S3RdmaStorageDriverExtension<>();
		final var provider = ext.schemaProvider();
		final var schema = provider.schema();
		assertNotNull(schema, "Schema should be loadable from classpath resource");
		assertFalse(schema.isEmpty(), "Schema should not be empty");
	}
}
