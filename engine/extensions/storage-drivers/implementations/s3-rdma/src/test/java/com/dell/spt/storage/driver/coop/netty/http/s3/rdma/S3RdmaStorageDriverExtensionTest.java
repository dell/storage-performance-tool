package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import com.dell.spt.base.config.NicSetConfig;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetSettings;
import com.github.akurilov.confuse.Config;
import io.netty.channel.epoll.Epoll;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

	@Test
	void aNicSetGetsTheRdmaDefaultEventLoopsWhileRdmaIsEnabled() {
		assumeTrue(Epoll.isAvailable());
		assertEquals(NicSetSettings.defaultRdmaThreadsPerNic(),
						S3RdmaStorageDriverExtension.nicSetSettings(storage(List.of("ens2np0"), 0, true)).orElseThrow().threadsPerNic());
	}

	@Test
	void aNicSetWithRdmaDisabledGetsTheHttpDefaultEventLoops() {
		assumeTrue(Epoll.isAvailable());
		assertEquals(NicSetSettings.defaultThreadsPerNic(),
						S3RdmaStorageDriverExtension.nicSetSettings(storage(List.of("ens2np0"), 0, false)).orElseThrow().threadsPerNic());
	}

	@Test
	void configuredDriverThreadsOverrideEitherDefault() {
		assumeTrue(Epoll.isAvailable());
		for (final var enabled : List.of(true, false)) {
			assertEquals(7, S3RdmaStorageDriverExtension.nicSetSettings(storage(List.of("ens2np0"), 7, enabled)).orElseThrow().threadsPerNic());
		}
	}

	@Test
	void noNicSetMeansNoSettings() {
		assertTrue(S3RdmaStorageDriverExtension.nicSetSettings(storage(List.of(), 0, true)).isEmpty());
	}

	@SuppressWarnings({"rawtypes", "unchecked"
	})
	private static Config storage(final List<String> interfaces, final int driverThreads, final boolean rdmaEnabled) {
		final Config storage = mock(Config.class);
		final Config net = mock(Config.class);
		final Config rdma = mock(Config.class);
		when(storage.listVal(NicSetConfig.INTERFACES_PATH)).thenReturn((List) interfaces);
		when(storage.stringVal("net-endpoint-selection")).thenReturn("default");
		when(storage.configVal("net")).thenReturn(net);
		when(storage.configVal("rdma")).thenReturn(rdma);
		when(storage.intVal("driver-threads")).thenReturn(driverThreads);
		when(net.stringVal("transport")).thenReturn("epoll");
		when(net.intVal("node-connAttemptsLimit")).thenReturn(0);
		when(net.intVal("timeoutMilliSec")).thenReturn(1);
		when(rdma.boolVal("enabled")).thenReturn(rdmaEnabled);
		return storage;
	}
}
