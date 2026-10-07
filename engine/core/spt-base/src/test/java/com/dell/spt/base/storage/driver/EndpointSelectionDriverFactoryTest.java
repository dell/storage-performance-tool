package com.dell.spt.base.storage.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.EndpointSelectionConfig;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.storage.driver.endpoint.EndpointSelectionDriverFactory;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.exceptions.InvalidValuePathException;
import java.util.List;
import org.junit.jupiter.api.Test;

@SuppressWarnings({"rawtypes", "unchecked"
})
class EndpointSelectionDriverFactoryTest {

	@Test
	void shippedDefaultIsTheDefaultSelection() {
		final var config = TestConfigBuilder.config().configVal("storage");
		assertEquals(EndpointSelectionConfig.DEFAULT_SELECTION, EndpointSelectionConfig.selection(config));
		assertTrue(EndpointSelectionConfig.isDefault(config));
	}

	@Test
	void missingPathMeansDefaultSelection() {
		final Config config = mock(Config.class);
		when(config.stringVal(EndpointSelectionConfig.SELECTION_PATH)).thenThrow(new InvalidValuePathException("net"));
		assertEquals(EndpointSelectionConfig.DEFAULT_SELECTION, EndpointSelectionConfig.selection(config));
	}

	@Test
	void defaultSelectionConstructsThroughAnyFactory() throws Exception {
		final var config = storage("third-party", "default");
		final StorageDriverFactory factory = mock(StorageDriverFactory.class);
		final StorageDriver driver = mock(StorageDriver.class);
		when(factory.id()).thenReturn("third-party");
		when(factory.create("step", null, config, false, 32)).thenReturn(driver);

		assertSame(driver, StorageDriver.instance(List.of(factory), config, null, false, 32, "step"));
	}

	@Test
	void nonDefaultSelectionRejectsFactoriesWithoutOptIn() throws Exception {
		for (final var mode : List.of("round-robin", "per-request-dns")) {
			for (final var id : List.of("s3-tables", "s3-rdma", "s3-aws", "dummy-mock", "third-party")) {
				final var config = storage(id, mode);
				final StorageDriverFactory factory = mock(StorageDriverFactory.class);
				when(factory.id()).thenReturn(id);

				final var failure = assertThrows(IllegalConfigurationException.class,
								() -> StorageDriver.instance(List.of(factory), config, null, false, 32, "step"));

				assertTrue(failure.getMessage().contains(mode), failure.getMessage());
				assertTrue(failure.getMessage().contains(id), failure.getMessage());
				verify(factory, never()).create(anyString(), any(), any(), anyBoolean(), anyInt());
			}
		}
	}

	@Test
	void nonDefaultSelectionReachesAnOptedInFactory() throws Exception {
		final var config = storage("s3", "round-robin");
		final EndpointSelectionDriverFactory factory = mock(EndpointSelectionDriverFactory.class);
		final StorageDriver driver = mock(StorageDriver.class);
		when(factory.id()).thenReturn("s3");
		when(factory.create("step", null, config, false, 32)).thenReturn(driver);

		assertSame(driver, StorageDriver.instance(List.of(factory), config, null, false, 32, "step"));
		verify(factory).create("step", null, config, false, 32);
	}

	private static Config storage(final String driverType, final String selection) {
		final var config = TestConfigBuilder.config().configVal("storage");
		config.val("driver-type", driverType);
		config.val(EndpointSelectionConfig.SELECTION_PATH, selection);
		return config;
	}
}
