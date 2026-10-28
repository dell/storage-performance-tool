package com.dell.spt.base.storage.driver;

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

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.config.NicSetConfig;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.storage.driver.nic.NicSetDriverFactory;
import com.github.akurilov.confuse.Config;
import java.util.List;
import org.junit.jupiter.api.Test;

@SuppressWarnings({"rawtypes", "unchecked"
})
class NicSetDriverFactoryTest {

	@Test
	void noNicSetConstructsThroughAnyFactory() throws Exception {
		final var config = storage("third-party", List.of());
		final StorageDriverFactory factory = mock(StorageDriverFactory.class);
		final StorageDriver driver = mock(StorageDriver.class);
		when(factory.id()).thenReturn("third-party");
		when(factory.create("step", null, config, false, 32)).thenReturn(driver);

		assertSame(driver, StorageDriver.instance(List.of(factory), config, null, false, 32, "step"));
	}

	@Test
	void nicSetRejectsFactoriesWithoutOptIn() throws Exception {
		for (final var interfaces : List.of(List.of("ens2np0"), List.of(NicSetConfig.AUTO))) {
			for (final var id : List.of("s3-tables", "s3-rdma", "s3-aws", "swift", "atmos", "dummy-mock", "third-party")) {
				final var config = storage(id, interfaces);
				final StorageDriverFactory factory = mock(StorageDriverFactory.class);
				when(factory.id()).thenReturn(id);

				final var failure = assertThrows(IllegalConfigurationException.class,
								() -> StorageDriver.instance(List.of(factory), config, null, false, 32, "step"));

				assertTrue(failure.getMessage().contains("storage.net.interfaces"), failure.getMessage());
				assertTrue(failure.getMessage().contains(id), failure.getMessage());
				verify(factory, never()).create(anyString(), any(), any(), anyBoolean(), anyInt());
			}
		}
	}

	@Test
	void nicSetReachesAnOptedInFactory() throws Exception {
		final var config = storage("s3", List.of("ens2np0", "ens10np0"));
		final NicSetDriverFactory factory = mock(NicSetDriverFactory.class);
		final StorageDriver driver = mock(StorageDriver.class);
		when(factory.id()).thenReturn("s3");
		when(factory.create("step", null, config, false, 32)).thenReturn(driver);

		assertSame(driver, StorageDriver.instance(List.of(factory), config, null, false, 32, "step"));
		verify(factory).create("step", null, config, false, 32);
	}

	@Test
	void explicitRdmaDeviceBesideANicSetFailsBeforeConstruction() throws Exception {
		final Config config = mock(Config.class);
		when(config.stringVal("driver-type")).thenReturn("s3");
		when(config.stringVal("net-endpoint-selection")).thenReturn("default");
		when(config.listVal(NicSetConfig.INTERFACES_PATH)).thenReturn(List.of("ens2np0"));
		when(config.stringVal("rdma-device")).thenReturn("mlx5_0");
		final NicSetDriverFactory factory = mock(NicSetDriverFactory.class);
		when(factory.id()).thenReturn("s3");

		assertThrows(IllegalConfigurationException.class,
						() -> StorageDriver.instance(List.of(factory), config, null, false, 32, "step"));
		verify(factory, never()).create(anyString(), any(), any(), anyBoolean(), anyInt());
	}

	@Test
	void malformedListFailsBeforeConstruction() throws Exception {
		final var config = storage("s3", List.of("ens2np0", "ens2np0"));
		final NicSetDriverFactory factory = mock(NicSetDriverFactory.class);
		when(factory.id()).thenReturn("s3");

		assertThrows(IllegalConfigurationException.class,
						() -> StorageDriver.instance(List.of(factory), config, null, false, 32, "step"));
		verify(factory, never()).create(anyString(), any(), any(), anyBoolean(), anyInt());
	}

	private static Config storage(final String driverType, final List<String> interfaces) {
		final var config = TestConfigBuilder.config().configVal("storage");
		config.val("driver-type", driverType);
		config.val(NicSetConfig.INTERFACES_PATH, interfaces);
		return config;
	}
}
