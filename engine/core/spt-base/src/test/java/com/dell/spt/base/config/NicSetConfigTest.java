package com.dell.spt.base.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.Constants;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.load.step.linear.LinearLoadStepLocal;
import com.dell.spt.base.metrics.MetricsManager;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.exceptions.InvalidValuePathException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;

@SuppressWarnings({"rawtypes", "unchecked"
})
class NicSetConfigTest {

	private Config defaults() throws Exception {
		final var schema = SchemaProvider.resolve(Constants.APP_NAME, getClass().getClassLoader()).stream().findFirst().orElseThrow();
		return ConfigUtil.loadConfig(Path.of(getClass().getResource("/config/defaults.yaml").toURI()).toFile(), schema);
	}

	@Test
	void shippedDefaultIsEmptyAndKeepsTheKernelRoute() throws Exception {
		final var config = defaults();
		assertEquals(List.of(), config.listVal("storage-net-interfaces"));
		assertEquals(List.of(), NicSetConfig.interfaces(config.configVal("storage")));
		assertFalse(NicSetConfig.isSet(config.configVal("storage")));
	}

	@Test
	void missingPathOrValueMeansNoNicSet() {
		final Config missingPath = mock(Config.class);
		when(missingPath.listVal(NicSetConfig.INTERFACES_PATH)).thenThrow(new InvalidValuePathException("net"));
		assertEquals(List.of(), NicSetConfig.interfaces(missingPath));
		final Config missingValue = mock(Config.class);
		when(missingValue.listVal(NicSetConfig.INTERFACES_PATH)).thenThrow(new NoSuchElementException("interfaces"));
		assertEquals(List.of(), NicSetConfig.interfaces(missingValue));
		final Config nullValue = mock(Config.class);
		when(nullValue.listVal(NicSetConfig.INTERFACES_PATH)).thenReturn(null);
		assertFalse(NicSetConfig.isSet(nullValue));
	}

	@Test
	void engineCliSetterSplitsOnCommasAndAnEmptyValueMeansNone() throws Exception {
		final var config = defaults();
		CliArgUtil.parseArgs("--storage-net-interfaces=ens2np0,ens10np0").forEach(config::val);
		assertEquals(List.of("ens2np0", "ens10np0"), NicSetConfig.interfaces(config.configVal("storage")));

		CliArgUtil.parseArgs("--storage-net-interfaces= ens2np0 , 192.0.2.10 ").forEach(config::val);
		assertEquals(List.of("ens2np0", "192.0.2.10"), NicSetConfig.interfaces(config.configVal("storage")));

		CliArgUtil.parseArgs("--storage-net-interfaces=auto").forEach(config::val);
		assertEquals(List.of(NicSetConfig.AUTO), NicSetConfig.interfaces(config.configVal("storage")));
		// The stored form is the request as given, never a resolved set.
		assertEquals(List.of(NicSetConfig.AUTO), config.listVal("storage-net-interfaces"));

		CliArgUtil.parseArgs("--storage-net-interfaces=a,,b").forEach(config::val);
		assertThrows(IllegalConfigurationException.class, () -> NicSetConfig.interfaces(config.configVal("storage")));

		// Confuse stores an empty value as one blank entry; it means no NIC set.
		CliArgUtil.parseArgs("--storage-net-interfaces=").forEach(config::val);
		assertEquals(List.of(""), config.listVal("storage-net-interfaces"));
		assertEquals(List.of(), NicSetConfig.interfaces(config.configVal("storage")));
		assertFalse(NicSetConfig.isSet(config.configVal("storage")));
	}

	@Test
	void malformedEntriesFailAtConfiguration() {
		final List<List<String>> cases = List.of(
						List.of("ens2np0", ""),
						List.of(" ", "ens2np0"),
						List.of("auto", "ens2np0"),
						List.of("ens2np0", "auto"),
						List.of("AUTO"),
						List.of("Auto"),
						List.of("fe80::1"),
						List.of("2001:db8::10"),
						List.of("ens2np0", "ens2np0"),
						List.of("ens2np0", " ens2np0 "),
						Arrays.asList("ens2np0", null));
		for (final var entries : cases) {
			final var failure = assertThrows(IllegalConfigurationException.class, () -> NicSetConfig.validate(entries),
							String.valueOf(entries));
			assertTrue(failure.getMessage().contains("storage.net.interfaces"), failure.getMessage());
		}
	}

	@Test
	void entriesAreTrimmedAndKeptInOrder() {
		assertEquals(List.of("mlx5_1", "198.51.100.7", "ens10np0"),
						NicSetConfig.validate(List.of(" mlx5_1", "198.51.100.7 ", "ens10np0")));
		assertEquals(List.of(), NicSetConfig.validate(List.of()));
		assertEquals(List.of(), NicSetConfig.validate(List.of("  ")));
	}

	@Test
	void workloadOutsideTheMatrixFailsOnlyWithANicSet() {
		final var withSet = storage(List.of("ens2np0"));
		assertThrows(IllegalConfigurationException.class,
						() -> NicSetConfig.requireSupportedWorkload(withSet, OpType.NOOP, null));
		final var copy = assertThrows(IllegalConfigurationException.class,
						() -> NicSetConfig.requireSupportedWorkload(withSet, OpType.CREATE, "/source"));
		assertTrue(copy.getMessage().contains("copy"), copy.getMessage());
		for (final var op : List.of(OpType.CREATE, OpType.READ, OpType.UPDATE, OpType.DELETE, OpType.LIST)) {
			assertDoesNotThrow(() -> NicSetConfig.requireSupportedWorkload(withSet, op, null), op.name());
			assertDoesNotThrow(() -> NicSetConfig.requireSupportedWorkload(withSet, op, ""), op.name());
		}
		assertDoesNotThrow(() -> NicSetConfig.requireSupportedWorkload(withSet, OpType.READ, "/source"));

		final var withoutSet = storage(List.of());
		assertDoesNotThrow(() -> NicSetConfig.requireSupportedWorkload(withoutSet, OpType.NOOP, null));
		assertDoesNotThrow(() -> NicSetConfig.requireSupportedWorkload(withoutSet, OpType.CREATE, "/source"));
	}

	@Test
	void explicitRdmaDeviceOrLocalAddressIsRejected() {
		final Config device = rdma("mlx5_0", "");
		assertTrue(assertThrows(IllegalConfigurationException.class, () -> NicSetConfig.requireNoRdmaAddress(device))
						.getMessage().contains("--rdma-device"));
		final Config localIp = rdma("auto", "192.0.2.10");
		assertTrue(assertThrows(IllegalConfigurationException.class, () -> NicSetConfig.requireNoRdmaAddress(localIp))
						.getMessage().contains("--rdma-local-ip"));
		assertDoesNotThrow(() -> NicSetConfig.requireNoRdmaAddress(rdma("auto", "")));
		assertDoesNotThrow(() -> NicSetConfig.requireNoRdmaAddress(rdma(null, null)));
		final Config absent = mock(Config.class);
		when(absent.stringVal(anyString())).thenThrow(new InvalidValuePathException("rdma"));
		assertDoesNotThrow(() -> NicSetConfig.requireNoRdmaAddress(absent));
	}

	@Test
	void malformedListFailsInRealStepConstructorBeforeInitialization() throws Exception {
		final var config = defaults();
		config.val("storage-net-interfaces", List.of("auto", "ens2np0"));
		assertThrows(IllegalConfigurationException.class,
						() -> new LinearLoadStepLocal(config, List.of(), null, mock(MetricsManager.class)));
	}

	private static Config storage(final List<String> interfaces) {
		final var config = TestConfigBuilder.config().configVal("storage");
		config.val(NicSetConfig.INTERFACES_PATH, interfaces);
		return config;
	}

	private static Config rdma(final String device, final String localIp) {
		final Config config = mock(Config.class);
		when(config.stringVal("rdma-device")).thenReturn(device);
		when(config.stringVal("rdma-localIp")).thenReturn(localIp);
		return config;
	}

	private Config withNodes(final String... addrs) throws Exception {
		final var config = defaults();
		config.val("load-step-node-addrs", List.of(addrs));
		config.val("load-step-node-port", 1099);
		return config;
	}

	@Test
	void shippedHostOverridesAreEmpty() throws Exception {
		final var config = defaults();
		assertEquals(Map.of(), config.mapVal("storage-net-nodeInterfaces"));
		assertEquals(Map.of(), NicSetConfig.nodeInterfaces(config));
	}

	@Test
	void hostOverridesArriveFromTheEngineCommandLineWithDashesInTheirKeys() throws Exception {
		final var config = withNodes("my-host.example:1099", "198.51.100.12:1099");
		config.val("storage-net-nodeInterfaces-entry", "none");
		config.val("storage-net-nodeInterfaces-my-host.example:1099", "mlx5_0,mlx5_1");
		config.val("storage-net-nodeInterfaces-198.51.100.12:1099", "auto");
		assertEquals(
						Map.of(
										"entry", List.of(),
										"my-host.example:1099", List.of("mlx5_0", "mlx5_1"),
										"198.51.100.12:1099", List.of("auto")),
						NicSetConfig.nodeInterfaces(config));
	}

	@Test
	void hostOverrideValuesMayBeLists() throws Exception {
		final var config = withNodes("198.51.100.12");
		config.val("storage-net-nodeInterfaces", Map.of("198.51.100.12:1099", List.of("eth0", "eth1")));
		assertEquals(Map.of("198.51.100.12:1099", List.of("eth0", "eth1")), NicSetConfig.nodeInterfaces(config));
	}

	@Test
	void hostOverrideKeysAreCheckedAgainstTheConfiguredNodes() throws Exception {
		final var config = withNodes("198.51.100.12:1099");
		config.val("storage-net-nodeInterfaces-203.0.113.9:1099", "auto");
		final var e = assertThrows(IllegalConfigurationException.class, () -> NicSetConfig.nodeInterfaces(config));
		assertEquals(
						"storage.net.nodeInterfaces names 203.0.113.9:1099, which is neither entry nor in load.step.node.addrs",
						e.getMessage());
	}

	@Test
	void anInvalidHostOverrideValueNamesItsKey() throws Exception {
		for (final var bad : List.of("", " ", "none,eth0", "auto,eth0", "eth0,eth0", "AUTO", "fe80::1")) {
			final var config = withNodes();
			config.val("storage-net-nodeInterfaces-entry", bad);
			final var e = assertThrows(
							IllegalConfigurationException.class, () -> NicSetConfig.nodeInterfaces(config), "value \"" + bad + "\"");
			assertTrue(e.getMessage().startsWith("storage.net.nodeInterfaces[entry]"), e.getMessage());
		}
	}

	@Test
	void noneStaysInvalidInTheStepsOwnSetting() throws Exception {
		final var config = defaults();
		config.val("storage-net-interfaces", List.of("none"));
		final var e = assertThrows(
						IllegalConfigurationException.class, () -> NicSetConfig.interfaces(config.configVal("storage")));
		assertEquals("storage.net.interfaces entry \"none\": none applies only to storage.net.nodeInterfaces", e.getMessage());
	}
}
