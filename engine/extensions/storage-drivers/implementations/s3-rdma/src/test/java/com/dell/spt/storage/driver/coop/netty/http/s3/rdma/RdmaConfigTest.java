package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import com.dell.spt.base.config.ConfigFormat;
import com.dell.spt.base.config.ConfigUtil;
import com.github.akurilov.confuse.Config;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

public class RdmaConfigTest {

	@Test
	void testConstructorWithAllFields() {
		final var config = new RdmaConfig(
						true, 1_048_576L, true, "mlx5_0", "10.0.0.1", "INFO");
		assertTrue(config.isEnabled());
		assertEquals(1_048_576L, config.getThresholdBytes());
		assertTrue(config.isFallbackEnabled());
		assertEquals("mlx5_0", config.getDevice());
		assertEquals("10.0.0.1", config.getLocalIp());
		assertEquals("INFO", config.getLogLevel());
	}

	@Test
	void testDisabledConfig() {
		final var config = new RdmaConfig(
						false, 0L, false, "auto", "", "WARN");
		assertFalse(config.isEnabled());
		assertEquals(0L, config.getThresholdBytes());
		assertFalse(config.isFallbackEnabled());
	}

	@Test
	void testThresholdBoundary() {
		final var config = new RdmaConfig(
						true, 0L, true, "auto", "", "WARN");
		assertEquals(0L, config.getThresholdBytes());
	}

	@Test
	void testLargeThreshold() {
		final long oneGb = 1_073_741_824L;
		final var config = new RdmaConfig(
						true, oneGb, true, "auto", "", "WARN");
		assertEquals(oneGb, config.getThresholdBytes());
	}

	@Test
	void testToString() {
		final var config = new RdmaConfig(
						true, 1_048_576L, true, "auto", "", "WARN");
		final String str = config.toString();
		assertNotNull(str);
		assertTrue(str.contains("enabled=true"));
		assertTrue(str.contains("thresholdBytes=1048576"));
		assertTrue(str.contains("fallbackEnabled=true"));
	}

	@Test
	void testEmptyLocalIp() {
		final var config = new RdmaConfig(
						true, 1_048_576L, true, "auto", "", "WARN");
		assertEquals("", config.getLocalIp());
	}

	@Test
	void testAutoDevice() {
		final var config = new RdmaConfig(
						true, 1_048_576L, true, "auto", "", "WARN");
		assertEquals("auto", config.getDevice());
	}

	// ---------- Null Config constructor (defaults) ----------

	@Test
	void testNullConfigUsesDefaults() {
		final var config = new RdmaConfig((Config) null);
		assertTrue(config.isEnabled());
		assertEquals(1_048_576L, config.getThresholdBytes());
		assertFalse(config.isFallbackEnabled());
		assertEquals("auto", config.getDevice());
		assertEquals("", config.getLocalIp());
		assertEquals("WARN", config.getLogLevel());
		assertEquals(30_000L, config.getTimeoutMs());
		assertFalse(config.isAllowMissingBytesHeader());
	}

	// ---------- Config-based constructor with mock ----------

	@Test
	void testConfigConstructor_parsesAllFields() {
		final Config mockCfg = Mockito.mock(Config.class);
		when(mockCfg.boolVal("enabled")).thenReturn(false);
		when(mockCfg.longVal("thresholdBytes")).thenReturn(4_194_304L);
		when(mockCfg.boolVal("fallback")).thenReturn(false);
		when(mockCfg.stringVal("device")).thenReturn("mlx5_bond_0");
		when(mockCfg.stringVal("localIp")).thenReturn("10.247.128.125");
		when(mockCfg.stringVal("logLevel")).thenReturn("DEBUG");
		when(mockCfg.longVal("timeoutMs")).thenReturn(60_000L);
		when(mockCfg.boolVal("allowMissingBytesHeader")).thenReturn(true);

		final var config = new RdmaConfig(mockCfg);
		assertTrue(config.isAllowMissingBytesHeader());
		assertFalse(config.isEnabled());
		assertEquals(4_194_304L, config.getThresholdBytes());
		assertFalse(config.isFallbackEnabled());
		assertEquals("mlx5_bond_0", config.getDevice());
		assertEquals("10.247.128.125", config.getLocalIp());
		assertEquals("DEBUG", config.getLogLevel());
		assertEquals(60_000L, config.getTimeoutMs());
	}

	@Test
	void testConfigConstructor_missingKeysFallBackToDefaults() {
		// When config throws on missing keys, defaults should be used
		final Config mockCfg = Mockito.mock(Config.class);
		when(mockCfg.boolVal("enabled")).thenThrow(new RuntimeException("no key"));
		when(mockCfg.longVal("thresholdBytes")).thenThrow(new RuntimeException("missing"));
		when(mockCfg.boolVal("fallback")).thenThrow(new RuntimeException("missing"));
		when(mockCfg.stringVal("device")).thenThrow(new RuntimeException("missing"));
		when(mockCfg.stringVal("localIp")).thenThrow(new RuntimeException("missing"));
		when(mockCfg.stringVal("logLevel")).thenThrow(new RuntimeException("missing"));
		when(mockCfg.longVal("timeoutMs")).thenThrow(new RuntimeException("missing"));
		when(mockCfg.boolVal("allowMissingBytesHeader")).thenThrow(new RuntimeException("missing"));

		final var config = new RdmaConfig(mockCfg);
		// All should be defaults
		assertFalse(config.isAllowMissingBytesHeader());
		assertTrue(config.isEnabled());
		assertEquals(1_048_576L, config.getThresholdBytes());
		assertFalse(config.isFallbackEnabled());
		assertEquals("auto", config.getDevice());
		assertEquals("", config.getLocalIp());
		assertEquals("WARN", config.getLogLevel());
		assertEquals(30_000L, config.getTimeoutMs());
	}

	@Test
	void testConfigConstructor_nullStringValReturnsDefault() {
		final Config mockCfg = Mockito.mock(Config.class);
		when(mockCfg.boolVal("enabled")).thenReturn(true);
		when(mockCfg.longVal("thresholdBytes")).thenReturn(1_048_576L);
		when(mockCfg.boolVal("fallback")).thenReturn(true);
		when(mockCfg.stringVal("device")).thenReturn(null);
		when(mockCfg.stringVal("localIp")).thenReturn(null);
		when(mockCfg.stringVal("logLevel")).thenReturn(null);
		when(mockCfg.longVal("timeoutMs")).thenReturn(30_000L);

		final var config = new RdmaConfig(mockCfg);
		assertEquals("auto", config.getDevice(), "null device should fall back to 'auto'");
		assertEquals("", config.getLocalIp(), "null localIp should fall back to empty string");
		assertEquals("WARN", config.getLogLevel(), "null logLevel should fall back to 'WARN'");
	}

	// ---------- 7-arg constructor with timeoutMs ----------

	@Test
	void testSevenArgConstructor() {
		final var config = new RdmaConfig(
						true, 2_097_152L, false, "mlx5_0", "192.168.1.1", "TRACE", 45_000L);
		assertEquals(2_097_152L, config.getThresholdBytes());
		assertFalse(config.isFallbackEnabled());
		assertEquals(45_000L, config.getTimeoutMs());
		assertEquals("TRACE", config.getLogLevel());
	}

	@Test
	void testSixArgConstructor_defaultTimeout() {
		// The 6-arg constructor should set default timeout of 30000ms
		final var config = new RdmaConfig(
						true, 1_048_576L, true, "auto", "", "WARN");
		assertEquals(30_000L, config.getTimeoutMs());
	}

	// ---------- Edge cases ----------

	@Test
	void testNegativeThreshold() {
		// Negative thresholds are allowed at config level (driver may clamp)
		final var config = new RdmaConfig(
						true, -1L, true, "auto", "", "WARN");
		assertEquals(-1L, config.getThresholdBytes());
	}

	@Test
	void testNegativeTimeout() {
		final var config = new RdmaConfig(
						true, 1_048_576L, true, "auto", "", "WARN", -1L);
		assertEquals(-1L, config.getTimeoutMs());
	}

	@Test
	void testZeroTimeout() {
		final var config = new RdmaConfig(
						true, 1_048_576L, true, "auto", "", "WARN", 0L);
		assertEquals(0L, config.getTimeoutMs());
	}

	@Test
	void testToString_includesTimeoutMs() {
		final var config = new RdmaConfig(
						true, 1_048_576L, true, "auto", "", "WARN", 45_000L);
		final String str = config.toString();
		assertTrue(str.contains("timeoutMs=45000"));
	}

	@Test
	void testToString_includesAllFields() {
		final var config = new RdmaConfig(
						false, 0L, false, "mlx5_0", "10.0.0.1", "DEBUG", 60_000L);
		final String str = config.toString();
		assertTrue(str.contains("enabled=false"));
		assertTrue(str.contains("thresholdBytes=0"));
		assertTrue(str.contains("fallbackEnabled=false"));
		assertTrue(str.contains("device='mlx5_0'"));
		assertTrue(str.contains("localIp='10.0.0.1'"));
		assertTrue(str.contains("logLevel='DEBUG'"));
		assertTrue(str.contains("timeoutMs=60000"));
	}

	// ---------- PUT payload copy threads ----------

	@Test
	void shippedDefaultsCopyOnTheDispatcherAlone() throws Exception {
		assertEquals(1, new RdmaConfig((Config) null).getCopyThreads());
		final Config shipped = shippedDefaults();
		// Read by its full path first: a key missing from the shipped file would throw here.
		assertEquals(1, shipped.intVal("storage-rdma-copyThreads"));
		assertEquals(1, new RdmaConfig(shipped.configVal("storage").configVal("rdma")).getCopyThreads());
	}

	@Test
	void copyThreadsSetThroughTheSchemaReachTheConfig() throws Exception {
		final Config config = shippedDefaults();
		// The path and the string value the CLI argument --storage-rdma-copyThreads=4 sets.
		config.val("storage-rdma-copyThreads", "4");

		final var parsed = new RdmaConfig(config.configVal("storage").configVal("rdma"));
		assertEquals(4, parsed.getCopyThreads());
		assertEquals(4, parsed.withLocalIp("10.0.0.1").getCopyThreads());
		assertTrue(parsed.toString().contains("copyThreads=4"));
	}

	@Test
	void missingCopyThreadsKeyLeavesTheDefault() {
		final Config mockCfg = Mockito.mock(Config.class);
		when(mockCfg.intVal("copyThreads")).thenThrow(new RuntimeException("missing"));
		assertEquals(1, new RdmaConfig(mockCfg).getCopyThreads());
	}

	/** The shipped defaults file, loaded against the shipped schema as the engine loads it. */
	private static Config shippedDefaults() throws Exception {
		final String defaults;
		try (final var in = RdmaConfigTest.class.getResourceAsStream("/config/defaults-storage-s3-rdma.yaml")) {
			defaults = new String(in.readAllBytes(), StandardCharsets.UTF_8);
		}
		return ConfigUtil.loadConfig(
						defaults, ConfigFormat.YAML, new S3RdmaStorageDriverExtension<>().schemaProvider().schema());
	}

	@Test
	void testMaxLongThreshold() {
		final var config = new RdmaConfig(
						true, Long.MAX_VALUE, true, "auto", "", "WARN");
		assertEquals(Long.MAX_VALUE, config.getThresholdBytes());
	}
}
