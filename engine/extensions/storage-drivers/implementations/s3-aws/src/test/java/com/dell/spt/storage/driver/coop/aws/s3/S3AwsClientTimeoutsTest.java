package com.dell.spt.storage.driver.coop.aws.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.github.akurilov.confuse.Config;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import software.amazon.awssdk.crtcore.CrtConfigurationUtils;

/**
 * The connection and stall bounds of the s3-aws clients: the shipped defaults, the CLI overrides,
 * what they become in the SDK configuration, and the values that are refused.
 */
final class S3AwsClientTimeoutsTest {

	private static final String CONNECTION_FLAG = "storage-crt-connectionTimeoutMs";
	private static final String SOCKET_FLAG = "storage-crt-socketTimeoutMs";

	private static Config storageConfig(final Path home, final Map<String, Object> overrides) throws Exception {
		return S3AwsMergedConfig.create(home, 9000, overrides).configVal("storage");
	}

	@Test
	void shippedDefaultsBoundConnectingAndIdleTime(@TempDir final Path home) throws Exception {
		final var timeouts = S3AwsClientTimeouts.from(storageConfig(home, Map.of()));

		assertEquals(Duration.ofSeconds(5), timeouts.connectionTimeout());
		assertEquals(Duration.ofSeconds(30), timeouts.stallTimeout());
		assertTrue(timeouts.stallDetectionEnabled());
	}

	@Test
	void cliOverridesReachTheTimeouts(@TempDir final Path home) throws Exception {
		// The CLI hands the engine strings; the schema coerces them to the integer leaves.
		final var timeouts = S3AwsClientTimeouts.from(
						storageConfig(home, Map.of(CONNECTION_FLAG, "7000", SOCKET_FLAG, "9000")));

		assertEquals(Duration.ofSeconds(7), timeouts.connectionTimeout());
		assertEquals(Duration.ofSeconds(9), timeouts.stallTimeout());
	}

	@Test
	void theCrtClientGetsTheConnectionTimeoutAndTheStallMonitor(@TempDir final Path home) throws Exception {
		final var timeouts = S3AwsClientTimeouts.from(storageConfig(home, Map.of(SOCKET_FLAG, 12_000)));

		final var http = timeouts.crtHttpConfiguration();

		assertEquals(Duration.ofSeconds(5), http.connectionTimeout());
		assertEquals(S3AwsClientTimeouts.MINIMUM_PROGRESS_BYTES_PER_SECOND,
						http.healthConfiguration().minimumThroughputInBps());
		assertEquals(Duration.ofSeconds(12), http.healthConfiguration().minimumThroughputTimeout());
	}

	@Test
	void zeroDisablesTheStallMonitorAndKeepsTheConnectionTimeout(@TempDir final Path home) throws Exception {
		final var timeouts = S3AwsClientTimeouts.from(storageConfig(home, Map.of(SOCKET_FLAG, 0)));

		assertFalse(timeouts.stallDetectionEnabled());
		final var http = timeouts.crtHttpConfiguration();
		assertNull(http.healthConfiguration());
		assertEquals(Duration.ofSeconds(5), http.connectionTimeout());
	}

	@ParameterizedTest(name = "{0}={1}")
	@CsvSource({
			CONNECTION_FLAG + ",0",
			CONNECTION_FLAG + ",-5000",
			SOCKET_FLAG + ",-1",
			SOCKET_FLAG + ",1999",
			SOCKET_FLAG + ",1000",
			SOCKET_FLAG + ",1"
	})
	void valuesOutsideTheirRangeAreRefusedWithTheirKey(
					final String flag, final int value, @TempDir final Path home) throws Exception {
		final Config storage = storageConfig(home, Map.of(flag, value));

		final var failure = assertThrows(IllegalConfigurationException.class, () -> S3AwsClientTimeouts.from(storage));

		assertTrue(failure.getMessage().contains("storage.crt." + flag.substring("storage-crt-".length())),
						failure.getMessage());
	}

	@ParameterizedTest(name = "{0} ms is {1} s for the native monitor")
	@CsvSource({"2000,2", "2500,2", "2999,2", "3000,3", "30000,30"
	})
	void theNativeMonitorCountsWholeSecondsRoundedDown(final int milliseconds, final int seconds) {
		final var timeouts = new S3AwsClientTimeouts(Duration.ofSeconds(5), Duration.ofMillis(milliseconds));

		final var options = CrtConfigurationUtils.resolveHttpMonitoringOptions(
						timeouts.crtHttpConfiguration().healthConfiguration()).orElseThrow();

		assertEquals(seconds, options.getAllowableThroughputFailureIntervalSeconds());
		assertEquals(S3AwsClientTimeouts.MINIMUM_PROGRESS_BYTES_PER_SECOND, options.getMinThroughputBytesPerSecond());
	}

	@Test
	void theLowestStallTimeoutIsAccepted(@TempDir final Path home) throws Exception {
		final var timeouts = S3AwsClientTimeouts.from(
						storageConfig(home, Map.of(SOCKET_FLAG, S3AwsClientTimeouts.MINIMUM_STALL_TIMEOUT_MS)));

		assertEquals(Duration.ofMillis(S3AwsClientTimeouts.MINIMUM_STALL_TIMEOUT_MS), timeouts.stallTimeout());
	}

	@Test
	void aValueThatIsNotAnIntegerIsRefusedByTheConfigurationBeforeTheDriverSeesIt(@TempDir final Path home)
					throws Exception {
		final Config config = S3AwsMergedConfig.create(home, 9000, Map.of());

		assertThrows(NumberFormatException.class, () -> config.val(SOCKET_FLAG, "soon"));
	}

	@Test
	void aMissingValueIsRefusedWithItsKeyInsteadOfFallingBack() {
		final Config storage = mock(Config.class);
		final Config crt = mock(Config.class);
		when(storage.configVal("crt")).thenReturn(crt);
		when(crt.intVal("connectionTimeoutMs")).thenReturn(5000);
		when(crt.intVal("socketTimeoutMs")).thenThrow(new NoSuchElementException("socketTimeoutMs"));

		final var failure = assertThrows(IllegalConfigurationException.class, () -> S3AwsClientTimeouts.from(storage));

		assertTrue(failure.getMessage().contains("storage.crt.socketTimeoutMs"), failure.getMessage());
		assertTrue(failure.getCause() instanceof NoSuchElementException);
	}

	@Test
	void aMissingBranchIsRefusedWithItsPath() {
		final Config storage = mock(Config.class);
		when(storage.configVal("crt")).thenReturn(null);

		final var failure = assertThrows(IllegalConfigurationException.class, () -> S3AwsClientTimeouts.from(storage));

		assertTrue(failure.getMessage().contains("storage.crt"), failure.getMessage());
	}
}
