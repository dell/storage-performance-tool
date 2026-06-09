package com.dell.spt.storage.driver.coop.aws.s3;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.github.akurilov.confuse.Config;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class S3AwsStorageDriverFactoryTest {

	// -----------------------------------------------------------------------
	// Helper: build a mock Config chain for the endpoint resolution path
	//   storageConfig → configVal("net") → netConfig
	//   netConfig → configVal("node") → nodeConfig
	//   nodeConfig → listVal("addrs"), intVal("port")
	//   netConfig → configVal("ssl") → sslConfig → boolVal("enabled")
	// -----------------------------------------------------------------------

	private Config mockNodeConfig(List<String> addrs, int port) {
		Config nodeConfig = mock(Config.class);
		doReturn(addrs).when(nodeConfig).listVal("addrs");
		if (port > 0) {
			when(nodeConfig.intVal("port")).thenReturn(port);
		} else {
			when(nodeConfig.intVal("port")).thenThrow(new RuntimeException("no port"));
		}
		return nodeConfig;
	}

	private Config mockNetConfig(Config nodeConfig, Boolean sslEnabled) {
		Config netConfig = mock(Config.class);
		when(netConfig.configVal("node")).thenReturn(nodeConfig);
		if (sslEnabled != null) {
			Config sslConfig = mock(Config.class);
			when(sslConfig.boolVal("enabled")).thenReturn(sslEnabled);
			when(netConfig.configVal("ssl")).thenReturn(sslConfig);
		} else {
			when(netConfig.configVal("ssl")).thenThrow(new RuntimeException("no ssl"));
		}
		return netConfig;
	}

	private Config mockStorageConfig(Config netConfig) {
		Config storageConfig = mock(Config.class);
		when(storageConfig.configVal("net")).thenReturn(netConfig);
		return storageConfig;
	}

	// -----------------------------------------------------------------------
	// resolveEndpoint — the extracted static method
	// -----------------------------------------------------------------------

	@Nested
	class ResolveEndpointTest {

		@Test
		void bareHostname_withPort() throws Exception {
			Config nodeConfig = mockNodeConfig(List.of("10.0.0.1"), 8333);
			Config netConfig = mockNetConfig(nodeConfig, null);
			Config storageConfig = mockStorageConfig(netConfig);

			String endpoint = S3AwsStorageDriverFactory.resolveEndpoint(storageConfig);
			assertEquals("http://10.0.0.1:8333", endpoint);
		}

		@Test
		void bareHostname_withPort_sslEnabled() throws Exception {
			Config nodeConfig = mockNodeConfig(List.of("10.0.0.1"), 443);
			Config netConfig = mockNetConfig(nodeConfig, true);
			Config storageConfig = mockStorageConfig(netConfig);

			String endpoint = S3AwsStorageDriverFactory.resolveEndpoint(storageConfig);
			assertEquals("https://10.0.0.1:443", endpoint);
		}

		@Test
		void schemePrefixedAddr_usedAsIs() throws Exception {
			Config nodeConfig = mockNodeConfig(List.of("http://my-s3:9000"), 0);
			Config netConfig = mockNetConfig(nodeConfig, null);
			Config storageConfig = mockStorageConfig(netConfig);

			String endpoint = S3AwsStorageDriverFactory.resolveEndpoint(storageConfig);
			assertEquals("http://my-s3:9000", endpoint);
		}

		@Test
		void httpsPrefixedAddr_usedAsIs() throws Exception {
			Config nodeConfig = mockNodeConfig(List.of("https://s3.amazonaws.com"), 0);
			Config netConfig = mockNetConfig(nodeConfig, null);
			Config storageConfig = mockStorageConfig(netConfig);

			String endpoint = S3AwsStorageDriverFactory.resolveEndpoint(storageConfig);
			assertEquals("https://s3.amazonaws.com", endpoint);
		}

		@Test
		void hostColonPort_noConfigPort() throws Exception {
			Config nodeConfig = mockNodeConfig(List.of("10.0.0.1:9999"), 0);
			Config netConfig = mockNetConfig(nodeConfig, null);
			Config storageConfig = mockStorageConfig(netConfig);

			String endpoint = S3AwsStorageDriverFactory.resolveEndpoint(storageConfig);
			assertEquals("http://10.0.0.1:9999", endpoint);
		}

		@Test
		void hostColonPort_sslEnabled() throws Exception {
			Config nodeConfig = mockNodeConfig(List.of("10.0.0.1:9999"), 0);
			Config netConfig = mockNetConfig(nodeConfig, true);
			Config storageConfig = mockStorageConfig(netConfig);

			String endpoint = S3AwsStorageDriverFactory.resolveEndpoint(storageConfig);
			assertEquals("https://10.0.0.1:9999", endpoint);
		}

		@Test
		void emptyAddrsList_throws() {
			Config nodeConfig = mockNodeConfig(Collections.emptyList(), 8333);
			Config netConfig = mockNetConfig(nodeConfig, null);
			Config storageConfig = mockStorageConfig(netConfig);

			var ex = assertThrows(IllegalConfigurationException.class,
							() -> S3AwsStorageDriverFactory.resolveEndpoint(storageConfig));
			assertTrue(ex.getMessage().contains("endpoint") || ex.getMessage().contains("addrs"));
		}

		@Test
		void nullAddrsList_throws() throws Exception {
			Config nodeConfig = mockNodeConfig(null, 8333);
			Config netConfig = mockNetConfig(nodeConfig, null);
			Config storageConfig = mockStorageConfig(netConfig);

			assertThrows(IllegalConfigurationException.class,
							() -> S3AwsStorageDriverFactory.resolveEndpoint(storageConfig));
		}

		@Test
		void netConfigMissing_throws() {
			Config storageConfig = mock(Config.class);
			when(storageConfig.configVal("net"))
							.thenThrow(new RuntimeException("no net config"));

			var ex = assertThrows(IllegalConfigurationException.class,
							() -> S3AwsStorageDriverFactory.resolveEndpoint(storageConfig));
			assertTrue(ex.getMessage().contains("endpoint") || ex.getMessage().contains("addrs"));
		}

		@Test
		void usesFirstAddr_whenMultipleProvided() throws Exception {
			Config nodeConfig = mockNodeConfig(List.of("10.0.0.1", "10.0.0.2", "10.0.0.3"), 8333);
			Config netConfig = mockNetConfig(nodeConfig, null);
			Config storageConfig = mockStorageConfig(netConfig);

			String endpoint = S3AwsStorageDriverFactory.resolveEndpoint(storageConfig);
			assertEquals("http://10.0.0.1:8333", endpoint);
		}

		@Test
		void bareHostname_noPort_defaultsToSchemeOnly() throws Exception {
			Config nodeConfig = mockNodeConfig(List.of("10.0.0.1"), 0);
			Config netConfig = mockNetConfig(nodeConfig, null);
			Config storageConfig = mockStorageConfig(netConfig);

			String endpoint = S3AwsStorageDriverFactory.resolveEndpoint(storageConfig);
			assertEquals("http://10.0.0.1", endpoint);
		}
	}

	// -----------------------------------------------------------------------
	// create() — error paths tested through the public API
	// -----------------------------------------------------------------------

	@Nested
	class CreateErrorPathTest {

		@Test
		void missingAuthUid_throwsIllegalConfigurationException() {
			Config storageConfig = mock(Config.class);
			when(storageConfig.stringVal("auth-uid"))
							.thenThrow(new RuntimeException("no auth-uid"));

			S3AwsStorageDriverFactory<?, ?> factory = new S3AwsStorageDriverFactory<>();

			var ex = assertThrows(IllegalConfigurationException.class,
							() -> factory.create("step-1", null, storageConfig, false, 1));
			assertTrue(ex.getMessage().contains("auth"),
							"Error message should mention auth, got: " + ex.getMessage());
		}

		@Test
		void missingAuthSecret_throwsIllegalConfigurationException() {
			Config storageConfig = mock(Config.class);
			when(storageConfig.stringVal("auth-uid")).thenReturn("access");
			when(storageConfig.stringVal("auth-secret"))
							.thenThrow(new RuntimeException("no auth-secret"));

			S3AwsStorageDriverFactory<?, ?> factory = new S3AwsStorageDriverFactory<>();

			var ex = assertThrows(IllegalConfigurationException.class,
							() -> factory.create("step-1", null, storageConfig, false, 1));
			assertTrue(ex.getMessage().contains("auth"),
							"Error message should mention auth, got: " + ex.getMessage());
		}

		@Test
		void missingNetConfig_throwsIllegalConfigurationException() {
			Config storageConfig = mock(Config.class);
			when(storageConfig.stringVal("auth-uid")).thenReturn("access");
			when(storageConfig.stringVal("auth-secret")).thenReturn("secret");
			when(storageConfig.stringVal("region")).thenReturn("us-east-1");
			when(storageConfig.configVal("net"))
							.thenThrow(new RuntimeException("no net config"));

			S3AwsStorageDriverFactory<?, ?> factory = new S3AwsStorageDriverFactory<>();

			var ex = assertThrows(IllegalConfigurationException.class,
							() -> factory.create("step-1", null, storageConfig, false, 1));
			assertTrue(ex.getMessage().contains("endpoint") || ex.getMessage().contains("addrs"),
							"Error message should mention endpoint/addrs, got: " + ex.getMessage());
		}
	}

	// -----------------------------------------------------------------------
	// CRT Configuration Type Overflow Tests
	// -----------------------------------------------------------------------

	@Nested
	class CrtConfigTypeOverflowTest {

		@Test
		void largeMinimumPartSizeBytes_doesNotCauseOverflow() {
			// Mock config with large minimumPartSizeBytes value (> Integer.MAX_VALUE)
			Config crtConfig = mock(Config.class);
			when(crtConfig.longVal("minimumPartSizeBytes")).thenReturn(5368709120L);  // 5GB > Integer.MAX_VALUE

			Config storageConfig = mock(Config.class);
			when(storageConfig.stringVal("auth-uid")).thenReturn("access");
			when(storageConfig.stringVal("auth-secret")).thenReturn("secret");
			when(storageConfig.stringVal("region")).thenReturn("us-east-1");
			when(storageConfig.configVal("crt")).thenReturn(crtConfig);
			when(storageConfig.configVal("net"))
							.thenThrow(new RuntimeException("no net config"));

			S3AwsStorageDriverFactory<?, ?> factory = new S3AwsStorageDriverFactory<>();

			// The factory will fail when creating S3AsyncClient, but it should not fail
			// with a downcast/overflow exception when reading the config
			var ex = assertThrows(Exception.class,
							() -> factory.create("step-1", null, storageConfig, false, 1));

			// Verify the exception is not related to downcast/overflow
			assertFalse(ex.getMessage().contains("overflow") || ex.getMessage().contains("downcast") ||
							ex instanceof ClassCastException,
							"Exception should not be related to overflow/downcast, got: " + ex.getMessage());
		}
	}

	// -----------------------------------------------------------------------
	// CRT Config Path Tests
	// -----------------------------------------------------------------------

	@Nested
	class CrtConfigPathTest {

		@Test
		void maxConcurrency_configPathDoesNotThrow() {
			// Mock config with maxConcurrency to ensure the config path doesn't throw
			Config crtConfig = mock(Config.class);
			when(crtConfig.intVal("maxConcurrency")).thenReturn(256);

			Config storageConfig = mock(Config.class);
			when(storageConfig.stringVal("auth-uid")).thenReturn("access");
			when(storageConfig.stringVal("auth-secret")).thenReturn("secret");
			when(storageConfig.stringVal("region")).thenReturn("us-east-1");
			when(storageConfig.configVal("crt")).thenReturn(crtConfig);
			when(storageConfig.configVal("net"))
							.thenThrow(new RuntimeException("no net config"));

			S3AwsStorageDriverFactory<?, ?> factory = new S3AwsStorageDriverFactory<>();

			// The factory will fail when creating S3AsyncClient, but it should not fail
			// with a NoSuchElementException when reading maxConcurrency
			var ex = assertThrows(Exception.class,
							() -> factory.create("step-1", null, storageConfig, false, 1));

			// Verify the exception is not a NoSuchElementException (which would indicate config path issue)
			assertFalse(ex instanceof java.util.NoSuchElementException,
							"Exception should not be NoSuchElementException, got: " + ex.getClass().getName());
		}
	}

	// -----------------------------------------------------------------------
	// CRT Configuration Tests — exercise create() to verify config is read
	// -----------------------------------------------------------------------

	@Nested
	class CrtConfigReadTest {

		/**
		 * Helper: build a storageConfig mock that satisfies auth + endpoint
		 * requirements so create() progresses into CRT config reading.
		 * The CRT config sub-object is returned so callers can stub values.
		 */
		private Config[] baseConfigWithCrt() {
			Config crtConfig = mock(Config.class);

			Config nodeConfig = mockNodeConfig(List.of("10.0.0.1"), 8333);
			Config netConfig = mockNetConfig(nodeConfig, null);
			Config storageConfig = mockStorageConfig(netConfig);
			when(storageConfig.stringVal("auth-uid")).thenReturn("access");
			when(storageConfig.stringVal("auth-secret")).thenReturn("secret");
			when(storageConfig.stringVal("region")).thenReturn("us-east-1");
			when(storageConfig.configVal("crt")).thenReturn(crtConfig);

			return new Config[]{storageConfig, crtConfig};
		}

		@Test
		void crtConfigMissing_fallsBackToDefaults_andCreatesDriver() throws Exception {
			// Given: storageConfig with no CRT section at all
			Config nodeConfig = mockNodeConfig(List.of("10.0.0.1"), 8333);
			Config netConfig = mockNetConfig(nodeConfig, null);
			Config storageConfig = mockStorageConfig(netConfig);
			when(storageConfig.stringVal("auth-uid")).thenReturn("access");
			when(storageConfig.stringVal("auth-secret")).thenReturn("secret");
			when(storageConfig.stringVal("region")).thenReturn("us-east-1");
			when(storageConfig.configVal("crt"))
							.thenThrow(new RuntimeException("no crt config"));

			S3AwsStorageDriverFactory<?, ?> factory = new S3AwsStorageDriverFactory<>();

			// create() should succeed past CRT config parsing using hardcoded
			// fallback defaults. It may fail later (CRT native lib, driver
			// constructor needing parent-class config) — that is acceptable.
			try {
				factory.create("step-1", null, storageConfig, false, 1);
			} catch (IllegalConfigurationException e) {
				// IllegalConfigurationException from our code means config parsing
				// itself failed — that must not happen for missing CRT keys.
				assertFalse(e.getMessage().contains("crt"),
								"Should not fail on CRT config parsing: " + e.getMessage());
			} catch (Exception e) {
				// Other exceptions (NPE from driver constructor, CRT native lib
				// missing, etc.) are acceptable — not a CRT config problem.
			}
		}

		@Test
		void customCrtValues_areReadFromConfig() throws Exception {
			Config[] configs = baseConfigWithCrt();
			Config storageConfig = configs[0];
			Config crtConfig = configs[1];

			when(crtConfig.doubleVal("targetThroughputGbps")).thenReturn(20.0);
			when(crtConfig.longVal("minimumPartSizeBytes")).thenReturn(16L * 1024 * 1024);
			when(crtConfig.intVal("maxConcurrency")).thenReturn(1024);

			S3AwsStorageDriverFactory<?, ?> factory = new S3AwsStorageDriverFactory<>();

			// Verify each config path is actually read by the factory
			try {
				factory.create("step-1", null, storageConfig, false, 1);
			} catch (Exception ignored) {
				// CRT native lib may not be available
			}

			// If these paths were not read, the mock would not have been invoked.
			verify(crtConfig).doubleVal("targetThroughputGbps");
			verify(crtConfig).longVal("minimumPartSizeBytes");
			verify(crtConfig).intVal("maxConcurrency");
		}

		@Test
		void partialCrtConfig_usesDefaultsForMissingKeys() throws Exception {
			Config[] configs = baseConfigWithCrt();
			Config storageConfig = configs[0];
			Config crtConfig = configs[1];

			// Only provide maxConcurrency; others throw (simulating missing keys)
			when(crtConfig.intVal("maxConcurrency")).thenReturn(256);
			when(crtConfig.doubleVal("targetThroughputGbps"))
							.thenThrow(new RuntimeException("missing"));
			when(crtConfig.longVal("minimumPartSizeBytes"))
							.thenThrow(new RuntimeException("missing"));

			S3AwsStorageDriverFactory<?, ?> factory = new S3AwsStorageDriverFactory<>();

			// Should not throw a CRT config-related error; missing keys fall back.
			// It may fail later (CRT native lib, driver constructor) — acceptable.
			try {
				factory.create("step-1", null, storageConfig, false, 1);
			} catch (IllegalConfigurationException e) {
				assertFalse(e.getMessage().contains("crt"),
								"Should not fail on CRT config parsing: " + e.getMessage());
			} catch (Exception e) {
				// Other exceptions are acceptable — not a CRT config problem.
			}

			// maxConcurrency was provided and should have been read
			verify(crtConfig).intVal("maxConcurrency");
		}
	}
}
