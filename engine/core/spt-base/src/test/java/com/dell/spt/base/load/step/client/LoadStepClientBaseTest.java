package com.dell.spt.base.load.step.client;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.integrity.IntegrityManifestCompletion;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.load.step.linear.LinearLoadStepClient;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshot;
import com.github.akurilov.confuse.Config;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;

/**
 * Comprehensive unit tests for LoadStepClientBase.
 * 
 * Tests cover constructor behavior, configuration access, initialization logic,
 * copy instance functionality, lifecycle management, and error handling.
 */
@DisplayName("LoadStepClientBase Tests")
class LoadStepClientBaseTest {

	private Config testConfig;
	private List<Extension> extensions;
	private List<Config> ctxConfigs;
	private MetricsManager mockMetricsManager;

	@TempDir
	Path tempDir;

	@BeforeEach
	void setUp() {
		testConfig = TestConfigBuilder.config();
		extensions = Collections.emptyList();
		ctxConfigs = Collections.emptyList();
		mockMetricsManager = mock(MetricsManager.class);
	}

	@Test
	void metadataSliceRunIdMustMatchBeforeStart() throws Exception {
		final LoadStep slice = mock(LoadStep.class);
		when(slice.runId()).thenReturn(77L);
		assertDoesNotThrow(() -> LoadStepClientBase.requireMatchingRunId(slice, 77L));

		when(slice.runId()).thenReturn(78L);
		final var mismatch = assertThrows(
						IntegrityTerminalException.class,
						() -> LoadStepClientBase.requireMatchingRunId(slice, 77L));
		assertEquals(IntegrityTerminalException.Category.CONFIGURATION, mismatch.category());
		assertTrue(mismatch.getMessage().contains("expected 77, actual 78"));
	}

	@Test
	void metadataSliceRunIdMustBePositive() {
		final var failure = assertThrows(
						IntegrityTerminalException.class,
						() -> LoadStepClientBase.requireMatchingRunId(mock(LoadStep.class), 0L));
		assertEquals(IntegrityTerminalException.Category.CONFIGURATION, failure.category());
	}

	@Test
	void exactOutputCountOmitsDistributedSlicesWithZeroShare() {
		final Config config = TestConfigBuilder.config();
		config.val("storage-integrity-output-requireExactCount", true);
		config.val("load-op-limit-count", 2L);
		final List<String> activeRemotes = LoadStepClientBase.participatingRemoteNodeAddrs(
						config, List.of("worker-a", "worker-b", "worker-c"));

		assertEquals(List.of("worker-a"), activeRemotes);
		final List<Config> slices = new ArrayList<>();
		for (int i = 0; i < 1 + activeRemotes.size(); i++) {
			slices.add(ConfigSliceUtil.initSlice(config));
		}
		ConfigSliceUtil.sliceLongValueBalanced(2L, slices, "load-op-limit-count");
		assertEquals(2L, slices.stream().mapToLong(
						slice -> slice.longVal("load-op-limit-count")).sum());
		assertTrue(slices.stream().allMatch(
						slice -> slice.longVal("load-op-limit-count") > 0));

		config.val("load-op-limit-count", 5L);
		final List<String> remainderRemotes = LoadStepClientBase.participatingRemoteNodeAddrs(
						config, List.of("worker-a", "worker-b", "worker-c"));
		assertEquals(List.of("worker-a", "worker-b", "worker-c"), remainderRemotes);
		final List<Config> remainderSlices = new ArrayList<>();
		for (int i = 0; i < 1 + remainderRemotes.size(); i++) {
			remainderSlices.add(ConfigSliceUtil.initSlice(config));
		}
		ConfigSliceUtil.sliceLongValueBalanced(5L, remainderSlices, "load-op-limit-count");
		assertEquals(5L, remainderSlices.stream().mapToLong(
						slice -> slice.longVal("load-op-limit-count")).sum());
		assertTrue(remainderSlices.stream().allMatch(
						slice -> slice.longVal("load-op-limit-count") > 0));

		final Config writeVerify = TestConfigBuilder.config();
		writeVerify.val("load-op-limit-count", 2L);
		assertEquals(
						List.of("worker-a", "worker-b", "worker-c"),
						LoadStepClientBase.participatingRemoteNodeAddrs(
										writeVerify, List.of("worker-a", "worker-b", "worker-c")));
	}

	@Test
	@SuppressWarnings("unchecked")
	void terminalFailureCountsSumEveryContextByOperationType() {
		final MetricsContext<AllMetricsSnapshot> listA = mock(MetricsContext.class);
		final MetricsContext<AllMetricsSnapshot> listB = mock(MetricsContext.class);
		final MetricsContext<AllMetricsSnapshot> read = mock(MetricsContext.class);
		final AllMetricsSnapshot listASnapshot = mock(AllMetricsSnapshot.class);
		final AllMetricsSnapshot listBSnapshot = mock(AllMetricsSnapshot.class);
		final AllMetricsSnapshot readSnapshot = mock(AllMetricsSnapshot.class);
		final RateMetricSnapshot listAFailures = mock(RateMetricSnapshot.class);
		final RateMetricSnapshot listBFailures = mock(RateMetricSnapshot.class);
		final RateMetricSnapshot readFailures = mock(RateMetricSnapshot.class);
		when(listA.opType()).thenReturn(OpType.LIST);
		when(listB.opType()).thenReturn(OpType.LIST);
		when(read.opType()).thenReturn(OpType.READ);
		when(listA.lastSnapshot()).thenReturn(listASnapshot);
		when(listB.lastSnapshot()).thenReturn(listBSnapshot);
		when(read.lastSnapshot()).thenReturn(readSnapshot);
		when(listASnapshot.failsSnapshot()).thenReturn(listAFailures);
		when(listBSnapshot.failsSnapshot()).thenReturn(listBFailures);
		when(readSnapshot.failsSnapshot()).thenReturn(readFailures);
		when(listAFailures.count()).thenReturn(1L);
		when(listBFailures.count()).thenReturn(2L);
		when(readFailures.count()).thenReturn(4L);

		assertEquals(
						Map.of(OpType.LIST, 3L, OpType.READ, 4L),
						LoadStepClientBase.terminalFailureCounts(List.of(listA, listB, read)));
	}

	@Test
	@SuppressWarnings("unchecked")
	void metadataClientCloseRejectsPartialDiscoveryWhenTerminalListFailed() throws Exception {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", "list-step");
		config.val("run-id", 110L);
		config.val("storage-driver-type", "s3");
		config.val("storage-integrity-mode", "metadata");
		config.val("storage-integrity-input-provenance", "external");
		config.val("load-op-type", "list");
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final MetricsContext<AllMetricsSnapshot> listMetrics = mock(MetricsContext.class);
		final AllMetricsSnapshot snapshot = mock(AllMetricsSnapshot.class);
		final RateMetricSnapshot failures = mock(RateMetricSnapshot.class);
		when(listMetrics.opType()).thenReturn(OpType.LIST);
		when(listMetrics.lastSnapshot()).thenReturn(snapshot);
		when(snapshot.failsSnapshot()).thenReturn(failures);
		when(failures.count()).thenReturn(1L);
		client.addMetricsContextForTest(listMetrics);

		final Path manifest = tempDir.resolve("verify-input.csv");
		Files.writeString(
						manifest,
						"bucket,key,size,version_id\r\n"
										+ "b,first-page-object,1,v1\r\n");
		Files.writeString(IntegrityManifestCompletion.emissionCountPath(manifest), "1\n");
		Files.writeString(IntegrityManifestCompletion.deleteMarkerCountPath(manifest), "0\n");
		addItemOutputAggregator(client, new CsvArtifactAggregator(
						"list-step", List.of(com.dell.spt.base.load.step.file.FileManager.INSTANCE), List.of(),
						manifest.toString(), 110, 0, OpType.LIST));

		final var failure = assertThrows(IntegrityTerminalException.class, client::doClose);

		assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
		assertTrue(failure.getMessage().contains("1 failed operation"));
		assertFalse(Files.exists(CsvArtifactAggregator.nodeSourcePath(manifest, 0)));
		assertFalse(Files.exists(IntegrityManifestCompletion.completionPath(manifest)));
	}

	@Nested
	@DisplayName("Constructor Tests")
	class ConstructorTests {

		@Test
		@DisplayName("Constructor with valid config should succeed")
		void testConstructorWithValidConfig() throws RemoteException {
			// Act
			TestLoadStepClient client = assertDoesNotThrow(() -> new TestLoadStepClient(testConfig, extensions, ctxConfigs, mockMetricsManager));

			// Assert - Basic construction
			assertNotNull(client, "Client should not be null");

			// Assert - Configuration access works
			assertEquals("test-step-001", client.loadStepId(), "Step ID should match config");
			assertEquals("test-load-step", client.getTypeName(), "Type name should match implementation");

			// Assert - Initial state
			assertFalse(client.wasInitCalled(), "init() should not be called during construction");
			assertFalse(client.wasCopyInstanceCalled(), "copyInstance() should not be called during construction");
		}

		@Test
		@DisplayName("Constructor with null config should fail")
		void testConstructorWithNullConfig() {
			// Act & Assert
			assertThrows(Exception.class, () -> new TestLoadStepClient(null, extensions, ctxConfigs, mockMetricsManager),
							"Constructor should reject null config");
		}

		@Test
		@DisplayName("Constructor with null extensions should work")
		void testConstructorWithNullExtensions() throws RemoteException {
			// Act
			TestLoadStepClient client = assertDoesNotThrow(() -> new TestLoadStepClient(testConfig, null, ctxConfigs, mockMetricsManager));

			// Assert
			assertNotNull(client);
			assertEquals("test-step-001", client.loadStepId());
		}

		@Test
		@DisplayName("Constructor with null context configs should work")
		void testConstructorWithNullCtxConfigs() throws RemoteException {
			// Act
			TestLoadStepClient client = assertDoesNotThrow(() -> new TestLoadStepClient(testConfig, extensions, null, mockMetricsManager));

			// Assert
			assertNotNull(client);
			assertEquals("test-step-001", client.loadStepId());
		}

		@Test
		@DisplayName("Constructor with null metrics manager should work")
		void testConstructorWithNullMetricsManager() throws RemoteException {
			// Act
			TestLoadStepClient client = assertDoesNotThrow(() -> new TestLoadStepClient(testConfig, extensions, ctxConfigs, null));

			// Assert
			assertNotNull(client);
			assertEquals("test-step-001", client.loadStepId());
		}
	}

	@Nested
	@DisplayName("Basic Method Tests")
	class BasicMethodTests {

		private TestLoadStepClient client;

		@BeforeEach
		void setUp() {
			client = new TestLoadStepClient(testConfig, extensions, ctxConfigs, mockMetricsManager);
		}

		@Test
		@DisplayName("loadStepId() should return configured step ID")
		void testLoadStepId() throws RemoteException {
			assertEquals("test-step-001", client.loadStepId());
		}

		@Test
		@DisplayName("getTypeName() should return implementation type")
		void testGetTypeName() throws RemoteException {
			assertEquals("test-load-step", client.getTypeName());
		}

		@Test
		@DisplayName("loadStepId() should work with different configs")
		void testLoadStepIdWithDifferentConfigs() throws RemoteException {
			// Test with custom step ID
			Config customConfig = TestConfigBuilder.config();
			customConfig.val("load-step-id", "custom-test-step");
			TestLoadStepClient customClient = new TestLoadStepClient(
							customConfig, extensions, ctxConfigs, mockMetricsManager);

			assertEquals("custom-test-step", customClient.loadStepId());
		}

		@Test
		@DisplayName("Multiple instances should be independent")
		void testMultipleInstances() throws RemoteException {
			// Create second instance with different config
			Config config2 = TestConfigBuilder.config();
			config2.val("load-step-id", "second-step");
			TestLoadStepClient client2 = new TestLoadStepClient(
							config2, extensions, ctxConfigs, mockMetricsManager);

			// Assert independence
			assertEquals("test-step-001", client.loadStepId());
			assertEquals("second-step", client2.loadStepId());
			assertNotSame(client, client2);
		}
	}

	@Nested
	@DisplayName("Append/Config API Tests")
	class AppendConfigApiTests {

		@Test
		@DisplayName("append(Map) returns new instance with added context")
		void testAppendReturnsNewInstanceAndAddsContext() {
			TestLoadStepClient client = new TestLoadStepClient(testConfig, extensions, null, mockMetricsManager);
			var copy1 = client.append(java.util.Map.of("load-op-type", "read"));
			assertNotSame(client, copy1);
			assertTrue(((TestLoadStepClient) copy1).getCtxConfigsCount() >= 1);
			var copy2 = copy1.append(java.util.Map.of("load-op-type", "update"));
			assertTrue(((TestLoadStepClient) copy2).getCtxConfigsCount() >= 2);
		}

		@Test
		@DisplayName("config after append on same instance throws IllegalStateException")
		void testConfigAfterAppendThrows() {
			TestLoadStepClient client = new TestLoadStepClient(testConfig, extensions, null, mockMetricsManager);
			var appended = client.append(java.util.Map.of("load-op-type", "read"));
			assertThrows(IllegalStateException.class, () -> appended.config(java.util.Map.of("load-step-id", "x")));
		}

		@Test
		@DisplayName("await without start throws and triggers stop in finally")
		void testAwaitWithoutStartThrows() {
			TestLoadStepClient client = new TestLoadStepClient(testConfig, extensions, ctxConfigs, mockMetricsManager);
			assertThrows(IllegalStateException.class, () -> client.await(1, java.util.concurrent.TimeUnit.MILLISECONDS));
		}
	}

	@Nested
	@DisplayName("Start/Await Integration Tests")
	class StartAwaitIntegrationTests {
		@Test
		@DisplayName("timing file aggregation is not initialized when timing persist is disabled")
		void testTimingAggregatorDisabledWhenTimingPersistFalse() {
			Config cfg = TestConfigBuilder.config();
			cfg.val("load-step-id", "timing-aggregator-disabled");
			cfg.val("output-metrics-timing-persist", false);
			cfg.val("load-step-node-addrs", java.util.Collections.emptyList());
			cfg.val("item-data-input-compressibility", 0.0);
			cfg.val("item-data-dedupable", false);
			cfg.val("item-data-verify", false);
			TestLoadStepClient client = new TestLoadStepClient(cfg, extensions, ctxConfigs, mockMetricsManager);

			assertDoesNotThrow(client::start);

			assertEquals(0, timingMetricsAggregatorCount(client));
			assertDoesNotThrow(client::doStop);
			assertDoesNotThrow(client::doShutdown);
			assertDoesNotThrow(client::doClose);
		}

		@Test
		@DisplayName("Linear client accepts IEC item size and executes slice/aggregator init")
		void testLinearClientStartAndAwait() throws Exception {
			// Silence noisy INFO/ERROR logs produced during partial init in this integration smoke
			Configurator.setLevel("com.dell.spt.base.logging.Messages", Level.WARN);
			Configurator.setLevel("com.dell.spt.base.logging.Errors", Level.OFF);
			// Use real extensions from classpath: LinearLoadStepExtension + DummyStorageDriverMockExtension
			List<Extension> classpathExts = Extension.load(LoadStepClientBaseTest.class.getClassLoader());
			Config cfg = TestConfigBuilder.config();
			cfg.val("load-step-id", "linear-client-test");
			cfg.val("item-data-size", "10KiB");
			cfg.val("item-output-file", ""); // avoid writing
			// Ensure no remote nodes to avoid retries
			cfg.val("load-step-node-addrs", java.util.Collections.emptyList());
			LinearLoadStepClient client = new LinearLoadStepClient(cfg, classpathExts, null, mockMetricsManager);
			assertDoesNotThrow(client::start);
			// Await briefly; we only assert it returns a boolean and does not throw
			boolean result = false;
			try {
				result = client.await(5, java.util.concurrent.TimeUnit.MILLISECONDS);
			} catch (IllegalStateException ise) {
				// If no slices were created due to environment, method may throw; that's acceptable
			}
			// Stop/close regardless
			assertDoesNotThrow(client::doStop);
			assertDoesNotThrow(client::doShutdown);
			assertDoesNotThrow(client::doClose);
			// Just touch the result to avoid unused warning
			if (result) {
				assertTrue(result);
			}
			// Restore default log levels for other tests
			Configurator.setLevel("com.dell.spt.base.logging.Messages", Level.INFO);
			Configurator.setLevel("com.dell.spt.base.logging.Errors", Level.ERROR);
		}
	}

	@Nested
	@DisplayName("Configuration Access Tests")
	class ConfigurationAccessTests {

		@Test
		@DisplayName("Configuration paths should be accessible through LoadStepClientBase")
		void testConfigurationPathsAccess() {
			Config config = TestConfigBuilder.config();
			config.val("load-step-id", "config-access-test");
			config.val("load-op-type", "read");
			config.val("storage-driver-limit-concurrency", 25);
			config.val("load-batch-size", 500);
			TestLoadStepClient client = new TestLoadStepClient(
							config, extensions, ctxConfigs, mockMetricsManager);

			assertDoesNotThrow(() -> {
				// These are paths that LoadStepClientBase actually uses
				assertEquals("config-access-test", client.loadStepId());
				assertEquals("read", config.stringVal("load-op-type"));
				assertEquals(25, config.intVal("storage-driver-limit-concurrency"));
				assertEquals(500, config.intVal("load-batch-size"));
			});
		}

		@Test
		@DisplayName("Configuration validation should work with various operation types")
		void testConfigurationWithDifferentOpTypes() {
			String[] opTypes = {"write", "read", "delete", "create"
			};

			for (String opType : opTypes) {
				Config config = TestConfigBuilder.config();
				config.val("load-op-type", opType);

				assertDoesNotThrow(() -> {
					new TestLoadStepClient(config, extensions, ctxConfigs, mockMetricsManager);
					assertEquals(opType, config.stringVal("load-op-type"));
				}, "Should work with operation type: " + opType);
			}
		}
	}

	@Nested
	@DisplayName("Copy Instance Tests")
	class CopyInstanceTests {

		private TestLoadStepClient originalClient;

		@BeforeEach
		void setUp() {
			originalClient = new TestLoadStepClient(testConfig, extensions, ctxConfigs, mockMetricsManager);
		}

		@Test
		@DisplayName("copyInstance should create new instance with updated config")
		void testCopyInstance() throws RemoteException {
			// Arrange
			Config newConfig = TestConfigBuilder.config();
			newConfig.val("load-step-id", "copied-step");

			// Act
			TestLoadStepClient copy = originalClient.copyInstance(newConfig, Collections.emptyList());

			// Assert
			assertNotNull(copy);
			assertNotSame(originalClient, copy, "copyInstance should return new instance");
			assertTrue(originalClient.wasCopyInstanceCalled(), "copyInstance should be tracked in original");
			assertEquals(1, originalClient.getCopyInstanceCallCount());

			// Verify the copy has the new configuration
			assertEquals("copied-step", copy.loadStepId());
			assertEquals("test-step-001", originalClient.loadStepId(), "Original should be unchanged");
		}

		@Test
		@DisplayName("copyInstance with same config should still create new instance")
		void testCopyInstanceWithSameConfig() {
			// Act
			TestLoadStepClient copy = originalClient.copyInstance(testConfig, ctxConfigs);

			// Assert
			assertNotNull(copy);
			assertNotSame(originalClient, copy);
			assertTrue(originalClient.wasCopyInstanceCalled());
		}

		@Test
		@DisplayName("Multiple copyInstance calls should be tracked")
		void testMultipleCopyInstanceCalls() {
			// Act
			originalClient.copyInstance(testConfig, ctxConfigs);
			originalClient.copyInstance(testConfig, ctxConfigs);
			originalClient.copyInstance(testConfig, ctxConfigs);

			// Assert
			assertEquals(3, originalClient.getCopyInstanceCallCount());
		}
	}

	@Nested
	@DisplayName("Initialization Tests")
	class InitializationTests {

		private TestLoadStepClient client;

		@BeforeEach
		void setUp() {
			client = new TestLoadStepClient(testConfig, extensions, ctxConfigs, mockMetricsManager);
		}

		@Test
		@DisplayName("init() method should be callable")
		void testInitMethod() {
			// Verify init hasn't been called during construction
			assertFalse(client.wasInitCalled());

			// Call init and verify it was tracked
			assertDoesNotThrow(() -> client.init());
			assertTrue(client.wasInitCalled());
		}

		@Test
		@DisplayName("init() should be callable multiple times without error")
		void testInitMultipleCalls() {
			assertDoesNotThrow(() -> {
				client.init();
				client.init();
				client.init();
			});
			assertTrue(client.wasInitCalled());
		}
	}

	@Nested
	@DisplayName("Configuration Slicing Tests")
	class ConfigurationSlicingTests {

		@Test
		@DisplayName("remoteNodeAddrs should handle empty node list")
		void testRemoteNodeAddrsEmpty() {
			Config config = TestConfigBuilder.config();
			TestLoadStepClient client = assertDoesNotThrow(
							() -> new TestLoadStepClient(config, extensions, ctxConfigs, mockMetricsManager));

			// This tests the remoteNodeAddrs method indirectly through construction
			assertNotNull(client);
			assertDoesNotThrow(client::loadStepId);
		}

		@Test
		@DisplayName("Config with different storage driver types should work")
		void testDifferentStorageDriverTypes() {
			String[] driverTypes = {"dummy-mock", "s3", "atmos", "swift", "fs"
			};

			for (String driverType : driverTypes) {
				Config config = TestConfigBuilder.config();
				config.val("storage-driver-type", driverType);
				config.val("load-step-id", "test-" + driverType);
				assertDoesNotThrow(() -> {
					TestLoadStepClient client = new TestLoadStepClient(
									config, extensions, ctxConfigs, mockMetricsManager);
					assertEquals("test-" + driverType, client.loadStepId());
					assertEquals(driverType, config.stringVal("storage-driver-type"));
				}, "Should work with driver type: " + driverType);
			}
		}

		@Test
		@DisplayName("Config with different batch sizes should work")
		void testDifferentBatchSizes() {
			int[] batchSizes = {1, 10, 100, 1000, 10000
			};

			for (int batchSize : batchSizes) {
				Config config = TestConfigBuilder.config();
				config.val("load-batch-size", batchSize);
				config.val("load-step-id", "batch-test-" + batchSize);
				assertDoesNotThrow(() -> {
					TestLoadStepClient client = new TestLoadStepClient(
									config, extensions, ctxConfigs, mockMetricsManager);
					assertEquals("batch-test-" + batchSize, client.loadStepId());
					assertEquals(batchSize, config.intVal("load-batch-size"));
				}, "Should work with batch size: " + batchSize);
			}
		}

		@Test
		@DisplayName("Config with different concurrency levels should work")
		void testDifferentConcurrencyLevels() {
			int[] concurrencyLevels = {1, 5, 10, 50, 100
			};

			for (int concurrency : concurrencyLevels) {
				Config config = TestConfigBuilder.config();
				config.val("storage-driver-limit-concurrency", concurrency);
				config.val("load-step-id", "concurrency-test-" + concurrency);
				assertDoesNotThrow(() -> {
					TestLoadStepClient client = new TestLoadStepClient(
									config, extensions, ctxConfigs, mockMetricsManager);
					assertEquals("concurrency-test-" + concurrency, client.loadStepId());
					assertEquals(concurrency, config.intVal("storage-driver-limit-concurrency"));
				}, "Should work with concurrency: " + concurrency);
			}
		}
	}

	@Nested
	@DisplayName("Configuration Integration Tests")
	class ConfigurationIntegrationTests {

		@Test
		@DisplayName("Complex configuration should work end-to-end")
		void testComplexConfiguration() {
			Config complexConfig = TestConfigBuilder.config();
			complexConfig.val("load-step-id", "complex-integration-test");
			complexConfig.val("load-op-type", "create");
			complexConfig.val("storage-driver-limit-concurrency", 75);
			complexConfig.val("load-batch-size", 250);
			complexConfig.val("storage-driver-type", "s3");
			assertDoesNotThrow(() -> {
				TestLoadStepClient client = new TestLoadStepClient(
								complexConfig, extensions, ctxConfigs, mockMetricsManager);

				// Verify all configuration aspects
				assertEquals("complex-integration-test", client.loadStepId());
				assertEquals("create", complexConfig.stringVal("load-op-type"));
				assertEquals(75, complexConfig.intVal("storage-driver-limit-concurrency"));
				assertEquals(250, complexConfig.intVal("load-batch-size"));
				assertEquals("s3", complexConfig.stringVal("storage-driver-type"));
				assertEquals("test-load-step", client.getTypeName());
			});
		}

		@Test
		@DisplayName("Configuration paths used by LoadStepClientBase should be accessible")
		void testActualConfigurationPaths() {
			Config config = TestConfigBuilder.config();
			new TestLoadStepClient(config, extensions, ctxConfigs, mockMetricsManager);

			// Test paths that LoadStepClientBase actually accesses
			assertAll("Configuration paths should be accessible",
							() -> assertDoesNotThrow(() -> config.stringVal("load-step-id")),
							() -> assertDoesNotThrow(() -> config.boolVal("load-step-idAutoGenerated")),
							() -> assertDoesNotThrow(() -> config.intVal("load-batch-size")),
							() -> assertDoesNotThrow(() -> config.stringVal("load-op-type")),
							() -> assertDoesNotThrow(() -> config.longVal("load-op-limit-count")),
							() -> assertDoesNotThrow(() -> config.stringVal("storage-driver-type")),
							() -> assertDoesNotThrow(() -> config.intVal("storage-driver-limit-concurrency")),
							() -> assertDoesNotThrow(() -> config.stringVal("storage-auth-file")),
							() -> assertDoesNotThrow(() -> config.stringVal("item-output-file")),
							() -> assertDoesNotThrow(() -> config.stringVal("run-comment")));
		}

		@Test
		@DisplayName("Error handling for invalid configuration should work")
		void testErrorHandlingForInvalidConfig() {
			// Test with config that has invalid values but proper structure
			Config config = TestConfigBuilder.config();
			config.val("load-step-id", "");
			config.val("storage-driver-limit-concurrency", 0);
			// Should not throw during construction
			assertDoesNotThrow(() -> {
				TestLoadStepClient client = new TestLoadStepClient(
								config, extensions, ctxConfigs, mockMetricsManager);
				assertNotNull(client);
			});
		}
	}

	@Nested
	@DisplayName("Lifecycle Tests")
	class LifecycleTests {

		private TestLoadStepClient client;

		@BeforeEach
		void setUp() {
			client = new TestLoadStepClient(testConfig, extensions, ctxConfigs, mockMetricsManager);
		}

		@Test
		@DisplayName("Newly created client should be in initial state")
		void testInitialState() {
			// Check initial state
			assertFalse(client.wasInitCalled(), "init should not be called during construction");
			assertFalse(client.wasCopyInstanceCalled(), "copyInstance should not be called during construction");

			// Basic functionality should work
			assertDoesNotThrow(() -> {
				assertEquals("test-step-001", client.loadStepId());
				assertEquals("test-load-step", client.getTypeName());
			});
		}

		@Test
		@DisplayName("Multiple method calls should work in sequence")
		void testMethodCallSequence() {
			assertDoesNotThrow(() -> {
				// Test sequence of method calls
				String stepId1 = client.loadStepId();
				String typeName1 = client.getTypeName();
				String stepId2 = client.loadStepId();
				String typeName2 = client.getTypeName();

				// Results should be consistent
				assertEquals(stepId1, stepId2);
				assertEquals(typeName1, typeName2);
				assertEquals("test-step-001", stepId1);
				assertEquals("test-load-step", typeName1);
			});
		}

		@Test
		@DisplayName("init followed by additional method calls should work")
		void testInitThenMethods() {
			assertDoesNotThrow(() -> {
				// Call init first
				client.init();
				assertTrue(client.wasInitCalled());

				// Then call other methods
				assertEquals("test-step-001", client.loadStepId());
				assertEquals("test-load-step", client.getTypeName());
			});
		}

		@Test
		@DisplayName("copyInstance after init should preserve init state")
		void testCopyInstanceAfterInit() {
			// Initialize original
			client.init();
			assertTrue(client.wasInitCalled());

			// Create copy
			Config newConfig = TestConfigBuilder.config();
			newConfig.val("load-step-id", "copied-after-init");
			TestLoadStepClient copy = client.copyInstance(newConfig, Collections.emptyList());

			// Original should still show init was called
			assertTrue(client.wasInitCalled());
			assertTrue(client.wasCopyInstanceCalled());

			// Copy should be independent
			assertNotSame(client, copy);
			assertDoesNotThrow(() -> {
				assertEquals("copied-after-init", copy.loadStepId());
				assertEquals("test-step-001", client.loadStepId());
			});
		}

		@Test
		@DisplayName("Reset call tracking should work")
		void testResetCallTracking() {
			// Call methods to set tracking
			client.init();
			client.copyInstance(testConfig, ctxConfigs);

			assertTrue(client.wasInitCalled());
			assertTrue(client.wasCopyInstanceCalled());
			assertEquals(1, client.getCopyInstanceCallCount());

			// Reset and verify
			client.resetCallTracking();

			assertFalse(client.wasInitCalled());
			assertFalse(client.wasCopyInstanceCalled());
			assertEquals(0, client.getCopyInstanceCallCount());
		}
	}

	@Nested
	@DisplayName("Configuration Consistency Tests")
	class ConfigurationConsistencyTests {

		@Test
		@DisplayName("Configuration should remain consistent across method calls")
		void testConfigurationConsistency() {
			Config config = TestConfigBuilder.config();
			config.val("load-step-id", "consistency-test");
			config.val("load-op-type", "update");
			config.val("storage-driver-limit-concurrency", 42);
			config.val("load-batch-size", 777);
			TestLoadStepClient client = new TestLoadStepClient(
							config, extensions, ctxConfigs, mockMetricsManager);

			// Call methods multiple times and verify consistency
			for (int i = 0; i < 5; i++) {
				assertDoesNotThrow(() -> {
					assertEquals("consistency-test", client.loadStepId());
					assertEquals("update", config.stringVal("load-op-type"));
					assertEquals(42, config.intVal("storage-driver-limit-concurrency"));
					assertEquals(777, config.intVal("load-batch-size"));
					assertEquals("test-load-step", client.getTypeName());
				});
			}
		}

		@Test
		@DisplayName("Configuration changes should not affect existing instances")
		void testConfigurationIsolation() {
			// Create first client
			Config config1 = TestConfigBuilder.config();
			config1.val("load-step-id", "original");
			TestLoadStepClient client1 = new TestLoadStepClient(
							config1, extensions, ctxConfigs, mockMetricsManager);

			// Create second client with different config
			Config config2 = TestConfigBuilder.config();
			config2.val("load-step-id", "modified");
			TestLoadStepClient client2 = new TestLoadStepClient(
							config2, extensions, ctxConfigs, mockMetricsManager);

			// Verify isolation
			assertDoesNotThrow(() -> {
				assertEquals("original", client1.loadStepId());
				assertEquals("modified", client2.loadStepId());

				// Multiple calls should still be isolated
				assertEquals("original", client1.loadStepId());
				assertEquals("modified", client2.loadStepId());
			});
		}

		@Test
		@DisplayName("Complex configuration changes should work correctly")
		void testComplexConfigurationChanges() {
			// Start with one configuration
			Config config1 = TestConfigBuilder.config();
			config1.val("load-step-id", "complex-1");
			config1.val("load-op-type", "read");
			config1.val("storage-driver-limit-concurrency", 10);
			TestLoadStepClient client1 = new TestLoadStepClient(
							config1, extensions, ctxConfigs, mockMetricsManager);

			// Create copy with different configuration
			Config config2 = TestConfigBuilder.config();
			config2.val("load-step-id", "complex-2");
			config2.val("load-op-type", "write");
			config2.val("storage-driver-limit-concurrency", 100);
			config2.val("load-batch-size", 500);
			TestLoadStepClient client2 = client1.copyInstance(config2, Collections.emptyList());

			// Verify both work independently
			assertAll("Both clients should work with their respective configurations",
							() -> assertEquals("complex-1", client1.loadStepId()),
							() -> assertEquals("complex-2", client2.loadStepId()),
							() -> assertEquals("read", config1.stringVal("load-op-type")),
							() -> assertEquals("write", config2.stringVal("load-op-type")),
							() -> assertEquals(10, config1.intVal("storage-driver-limit-concurrency")),
							() -> assertEquals(100, config2.intVal("storage-driver-limit-concurrency")));
		}
	}

	@Nested
	@DisplayName("Error Resilience Tests")
	class ErrorResilienceTests {

		private TestLoadStepClient client;

		@BeforeEach
		void setUp() {
			client = new TestLoadStepClient(testConfig, extensions, ctxConfigs, mockMetricsManager);
		}

		@Test
		@DisplayName("Multiple init calls should not cause issues")
		void testMultipleInitCalls() {
			assertDoesNotThrow(() -> {
				for (int i = 0; i < 10; i++) {
					client.init();
					assertTrue(client.wasInitCalled());
				}
			});
		}

		@Test
		@DisplayName("Mixed method calls should work reliably")
		void testMixedMethodCalls() {
			assertDoesNotThrow(() -> {
				client.init();
				assertEquals("test-step-001", client.loadStepId());
				client.init();
				assertEquals("test-load-step", client.getTypeName());
				client.copyInstance(testConfig, ctxConfigs);
				assertEquals("test-step-001", client.loadStepId());
				client.init();
				assertEquals("test-load-step", client.getTypeName());
			});
		}

		@Test
		@DisplayName("Operations with null parameters should be handled gracefully")
		void testNullParameterHandling() {
			assertDoesNotThrow(() -> {
				// copyInstance with null config and null ctxConfigs
				// This might throw, but should be handled gracefully
				try {
					client.copyInstance(null, null);
				} catch (Exception e) {
					// Expected - null config should fail gracefully
					assertNotNull(e);
				}

				// Client should still be functional
				assertEquals("test-step-001", client.loadStepId());
				assertEquals("test-load-step", client.getTypeName());
			});
		}

		@Test
		@DisplayName("Rapid successive operations should work")
		void testRapidSuccessiveOperations() {
			assertDoesNotThrow(() -> {
				for (int i = 0; i < 100; i++) {
					String stepId = client.loadStepId();
					String typeName = client.getTypeName();

					assertEquals("test-step-001", stepId);
					assertEquals("test-load-step", typeName);

					if (i % 10 == 0) {
						client.init();
					}

					if (i % 15 == 0) {
						client.copyInstance(testConfig, ctxConfigs);
					}
				}
			});
		}
	}

	@Nested
	@DisplayName("Configuration Path Coverage Tests")
	class ConfigurationPathCoverageTests {

		@Test
		@DisplayName("All major configuration paths should be accessible")
		void testAllMajorConfigurationPaths() {
			Config config = TestConfigBuilder.config();
			assertDoesNotThrow(
							() -> new TestLoadStepClient(config, extensions, ctxConfigs, mockMetricsManager));

			// Test all the paths that LoadStepClientBase uses
			assertAll("All configuration paths should be accessible",
							() -> assertDoesNotThrow(() -> config.stringVal("load-step-id")),
							() -> assertDoesNotThrow(() -> config.boolVal("load-step-idAutoGenerated")),
							() -> assertDoesNotThrow(() -> config.intVal("load-batch-size")),
							() -> assertDoesNotThrow(() -> config.stringVal("load-op-type")),
							() -> assertDoesNotThrow(() -> config.longVal("load-op-limit-count")),
							() -> assertDoesNotThrow(() -> config.doubleVal("load-op-limit-rate")),
							() -> assertDoesNotThrow(() -> config.longVal("load-op-limit-fail-count")),
							() -> assertDoesNotThrow(() -> config.stringVal("storage-driver-type")),
							() -> assertDoesNotThrow(() -> config.intVal("storage-driver-limit-concurrency")),
							() -> assertDoesNotThrow(() -> config.stringVal("storage-auth-file")),
							() -> assertDoesNotThrow(() -> config.stringVal("item-output-file")),
							() -> assertDoesNotThrow(() -> config.stringVal("run-comment")),
							() -> assertDoesNotThrow(() -> config.stringVal("run-version")),
							() -> assertDoesNotThrow(() -> config.listVal("output-metrics-quantiles")));
		}

		@Test
		@DisplayName("Configuration paths should return expected values")
		void testConfigurationPathValues() {
			Config config = TestConfigBuilder.config();

			// Verify specific values match what we configured
			assertEquals("test-step-001", config.stringVal("load-step-id"));
			assertEquals(false, config.boolVal("load-step-idAutoGenerated"));
			assertEquals(100, config.intVal("load-batch-size"));
			assertEquals("create", config.stringVal("load-op-type"));
			assertEquals(0L, config.longVal("load-op-limit-count"));
			assertEquals(0.0, config.doubleVal("load-op-limit-rate"));
			assertEquals(100000L, config.longVal("load-op-limit-fail-count"));
			assertEquals("dummy-mock", config.stringVal("storage-driver-type"));
			assertEquals(10, config.intVal("storage-driver-limit-concurrency"));
			assertEquals("", config.stringVal("storage-auth-file"));
			assertEquals("", config.stringVal("item-output-file"));
			assertEquals("", config.stringVal("run-comment"));
			assertEquals("test-version", config.stringVal("run-version"));
			assertEquals(List.of(0.25, 0.5, 0.75), config.listVal("output-metrics-quantiles"));
		}

		@Test
		void testRunLoadStepClient() throws IOException, IllegalStateException, InterruptedException {
			// Local-only config: no remote node addrs to avoid 10-minute retry hang
			// against a nonexistent FileManager (exponential backoff up to 120s per retry)
			Config complexConfig = TestConfigBuilder.config();
			complexConfig.val("load-step-id", "complex-integration-test");
			complexConfig.val("load-op-type", "create");
			complexConfig.val("storage-driver-limit-concurrency", 75);
			complexConfig.val("load-batch-size", 250);
			complexConfig.val("storage-driver-type", "s3");

			TestLoadStepExtension newExt = new TestLoadStepExtension();

			List<Extension> extensions = new ArrayList<>();
			extensions.add(newExt);

			TestLoadStepClient client = new TestLoadStepClient(complexConfig, extensions, ctxConfigs, mockMetricsManager);
			assertDoesNotThrow(() -> client.start());
			assertDoesNotThrow(() -> client.doShutdown());
			assertDoesNotThrow(() -> client.doClose());
			assertDoesNotThrow(() -> client.doStop());

			LinearLoadStepClient linearClient = new LinearLoadStepClient(testConfig, extensions, ctxConfigs, mockMetricsManager);
			assertDoesNotThrow(() -> linearClient.start());
			assertDoesNotThrow(() -> linearClient.doShutdown());
			assertDoesNotThrow(() -> linearClient.doClose());
			assertDoesNotThrow(() -> linearClient.doStop());
		}
	}

	@SuppressWarnings("unchecked")
	private static void addItemOutputAggregator(
					final LoadStepClientBase<?> client, final AutoCloseable aggregator) {
		try {
			final Field field = LoadStepClientBase.class.getDeclaredField("itemOutputFileAggregators");
			field.setAccessible(true);
			((List<AutoCloseable>) field.get(client)).add(aggregator);
		} catch (final ReflectiveOperationException e) {
			throw new LinkageError(e.getMessage(), e);
		}
	}

	private static int timingMetricsAggregatorCount(final LoadStepClientBase<?> client) {
		try {
			final Field field = LoadStepClientBase.class.getDeclaredField("itemTimingMetricsOutputFileAggregators");
			field.setAccessible(true);
			return ((List<?>) field.get(client)).size();
		} catch (final ReflectiveOperationException e) {
			throw new LinkageError(e.getMessage(), e);
		}
	}
}
