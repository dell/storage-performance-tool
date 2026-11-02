package com.dell.spt.load.step.mixed;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import com.dell.spt.base.config.ConfigFormat;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.config.InitialConfigSchemaProvider;
import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.integrity.IntegrityTerminalException.Category;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.StorageDriverFactory;
import com.dell.spt.base.storage.driver.bucket.TargetBucketControl;
import com.dell.spt.base.storage.driver.bucket.TargetBucketControlFactory;
import com.dell.spt.base.storage.driver.bucket.TargetBucketResponse;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Target-bucket setup of a mixed step through its real factory and start path: the step inherits
 * load.op.type=create, keeps its GET/STAT pool in item.input.file and has no context
 * configurations when provisioning runs.
 */
class MixedTargetBucketSetupTest {

	@TempDir
	Path home;

	private final ScriptedControl control = new ScriptedControl();

	@Test
	void seededMixedChecksItsMembersWithoutCreatingAny() throws Exception {
		final var config = mixedConfig(4, pool("/bench-0/a", "/bench-3/b"));
		config.val("storage-targetBucket-provision", false);
		control.check("bench-3", TargetBucketResponse.ofHttpStatus(403));

		final var failure = startFailure(config);

		assertEquals(Category.EXECUTION, failure.category(), failure.getMessage());
		assertEquals(List.of("HEAD bench-0", "HEAD bench-1", "HEAD bench-2", "HEAD bench-3"), control.requests);
	}

	@Test
	void externalMixedCreatesMissingMembersAndOnlyChecksDisjointInputBuckets() throws Exception {
		final var config = mixedConfig(3, pool("/read-source/a", "/read-source/b"));
		config.val("load-op-delete-input-file", items("deletes.csv", "/delete-source/x").toString());
		for (int i = 0; i < 3; i++) {
			control.check("bench-" + i, TargetBucketResponse.notFound());
		}
		control.check("delete-source", TargetBucketResponse.ofHttpStatus(403));

		final var failure = startFailure(config);

		assertEquals(List.of(
						"HEAD bench-0", "PUT bench-0", "HEAD bench-1", "PUT bench-1", "HEAD bench-2", "PUT bench-2",
						"HEAD read-source", "HEAD delete-source"), control.requests);
		assertTrue(failure.getMessage().contains("\"delete-source\" is not accessible (HTTP 403)"), failure.getMessage());
	}

	@Test
	void aMissingDeleteOnlyBucketFailsBeforeAnySliceStarts() throws Exception {
		final var config = mixedConfig(2, pool("/bench/a"));
		config.val("load-op-delete-input-file", items("deletes.csv", "/delete-source/x").toString());
		control.check("delete-source", TargetBucketResponse.notFound());

		final var failure = startFailure(config);

		assertEquals(List.of("HEAD bench-0", "HEAD bench-1", "HEAD bench", "HEAD delete-source"), control.requests);
		assertTrue(failure.getMessage().contains("\"delete-source\" does not exist; this step uses existing objects only"),
						failure.getMessage());
		assertFalse(control.driverCreated, "no slice may start after a setup failure");
	}

	@Test
	void aMixedStepWithoutPutsCreatesNoMembers() throws Exception {
		final var config = mixedConfig(4, pool("/read-source/a"));
		config.val("load-op-weights-get", 60);
		config.val("load-op-weights-stat", 40);
		config.val("load-op-weights-put", 0);
		config.val("load-op-weights-delete", 0);
		control.check("read-source", TargetBucketResponse.ofHttpStatus(403));

		startFailure(config);

		assertEquals(List.of("HEAD read-source"), control.requests);
	}

	@Test
	void aMixedStepWithAnInputPathIsRejectedBeforeAnyRequest() throws Exception {
		final var config = mixedConfig(2, pool("/read-source/a"));
		config.val("item-input-path", "/source");

		final var failure = startFailure(config);

		assertEquals(Category.CONFIGURATION, failure.category());
		assertTrue(failure.getMessage().contains("item.input.path in a mixed step"), failure.getMessage());
		assertTrue(control.requests.isEmpty(), control.requests.toString());
	}

	@Test
	void oneBucketKeepsTodaysMixedSetup() throws Exception {
		final var config = mixedConfig(1, pool("/read-source/a"));
		config.val("load-op-delete-input-file", items("deletes.csv", "/delete-source/x").toString());
		control.check("delete-source", TargetBucketResponse.ofHttpStatus(403));

		assertThrows(Exception.class, () -> start(config));

		assertEquals(List.of("HEAD bench", "HEAD read-source"), control.requests);
		assertTrue(control.driverCreated, "setup passed and the step went on to start its slice");
	}

	@Test
	void seededMixedWithAListChecksItsListedBucketsWithoutCreatingAny() throws Exception {
		final var config = listConfig(pool("/ps-eu/a", "/ps-ap/b"), "ps-eu", "ps-us", "ps-ap");
		config.val("storage-targetBucket-provision", false);
		control.check("ps-ap", TargetBucketResponse.ofHttpStatus(403));

		final var failure = startFailure(config);

		assertEquals(Category.EXECUTION, failure.category(), failure.getMessage());
		assertEquals(List.of("HEAD ps-eu", "HEAD ps-us", "HEAD ps-ap"), control.requests);
	}

	@Test
	void externalMixedWithAListCreatesMissingListedBucketsAndOnlyChecksDisjointInputBuckets() throws Exception {
		final var config = listConfig(pool("/read-source/a", "/read-source/b"), "ps-eu", "PS_US");
		config.val("load-op-delete-input-file", items("deletes.csv", "/delete-source/x").toString());
		control.check("ps-eu", TargetBucketResponse.notFound());
		control.check("PS_US", TargetBucketResponse.notFound());
		control.check("delete-source", TargetBucketResponse.ofHttpStatus(403));

		final var failure = startFailure(config);

		assertEquals(List.of(
						"HEAD ps-eu", "PUT ps-eu", "HEAD PS_US", "PUT PS_US", "HEAD read-source", "HEAD delete-source"),
						control.requests, "the placeholder bucket is never contacted");
		assertTrue(failure.getMessage().contains("\"delete-source\" is not accessible (HTTP 403)"), failure.getMessage());
	}

	@Test
	void aMissingDeleteOnlyBucketFailsBeforeAnySliceStartsWithAList() throws Exception {
		final var config = listConfig(pool("/ps-eu/a"), "ps-eu", "ps-us");
		config.val("load-op-delete-input-file", items("deletes.csv", "/delete-source/x").toString());
		control.check("delete-source", TargetBucketResponse.notFound());

		final var failure = startFailure(config);

		assertEquals(List.of("HEAD ps-eu", "HEAD ps-us", "HEAD delete-source"), control.requests);
		assertTrue(failure.getMessage().contains("\"delete-source\" does not exist; this step uses existing objects only"),
						failure.getMessage());
		assertFalse(control.driverCreated, "no slice may start after a setup failure");
	}

	@Test
	void anInputBucketThatIsAListedNameIsCreatedAsAMemberNotJustChecked() throws Exception {
		final var config = listConfig(pool("/ps-us/a"), "ps-eu", "ps-us");
		control.check("ps-us", TargetBucketResponse.notFound());

		startFailure(config);

		assertEquals(List.of("HEAD ps-eu", "HEAD ps-us", "PUT ps-us"), control.requests);
	}

	@Test
	void aMixedStepWithAListAndAnInputPathIsRejectedBeforeAnyRequest() throws Exception {
		final var config = listConfig(pool("/read-source/a"), "ps-eu", "ps-us");
		config.val("item-input-path", "/source");

		final var failure = startFailure(config);

		assertEquals(Category.CONFIGURATION, failure.category());
		assertTrue(failure.getMessage().contains("storage.targetBucket.names (2 names)"), failure.getMessage());
		assertTrue(failure.getMessage().contains("item.input.path in a mixed step"), failure.getMessage());
		assertTrue(control.requests.isEmpty(), control.requests.toString());
	}

	@Test
	void aListAndACountAboveOneAreRejectedBeforeAnyRequest() throws Exception {
		final var config = listConfig(pool("/read-source/a"), "ps-eu", "ps-us");
		config.val("storage-targetBucket-count", 4);

		final var failure = startFailure(config);

		assertEquals(Category.CONFIGURATION, failure.category());
		assertTrue(failure.getMessage().contains("cannot be combined with storage.targetBucket.count 4"),
						failure.getMessage());
		assertTrue(control.requests.isEmpty(), control.requests.toString());
	}

	@Test
	void aOneNameListKeepsTodaysSingleBucketMixedSetup() throws Exception {
		final var config = listConfig(pool("/read-source/a"), "ps-eu");
		config.val("load-op-delete-input-file", items("deletes.csv", "/delete-source/x").toString());
		control.check("delete-source", TargetBucketResponse.ofHttpStatus(403));

		assertThrows(Exception.class, () -> start(config));

		assertEquals(List.of("HEAD ps-eu", "HEAD read-source"), control.requests);
		assertTrue(control.driverCreated, "setup passed and the step went on to start its slice");
	}

	private RunTerminalException startFailure(final Config config) {
		return assertInstanceOf(RunTerminalException.class, assertThrows(Exception.class, () -> start(config)));
	}

	private void start(final Config config) throws Exception {
		final List<Extension> extensions = List.of(control);
		final var client = new MixedLoadStepExtension().createClient(config, extensions, mock(MetricsManager.class));
		assertTimeoutPreemptively(Duration.ofSeconds(60), () -> {
			try (client) {
				client.start();
			}
		});
	}

	private Config mixedConfig(final int count, final Path pool) throws Exception {
		final Config config = defaultConfig();
		config.val("load-step-id", "mixed-setup");
		config.val("load-step-idAutoGenerated", false);
		config.val("load-step-limit-time", "5s");
		config.val("storage-driver-type", ScriptedControl.TYPE);
		config.val("storage-targetBucket-count", count);
		config.val("item-output-path", "/bench");
		config.val("item-input-file", pool.toString());
		config.val("item-output-file", home.resolve("put-remaining.csv").toString());
		return config;
	}

	private Config listConfig(final Path pool, final String... names) throws Exception {
		final var config = mixedConfig(1, pool);
		config.val("storage-targetBucket-names", List.of(names));
		config.val("item-output-path", "/placeholder");
		return config;
	}

	private Path pool(final String... names) throws IOException {
		return items("pool.csv", names);
	}

	private Path items(final String file, final String... names) throws IOException {
		final var lines = new ArrayList<String>();
		for (final var name : names) {
			lines.add(new DataItemImpl(name, 0, 1024).toString());
		}
		return Files.write(home.resolve(file), lines);
	}

	/** The shipped defaults of the engine and of this extension, as the engine merges them. */
	private static Config defaultConfig() throws Exception {
		final Config core = ConfigUtil.loadConfig(
						resource("/config/defaults.yaml"), ConfigFormat.YAML, InitialConfigSchemaProvider.provider().schema());
		final Config mixed = ConfigUtil.loadConfig(
						resource("/config/defaults-load-op-mixed.yaml"),
						ConfigFormat.YAML,
						new MixedLoadStepExtension().schemaProvider().schema());
		return ConfigUtil.merge(core.pathSep(), List.of(mixed, core));
	}

	private static String resource(final String name) throws IOException {
		try (InputStream stream = MixedTargetBucketSetupTest.class.getResourceAsStream(name)) {
			return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	/** Answers bucket checks from a script and records every request; it cannot run object operations. */
	@SuppressWarnings("rawtypes")
	static final class ScriptedControl implements StorageDriverFactory, TargetBucketControlFactory {

		static final String TYPE = "scripted-mixed-bucket";

		final List<String> requests = Collections.synchronizedList(new ArrayList<>());
		final Map<String, TargetBucketResponse> checks = new HashMap<>();
		volatile boolean driverCreated;

		void check(final String bucket, final TargetBucketResponse response) {
			checks.put(bucket, response);
		}

		@Override
		public String id() {
			return TYPE;
		}

		@Override
		public SchemaProvider schemaProvider() {
			return null;
		}

		@Override
		public void install(final Path dstPath) {}

		@Override
		public Config defaults(final Path appHomePath) {
			return null;
		}

		@Override
		public StorageDriver create(
						final String stepId, final DataInput dataInput, final Config storageConfig, final boolean verifyFlag,
						final int batchSize) {
			driverCreated = true;
			throw new UnsupportedOperationException("setup tests run no object operations");
		}

		@Override
		public TargetBucketControl openTargetBucketControl(final String stepId, final Config storageConfig) {
			return new TargetBucketControl() {
				@Override
				public TargetBucketResponse check(final String bucket, final Credential credential) {
					requests.add("HEAD " + bucket);
					return checks.getOrDefault(bucket, TargetBucketResponse.ok());
				}

				@Override
				public TargetBucketResponse create(final String bucket, final Credential credential) {
					requests.add("PUT " + bucket);
					checks.remove(bucket);
					return TargetBucketResponse.ok();
				}

				@Override
				public void close() {}
			};
		}
	}
}
