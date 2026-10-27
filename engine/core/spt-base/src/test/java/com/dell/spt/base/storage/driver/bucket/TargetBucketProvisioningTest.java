package com.dell.spt.base.storage.driver.bucket;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.Constants;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.control.run.RunScope;
import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.integrity.IntegrityTerminalException.Category;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.ThreadContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TargetBucketProvisioningTest {

	@TempDir
	Path tempDir;

	private final ScriptedTargetBucketExtension extension = new ScriptedTargetBucketExtension();

	static Config stepConfig(final String opType, final String outputPath) throws Exception {
		final var schema = SchemaProvider.resolve(Constants.APP_NAME, Thread.currentThread().getContextClassLoader())
						.stream()
						.findFirst()
						.orElseThrow();
		final var defaults = TargetBucketProvisioningTest.class.getClassLoader().getResource("config/defaults.yaml");
		final Config config = ConfigUtil.loadConfig(Path.of(defaults.toURI()).toFile(), schema);
		config.val("load-step-id", "step");
		config.val("storage-driver-type", ScriptedTargetBucketExtension.TYPE);
		config.val("load-op-type", opType);
		config.val("item-output-path", outputPath);
		return config;
	}

	private void provision(final Config config) {
		TargetBucketProvisioning.provision("step", config, null, List.<Extension> of(extension));
	}

	@Test
	void shippedDefaultEnablesProvisioning() throws Exception {
		assertTrue(stepConfig("create", "/bench").boolVal(TargetBucketProvisioning.POLICY));
	}

	@Test
	void existingBucketIsUsedWithoutMutation() throws Exception {
		provision(stepConfig("create", "/bench/prefix"));

		assertEquals(List.of("HEAD bench"), extension.requests);
		assertEquals(1, extension.opened);
		assertEquals(1, extension.closed);
	}

	@Test
	void missingBucketOfAWritingStepIsCreated() throws Exception {
		extension.check("bench", TargetBucketResponse.notFound());

		provision(stepConfig("create", "/bench"));

		assertEquals(List.of("HEAD bench", "PUT bench"), extension.requests);
		assertEquals(1, extension.closed);
	}

	@Test
	void disabledPolicyValidatesButNeverCreates() throws Exception {
		extension.check("bench", TargetBucketResponse.notFound());
		final var config = stepConfig("create", "/bench");
		config.val(TargetBucketProvisioning.POLICY, false);

		final var failure = assertThrows(RunTerminalException.class, () -> provision(config));

		assertEquals(List.of("HEAD bench"), extension.requests);
		assertEquals(Category.EXECUTION, failure.category());
		assertEquals("step", failure.stepId());
		assertEquals("target bucket \"bench\" does not exist and target-bucket provisioning is disabled "
						+ "(--provision-target-bucket=false, storage.targetBucket.provision: false)", failure.getMessage());
		assertEquals(1, extension.closed);
	}

	@Test
	void readingStepNeverCreatesItsSourceBucket() throws Exception {
		extension.check("source", TargetBucketResponse.notFound());
		final var config = stepConfig("read", "%{date:format(\"yyyy\").format(date:from(time:millisSinceEpoch()))}");
		config.val("item-input-path", "/source");

		final var failure = assertThrows(RunTerminalException.class, () -> provision(config));

		assertEquals(List.of("HEAD source"), extension.requests,
						"a reading step validates its source and ignores its unused output path");
		assertEquals("target bucket \"source\" does not exist; this step uses existing objects only "
						+ "and never creates a bucket", failure.getMessage());
	}

	@Test
	void requestsRunInTheStepLogContextAndTheCallersContextIsRestored() throws Exception {
		extension.leakLogContext = true;
		ThreadContext.put(Constants.KEY_STEP_ID, "previous-step");
		ThreadContext.put(Constants.KEY_CLASS_NAME, "Caller");
		try {
			provision(stepConfig("create", "/bench"));

			assertEquals(List.of("step"), extension.stepIdsSeen, "messages must reach the step's messages.log");
			assertEquals("previous-step", ThreadContext.get(Constants.KEY_STEP_ID));
			assertEquals("Caller", ThreadContext.get(Constants.KEY_CLASS_NAME));
		} finally {
			ThreadContext.remove(Constants.KEY_STEP_ID);
			ThreadContext.remove(Constants.KEY_CLASS_NAME);
		}
	}

	@Test
	void accessFailureIsNeverTreatedAsAbsence() throws Exception {
		extension.check("bench", TargetBucketResponse.ofHttpStatus(403));

		final var failure = assertThrows(RunTerminalException.class, () -> provision(stepConfig("create", "/bench")));

		assertEquals(List.of("HEAD bench"), extension.requests);
		assertEquals("target bucket \"bench\" is not accessible (HTTP 403)", failure.getMessage());
	}

	@Test
	void lostCreateRaceContinuesWhenTheRecheckSucceeds() throws Exception {
		extension.check("bench", TargetBucketResponse.notFound(), TargetBucketResponse.ok())
						.create("bench", TargetBucketResponse.ofHttpStatus(409));

		provision(stepConfig("create", "/bench"));

		assertEquals(List.of("HEAD bench", "PUT bench", "HEAD bench"), extension.requests);
	}

	@Test
	void failedCreateReportsBothOutcomesAfterExactlyOneRecheck() throws Exception {
		extension.check("bench", TargetBucketResponse.notFound(), TargetBucketResponse.notFound())
						.create("bench", TargetBucketResponse.ofHttpStatus(409));

		final var failure = assertThrows(RunTerminalException.class, () -> provision(stepConfig("create", "/bench")));

		assertEquals(List.of("HEAD bench", "PUT bench", "HEAD bench"), extension.requests);
		assertEquals("failed to create target bucket \"bench\" (HTTP 409); it is still not accessible (not found)",
						failure.getMessage());
	}

	@Test
	void driversWithoutTheCapabilityAreNotApplicable() throws Exception {
		final var config = stepConfig("create", "/bench/${path:random(1, 1)}");
		config.val("item-output-path", "/${bad}");

		assertDoesNotThrow(() -> TargetBucketProvisioning.provision(
						"step", config, null, List.<Extension> of(new ScriptedTargetBucketExtension.WithoutCapability())));
		assertEquals("/${bad}", config.stringVal("item-output-path"), "a non-applicable step is left untouched");
		config.val("storage-driver-type", "other");
		assertDoesNotThrow(() -> provision(config));
		assertEquals(0, extension.opened);
	}

	@Test
	void perOperationExpressionInBucketSegmentFailsBeforeAnyRequest() throws Exception {
		for (final var path : List.of("/bench-${int64:random()}/x", "#{date:format(\"s\")}", "/a${x}%{y}")) {
			final var failure = assertThrows(
							RunTerminalException.class, () -> provision(stepConfig("create", path)), path);
			assertEquals(Category.CONFIGURATION, failure.category());
			assertEquals("step", failure.stepId());
			assertTrue(failure.getMessage().startsWith("the bucket segment of item.output.path must be static"));
		}
		assertEquals(0, extension.opened);
	}

	@Test
	void dynamicPrefixAfterAStaticBucketIsSupported() throws Exception {
		final var path = "/bench/${path:random(2, 3)}";
		final var config = stepConfig("create", path);

		provision(config);

		assertEquals(List.of("HEAD bench"), extension.requests);
		assertEquals(path, config.stringVal("item-output-path"));
	}

	@Test
	void initializationExpressionInBucketSegmentIsResolvedOnceAndWrittenBack() throws Exception {
		final var config = stepConfig("create", "/%{\"bench\"}-7/prefix/${path:random(1, 1)}");

		provision(config);

		assertEquals("/bench-7/prefix/${path:random(1, 1)}", config.stringVal("item-output-path"));
		assertEquals(List.of("HEAD bench-7"), extension.requests);
	}

	@Test
	void shippedDefaultOutputPathResolvesToOneBucketSharedBySlices() throws Exception {
		final var config = stepConfig("create", "/placeholder");
		final var defaults = stepConfig("read", "/placeholder");
		config.val("item-output-path",
						"%{date:format(\"yyyyMMdd.HHmmss.SSS\").format(date:from(time:millisSinceEpoch()))}");

		provision(config);

		final var resolved = config.stringVal("item-output-path");
		assertTrue(resolved.matches("/\\d{8}\\.\\d{6}\\.\\d{3}"), resolved);
		assertEquals(List.of("HEAD " + resolved.substring(1)), extension.requests);
		assertEquals("/placeholder", defaults.stringVal("item-output-path"));
	}

	@Test
	void everyDistinctBucketIsValidatedInOrderAndFailureStopsTheRest() throws Exception {
		final var items = tempDir.resolve("items.csv");
		Files.writeString(items, "/b1/k1,0,1,0/0\n/b2/k2,1,1,0/0\n/b1/k3,2,1,0/0\nrelative,3,1,0/0\n/b3/k4,4,1,0/0\n");
		final var config = stepConfig("read", "/unused");
		config.val("item-input-file", items.toString());
		extension.check("b2", TargetBucketResponse.ofHttpStatus(500));

		final var failure = assertThrows(RunTerminalException.class, () -> provision(config));

		assertEquals(List.of("HEAD b1", "HEAD b2"), extension.requests, "fail fast after the first unusable bucket");
		assertTrue(failure.getMessage().contains("\"b2\" is not accessible (HTTP 500)"));
		assertEquals(1, extension.closed);
	}

	@Test
	void integrityManifestBucketsAreValidatedOnly() throws Exception {
		final var manifest = tempDir.resolve("written.csv");
		Files.writeString(manifest, "bucket,key,size,version_id\nm1,a,1,\nm2,b,1,v1\nm1,c,1,\n");
		final var config = stepConfig("delete", "/unused");
		config.val("item-input-file", manifest.toString());
		extension.check("m2", TargetBucketResponse.notFound());

		final var failure = assertThrows(RunTerminalException.class, () -> provision(config));

		assertEquals(List.of("HEAD m1", "HEAD m2"), extension.requests);
		assertTrue(failure.getMessage().contains("\"m2\" does not exist"));
	}

	@Test
	void copyModeCreatesItsDestinationButOnlyValidatesItsSource() throws Exception {
		extension.check("source", TargetBucketResponse.notFound());
		extension.check("dest", TargetBucketResponse.notFound());
		final var config = stepConfig("create", "/dest");
		config.val("item-input-path", "/source");

		assertThrows(RunTerminalException.class, () -> provision(config));

		assertEquals(List.of("HEAD dest", "PUT dest", "HEAD source"), extension.requests);
	}

	@Test
	void contextsSharingABucketAreCheckedOnceAndCreatableWhenAnyWrites() throws Exception {
		extension.check("bench", TargetBucketResponse.notFound());
		final var config = stepConfig("read", "/bench");
		config.val("item-input-path", "/bench");
		final var readContext = new BasicConfig(config.pathSep(), config.schema(),
						Map.of("load", Map.of("op", Map.of("type", "read"))));
		final var writeContext = new BasicConfig(config.pathSep(), config.schema(),
						Map.of("load", Map.of("op", Map.of("type", "create"))));

		TargetBucketProvisioning.provision("step", config, List.of(readContext, writeContext), List.of(extension));

		assertEquals(List.of("HEAD bench", "PUT bench"), extension.requests);
		assertEquals(1, extension.opened);
	}

	@Test
	void perPathCredentialsFromTheAuthFileAreUsedPerBucket() throws Exception {
		final var authFile = tempDir.resolve("auth.csv");
		Files.writeString(authFile, "/bench/prefix,writer,secret-w\n/source,reader,secret-r\n");
		final var config = stepConfig("create", "/bench/prefix");
		config.val("item-input-path", "/source");
		config.val("storage-auth-file", authFile.toString());

		provision(config);

		assertEquals(List.of("HEAD bench as writer", "HEAD source as reader"), extension.requests);
	}

	@Test
	void unreadableCredentialsAreATerminalConfigurationFailure() throws Exception {
		final var config = stepConfig("create", "/bench");
		config.val("storage-auth-file", tempDir.resolve("missing-auth.csv").toString());

		final var failure = assertThrows(RunTerminalException.class, () -> provision(config));

		assertEquals(Category.CONFIGURATION, failure.category());
		assertEquals("failed to read storage.auth.file to check the step's target buckets", failure.getMessage());
		assertEquals("step", failure.stepId());
		assertEquals(0, extension.opened, "no bucket may be checked with an unknown identity");
	}

	@Test
	void undecodableItemsFileIsATerminalInputFailure() throws Exception {
		final var items = tempDir.resolve("items.csv");
		Files.write(items, new byte[]{'/', 'b', '/', 'k', ',', (byte) 0xC3, (byte) 0x28, '\n'
		});
		final var config = stepConfig("read", "/unused");
		config.val("item-input-file", items.toString());

		final var failure = assertThrows(RunTerminalException.class, () -> provision(config));

		assertEquals(Category.INPUT, failure.category());
		assertEquals("failed to read item.input.file to find the step's target buckets", failure.getMessage());
		assertEquals(0, extension.opened);
	}

	@Test
	void missingItemsFileNamesNoBucketAndKeepsTheStepsOwnHandling() throws Exception {
		// A generated cleanup may name a file no earlier phase wrote, such as put-remaining.csv
		// after a mixed run without PUTs; the step itself treats that as an empty input.
		final var config = stepConfig("delete", "/unused");
		config.val("item-input-file", tempDir.resolve("put-remaining.csv").toString());

		assertDoesNotThrow(() -> provision(config));
		assertEquals(0, extension.opened);
	}

	@Test
	void eachContextUsesItsEffectiveStorageConfiguration() throws Exception {
		final var config = stepConfig("create", "/placeholder");
		final var first = new BasicConfig(config.pathSep(), config.schema(), Map.of(
						"item", Map.of("output", Map.of("path", "/bucket-a")),
						"storage", Map.of("namespace", "tenant-a")));
		final var second = new BasicConfig(config.pathSep(), config.schema(), Map.of(
						"item", Map.of("output", Map.of("path", "/bucket-b")),
						"storage", Map.of("namespace", "tenant-b")));

		TargetBucketProvisioning.provision("step", config, List.of(first, second), List.of(extension));

		assertEquals(List.of("HEAD bucket-a", "HEAD bucket-b"), extension.requests);
		// Any effective storage setting separates control clients; region, TLS and headers live in
		// extension schemas, so the core schema's namespace stands in for them here.
		assertEquals(List.of("tenant-a", "tenant-b"),
						extension.openedWith.stream().map(c -> c.stringVal("namespace")).toList(),
						"each control client must use its context's effective storage configuration");
		assertEquals(2, extension.closed);
	}

	@Test
	void eachCredentialForASharedBucketIsChecked() throws Exception {
		final var config = stepConfig("create", "/bench");
		final var authA = Files.writeString(tempDir.resolve("a.csv"), "/bench,tenant-a,example-secret-a\n");
		final var authB = Files.writeString(tempDir.resolve("b.csv"), "/bench,tenant-b,example-secret-b\n");
		final var first = new BasicConfig(config.pathSep(), config.schema(), Map.of(
						"storage", Map.of("auth", Map.of("file", authA.toString()))));
		final var second = new BasicConfig(config.pathSep(), config.schema(), Map.of(
						"storage", Map.of("auth", Map.of("file", authB.toString()))));

		TargetBucketProvisioning.provision("step", config, List.of(first, second), List.of(extension));

		assertEquals(List.of("HEAD bench as tenant-a", "HEAD bench as tenant-b"), extension.requests,
						"checking the first tenant does not establish access for the second");
	}

	@Test
	void oneContextChecksASharedBucketOncePerCredential() throws Exception {
		final var auth = Files.writeString(tempDir.resolve("auth.csv"),
						"/bench/out,writer,example-secret-w\n/bench,reader,example-secret-r\n");
		final var config = stepConfig("create", "/bench/out");
		config.val("item-input-path", "/bench");
		config.val("storage-auth-file", auth.toString());

		provision(config);

		assertEquals(List.of("HEAD bench as writer", "HEAD bench as reader"), extension.requests);
	}

	@Test
	void logFingerprintIsNotA32BitHashAndCarriesNoSecret() throws Exception {
		final var config = stepConfig("create", "/bench");
		config.val("storage-auth-uid", "uid-1");
		config.val("storage-auth-secret", "secret-1");
		final var fingerprint = TargetBucketProvisioning.fingerprint(config.configVal("storage"));

		assertEquals(32, fingerprint.length());
		assertFalse(fingerprint.contains("secret-1") || fingerprint.contains("uid-1"));
		config.val("storage-auth-secret", "secret-2");
		assertEquals(fingerprint, TargetBucketProvisioning.fingerprint(config.configVal("storage")),
						"the secret plays no part in the fingerprint");
	}

	@Test
	void interruptionRestoresTheFlagAndClosesTheControlClient() throws Exception {
		extension.interruptOnCheck = true;
		try {
			final var failure = assertThrows(Exception.class, () -> provision(stepConfig("create", "/bench")));
			assertInstanceOf(InterruptedException.class, failure);
			assertTrue(Thread.currentThread().isInterrupted());
			assertEquals(1, extension.closed);
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	void runScopeReportsEachTargetOnceAndClearsOnClose() {
		try (final var scope = RunScope.open()) {
			assertSame(scope, RunScope.current());
			assertTrue(scope.firstReport("s3|bench"));
			assertFalse(scope.firstReport("s3|bench"));
			try (final var nested = RunScope.open()) {
				assertTrue(nested.firstReport("s3|bench"), "a nested run has its own scope");
			}
			assertSame(scope, RunScope.current());
		}
		assertNull(RunScope.current());
	}
}
