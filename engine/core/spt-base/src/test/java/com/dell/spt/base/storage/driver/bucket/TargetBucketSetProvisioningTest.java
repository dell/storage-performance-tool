package com.dell.spt.base.storage.driver.bucket;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.integrity.IntegrityTerminalException.Category;
import com.dell.spt.base.item.DataItemImpl;
import com.github.akurilov.confuse.Config;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Setup of a target bucket set: storage.targetBucket.count greater than one. */
class TargetBucketSetProvisioningTest {

	@TempDir
	Path tempDir;

	private final ScriptedTargetBucketExtension extension = new ScriptedTargetBucketExtension();

	private static Config stepConfig(final String opType, final String outputPath, final int count) throws Exception {
		final var config = TargetBucketProvisioningTest.stepConfig(opType, outputPath);
		config.val(TargetBucketSet.COUNT, count);
		return config;
	}

	private void provision(final Config config) {
		TargetBucketProvisioning.provision("step", config, null, List.<Extension> of(extension));
	}

	private Path itemsFile(final String name, final String... itemNames) throws Exception {
		final var file = tempDir.resolve(name);
		final var lines = new StringBuilder();
		for (final var itemName : itemNames) {
			lines.append(new DataItemImpl(itemName, 0, 1024)).append('\n');
		}
		Files.writeString(file, lines);
		return file;
	}

	@Test
	void allMissingMembersOfAWritingStepAreCreatedAndTheBaseIsNeverContacted() throws Exception {
		for (int i = 0; i < 4; i++) {
			extension.check("bench-" + i, TargetBucketResponse.notFound());
		}

		provision(stepConfig("create", "/bench/prefix", 4));

		assertEquals(List.of(
						"HEAD bench-0", "PUT bench-0", "HEAD bench-1", "PUT bench-1",
						"HEAD bench-2", "PUT bench-2", "HEAD bench-3", "PUT bench-3"), extension.requests);
		assertEquals(1, extension.opened);
		assertEquals(1, extension.closed);
	}

	@Test
	void onlyTheMissingMembersAreCreated() throws Exception {
		extension.check("bench-1", TargetBucketResponse.notFound());
		extension.check("bench-3", TargetBucketResponse.notFound());

		provision(stepConfig("create", "/bench", 4));

		assertEquals(List.of(
						"HEAD bench-0", "HEAD bench-1", "PUT bench-1", "HEAD bench-2", "HEAD bench-3", "PUT bench-3"),
						extension.requests);
	}

	@Test
	void disabledProvisioningChecksTheMembersAndNeverCreatesOne() throws Exception {
		extension.check("bench-1", TargetBucketResponse.notFound());
		final var config = stepConfig("create", "/bench", 4);
		config.val(TargetBucketProvisioning.POLICY, false);

		final var failure = assertThrows(RunTerminalException.class, () -> provision(config));

		assertEquals(List.of("HEAD bench-0", "HEAD bench-1"), extension.requests);
		assertEquals(Category.EXECUTION, failure.category());
		assertTrue(failure.getMessage().contains("\"bench-1\" does not exist and target-bucket provisioning is disabled"),
						failure.getMessage());
		assertEquals(1, extension.closed);
	}

	@Test
	void aDeniedMemberStopsSetupAndKeepsTheMembersCreatedBeforeIt() throws Exception {
		extension.check("bench-0", TargetBucketResponse.notFound());
		extension.check("bench-1", TargetBucketResponse.ofHttpStatus(403));

		final var failure = assertThrows(RunTerminalException.class, () -> provision(stepConfig("create", "/bench", 3)));

		assertEquals(List.of("HEAD bench-0", "PUT bench-0", "HEAD bench-1"), extension.requests);
		assertTrue(failure.getMessage().contains("\"bench-1\" is not accessible (HTTP 403)"), failure.getMessage());
	}

	@Test
	void aLaterPhaseChecksTheMembersAnEarlierPhaseCreated() throws Exception {
		extension.check("bench-0", TargetBucketResponse.notFound());
		extension.check("bench-1", TargetBucketResponse.notFound());
		provision(stepConfig("create", "/bench", 2));
		final var items = itemsFile("items.csv", "/bench-0/a", "/bench-1/b", "/bench-0/c");
		extension.requests.clear();
		extension.check("bench-1", TargetBucketResponse.notFound());
		final var read = stepConfig("read", "/ignored", 2);
		read.val("item-input-file", items.toString());

		final var failure = assertThrows(RunTerminalException.class, () -> provision(read));

		assertEquals(List.of("HEAD bench-0", "HEAD bench-1"), extension.requests);
		assertTrue(failure.getMessage().contains("\"bench-1\" does not exist; this step uses existing objects only"),
						failure.getMessage());
	}

	@Test
	void aReadingStepChecksTheBucketsItsInputsNameAsWritten() throws Exception {
		final var items = itemsFile("items.csv", "/bench-2/a", "/bench-0/b", "/bench-2/c");
		final var config = stepConfig("read", "/bench", 4);
		config.val("item-input-file", items.toString());
		config.val("item-input-path", "/source");

		provision(config);

		assertEquals(List.of("HEAD source", "HEAD bench-2", "HEAD bench-0"), extension.requests);
	}

	@Test
	void aWritingStepWithoutInputsNamingTheBaseNeverContactsIt() throws Exception {
		provision(stepConfig("create", "/bench", 2));

		assertFalse(extension.requests.contains("HEAD bench"), extension.requests.toString());
	}

	@Test
	void anInitializationExpressionInTheBucketSegmentIsResolvedOnceBeforeTheSuffix() throws Exception {
		final var config = stepConfig("create", "/%{\"bench\"}-7/prefix", 2);

		provision(config);

		assertEquals("/bench-7/prefix", config.stringVal("item-output-path"));
		assertEquals(List.of("HEAD bench-7-0", "HEAD bench-7-1"), extension.requests);
	}

	@Test
	void anInterruptedMemberCheckStopsSetup() throws Exception {
		extension.interruptOnCheck = true;
		try {
			final var failure = assertThrows(Exception.class, () -> provision(stepConfig("create", "/bench", 4)));

			assertInstanceOf(InterruptedException.class, failure);
			assertEquals(List.of("HEAD bench-0"), extension.requests);
			assertTrue(Thread.currentThread().isInterrupted(), "the interrupt flag is restored");
			assertEquals(1, extension.closed);
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	void anInvalidMemberNameFailsBeforeAnyRequest() throws Exception {
		final var failure = assertThrows(RunTerminalException.class, () -> provision(stepConfig("create", "/xn--b", 2)));

		assertEquals(Category.CONFIGURATION, failure.category());
		assertTrue(extension.requests.isEmpty(), extension.requests.toString());
	}

	@Test
	void anOutOfRangeCountFailsEvenForADriverWithoutBucketControl() throws Exception {
		final var config = stepConfig("create", "/bench", 0);

		final var failure = assertThrows(RunTerminalException.class,
						() -> TargetBucketProvisioning.provision("step", config, null, List.of()));

		assertEquals(Category.CONFIGURATION, failure.category());
		assertTrue(failure.getMessage().contains("between 1 and 1000, not 0"), failure.getMessage());
	}

	@Test
	void unsupportedCombinationsFailBeforeAnyRequest() throws Exception {
		final var authFile = Files.writeString(tempDir.resolve("auth.csv"), "/bench-0,u,s\n");
		final var items = itemsFile("source.csv", "/source/a");

		assertRejected(stepConfig("create", "/bench", 2), List.of(), "requires the s3, s3-aws or s3-rdma storage driver");
		final var withAuthFile = stepConfig("create", "/bench", 2);
		withAuthFile.val("storage-auth-file", authFile.toString());
		assertRejected(withAuthFile, List.of(extension), "cannot be combined with storage.auth.file");
		final var list = stepConfig("list", "/bench", 2);
		list.val("item-type", "path");
		assertRejected(list, List.of(extension), "cannot be combined with LIST");
		final var copy = stepConfig("create", "/bench", 2);
		copy.val("item-input-path", "/source");
		assertRejected(copy, List.of(extension), "item.input.path in a CREATE step");
		final var recreate = stepConfig("create", "/bench", 2);
		recreate.val("item-input-file", items.toString());
		assertRejected(recreate, List.of(extension), "item.input.file in a CREATE step");
		for (final var path : List.of("/bench/${int64:random()}", "/bench/a/#{x}", "/bench/%{\"x\"}")) {
			assertRejected(stepConfig("create", path, 2), List.of(extension), "static item.output.path after the bucket");
		}
		assertTrue(extension.requests.isEmpty(), extension.requests.toString());
	}

	@Test
	void oneBucketKeepsTodaysRulesForTheSameCombinations() throws Exception {
		final var copy = stepConfig("create", "/bench/${int64:random()}", 1);
		copy.val("item-input-path", "/source");

		provision(copy);

		assertEquals(List.of("HEAD bench", "HEAD source"), extension.requests);
	}

	private static void assertRejected(final Config config, final List<Extension> extensions, final String reason) {
		final var failure = assertThrows(RunTerminalException.class,
						() -> TargetBucketProvisioning.provision("step", config, null, extensions));
		assertEquals(Category.CONFIGURATION, failure.category(), failure.getMessage());
		assertTrue(failure.getMessage().contains(reason), failure.getMessage());
	}
}
