package com.dell.spt.base.storage.driver.bucket;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.integrity.IntegrityTerminalException.Category;
import com.dell.spt.base.item.DataItemImpl;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Setup of a listed target bucket set: storage.targetBucket.names. */
class TargetBucketListProvisioningTest {

	private static final String[] NAMES = {"ps-eu", "PS_US", "ps-ap"
	};

	@TempDir
	Path tempDir;

	private final ScriptedTargetBucketExtension extension = new ScriptedTargetBucketExtension();

	private static Config stepConfig(final String opType, final String outputPath, final String... names)
					throws Exception {
		final var config = TargetBucketProvisioningTest.stepConfig(opType, outputPath);
		config.val(TargetBucketSet.NAMES, List.of(names));
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
	void allMissingListedBucketsAreCreatedInListOrderAndThePlaceholderIsNeverContacted() throws Exception {
		for (final var name : NAMES) {
			extension.check(name, TargetBucketResponse.notFound());
		}

		provision(stepConfig("create", "/placeholder/prefix", NAMES));

		assertEquals(List.of(
						"HEAD ps-eu", "PUT ps-eu", "HEAD PS_US", "PUT PS_US", "HEAD ps-ap", "PUT ps-ap"),
						extension.requests);
		assertEquals(1, extension.opened);
		assertEquals(1, extension.closed);
	}

	@Test
	void onlyTheMissingListedBucketsAreCreated() throws Exception {
		extension.check("PS_US", TargetBucketResponse.notFound());

		provision(stepConfig("create", "/placeholder", NAMES));

		assertEquals(List.of("HEAD ps-eu", "HEAD PS_US", "PUT PS_US", "HEAD ps-ap"), extension.requests);
	}

	@Test
	void whenAllListedBucketsExistNothingIsCreated() throws Exception {
		provision(stepConfig("create", "/placeholder", NAMES));

		assertEquals(List.of("HEAD ps-eu", "HEAD PS_US", "HEAD ps-ap"), extension.requests);
	}

	@Test
	void disabledProvisioningChecksTheListedBucketsAndNeverCreatesOne() throws Exception {
		extension.check("PS_US", TargetBucketResponse.notFound());
		final var config = stepConfig("create", "/placeholder", NAMES);
		config.val(TargetBucketProvisioning.POLICY, false);

		final var failure = assertThrows(RunTerminalException.class, () -> provision(config));

		assertEquals(List.of("HEAD ps-eu", "HEAD PS_US"), extension.requests);
		assertEquals(Category.EXECUTION, failure.category());
		assertTrue(failure.getMessage().contains("\"PS_US\" does not exist and target-bucket provisioning is disabled"),
						failure.getMessage());
	}

	@Test
	void disabledProvisioningWithEveryListedBucketPresentChecksThemAndSucceeds() throws Exception {
		final var config = stepConfig("create", "/placeholder", NAMES);
		config.val(TargetBucketProvisioning.POLICY, false);

		provision(config);

		assertEquals(List.of("HEAD ps-eu", "HEAD PS_US", "HEAD ps-ap"), extension.requests);
	}

	@Test
	void aDeniedListedBucketStopsSetupAndKeepsTheOnesCreatedBeforeIt() throws Exception {
		extension.check("ps-eu", TargetBucketResponse.notFound());
		extension.check("PS_US", TargetBucketResponse.ofHttpStatus(403));

		final var failure = assertThrows(RunTerminalException.class,
						() -> provision(stepConfig("create", "/placeholder", NAMES)));

		assertEquals(List.of("HEAD ps-eu", "PUT ps-eu", "HEAD PS_US"), extension.requests);
		assertTrue(failure.getMessage().contains("\"PS_US\" is not accessible (HTTP 403)"), failure.getMessage());
	}

	@Test
	void aLaterPhaseOnlyChecksTheListedBucketsAnEarlierPhaseCreated() throws Exception {
		extension.check("ps-eu", TargetBucketResponse.notFound());
		provision(stepConfig("create", "/placeholder", "ps-eu", "ps-us"));
		final var items = itemsFile("items.csv", "/ps-eu/a", "/ps-us/b");
		extension.requests.clear();
		extension.check("ps-us", TargetBucketResponse.notFound());
		final var read = stepConfig("read", "/placeholder", "ps-eu", "ps-us");
		read.val("item-input-file", items.toString());

		final var failure = assertThrows(RunTerminalException.class, () -> provision(read));

		assertEquals(List.of("HEAD ps-eu", "HEAD ps-us"), extension.requests);
		assertTrue(failure.getMessage().contains("\"ps-us\" does not exist; this step uses existing objects only"),
						failure.getMessage());
	}

	@Test
	void aReadingStepChecksTheBucketsItsInputsNameNotTheList() throws Exception {
		final var items = itemsFile("items.csv", "/ps-ap/a", "/ps-eu/b", "/ps-ap/c");
		final var config = stepConfig("read", "/placeholder", NAMES);
		config.val("item-input-file", items.toString());

		provision(config);

		assertEquals(List.of("HEAD ps-ap", "HEAD ps-eu"), extension.requests);
	}

	@Test
	void theOutputPathPlaceholderIsNeitherValidatedNorEvaluatedForAList() throws Exception {
		provision(stepConfig("create", "/%{\"a/b\"}-x/prefix", "ps-eu", "ps-us"));

		assertEquals(List.of("HEAD ps-eu", "HEAD ps-us"), extension.requests);
	}

	@Test
	void anEmptyOutputPathWithAListStillCreatesTheListedBuckets() throws Exception {
		final var config = stepConfig("create", "", "ps-eu", "ps-us");
		config.val("item-output-path", "");

		provision(config);

		assertEquals(List.of("HEAD ps-eu", "HEAD ps-us"), extension.requests);
	}

	@Test
	void aCountConflictFailsBeforeAnyRequestAndAnInvalidListFailsEvenWithoutBucketControl() throws Exception {
		final var conflict = stepConfig("create", "/placeholder", "ps-eu", "ps-us");
		conflict.val(TargetBucketSet.COUNT, 4);

		final var failure = assertThrows(RunTerminalException.class, () -> provision(conflict));

		assertEquals(Category.CONFIGURATION, failure.category());
		assertTrue(failure.getMessage().contains("cannot be combined with storage.targetBucket.count 4"),
						failure.getMessage());
		assertTrue(extension.requests.isEmpty(), extension.requests.toString());

		final var invalid = stepConfig("create", "/placeholder", "ps-eu", "bad name");
		final var withoutControl = assertThrows(RunTerminalException.class,
						() -> TargetBucketProvisioning.provision("step", invalid, null, List.of()));

		assertEquals(Category.CONFIGURATION, withoutControl.category());
		assertTrue(withoutControl.getMessage().contains("storage.targetBucket.names[1] \"bad name\""),
						withoutControl.getMessage());
		assertTrue(extension.requests.isEmpty(), extension.requests.toString());
	}

	@Test
	void everyMultiBucketRejectionAppliesToAListOfTwoOrMoreNames() throws Exception {
		final var authFile = Files.writeString(tempDir.resolve("auth.csv"), "/ps-eu,u,s\n");
		final var items = itemsFile("source.csv", "/source/a");

		assertRejected(stepConfig("create", "/placeholder", NAMES), List.of(),
						"requires the s3, s3-aws or s3-rdma storage driver");
		final var withAuthFile = stepConfig("create", "/placeholder", NAMES);
		withAuthFile.val("storage-auth-file", authFile.toString());
		assertRejected(withAuthFile, List.of(extension), "cannot be combined with storage.auth.file");
		final var list = stepConfig("list", "/placeholder", NAMES);
		list.val("item-type", "path");
		assertRejected(list, List.of(extension), "cannot be combined with LIST");
		final var copy = stepConfig("create", "/placeholder", NAMES);
		copy.val("item-input-path", "/source");
		assertRejected(copy, List.of(extension), "item.input.path in a CREATE step");
		final var recreate = stepConfig("create", "/placeholder", NAMES);
		recreate.val("item-input-file", items.toString());
		assertRejected(recreate, List.of(extension), "item.input.file in a CREATE step");
		for (final var path : List.of("/placeholder/${int64:random()}", "/placeholder/a/#{x}", "/placeholder/%{\"x\"}")) {
			assertRejected(stepConfig("create", path, NAMES), List.of(extension), "static item.output.path after the bucket");
		}
		assertTrue(extension.requests.isEmpty(), extension.requests.toString());
	}

	@Test
	void theRejectionNamesTheListAndNotTheCount() throws Exception {
		final var copy = stepConfig("create", "/placeholder", NAMES);
		copy.val("item-input-file", itemsFile("source.csv", "/source/a").toString());

		final var failure = assertThrows(RunTerminalException.class,
						() -> TargetBucketProvisioning.provision("step", copy, null, List.<Extension> of(extension)));

		assertTrue(failure.getMessage().contains("storage.targetBucket.names (3 names)"), failure.getMessage());
		assertTrue(failure.getMessage().contains("the bucket list applies only to newly generated objects"),
						failure.getMessage());
		assertFalse(failure.getMessage().contains("storage.targetBucket.count"), failure.getMessage());
	}

	@Test
	void aPipelineCreateFedByThePreviousSubStepIsRejectedForAList() throws Exception {
		final var config = stepConfig("create", "/placeholder", NAMES);
		final var create = new BasicConfig(config.pathSep(), config.schema(),
						Map.of("load", Map.of("op", Map.of("type", "create"))));

		final var failure = assertThrows(RunTerminalException.class, () -> TargetBucketProvisioning.provision(
						"step", config, List.of(create, create), List.<Extension> of(extension), TargetBucketStep.PIPELINE));

		assertEquals(Category.CONFIGURATION, failure.category());
		assertTrue(failure.getMessage().contains("pipeline CREATE sub-step after the first"), failure.getMessage());
		assertTrue(extension.requests.isEmpty(), extension.requests.toString());
	}

	@Test
	void aOneNameListIsASingleBucketWithNoneOfTheMultiBucketRejections() throws Exception {
		final var authFile = Files.writeString(tempDir.resolve("auth.csv"), "/ps-eu,writer,secret-w\n/source,reader,secret-r\n");
		final var copy = stepConfig("create", "/placeholder/${int64:random()}", "ps-eu");
		copy.val("item-input-path", "/source");
		copy.val("storage-auth-file", authFile.toString());

		provision(copy);

		assertEquals(List.of("HEAD ps-eu as writer", "HEAD source as reader"), extension.requests,
						"item.input.path, an expression after the bucket and storage.auth.file are all accepted");
	}

	@Test
	void aOneNameListLooksUpItsCredentialByTheListedNameNotThePlaceholder() throws Exception {
		final var authFile = Files.writeString(tempDir.resolve("auth.csv"),
						"/placeholder,wrong,secret-x\n/ps-eu/prefix,writer,secret-w\n");

		final var config = stepConfig("create", "/placeholder/prefix", "ps-eu");
		config.val("storage-auth-file", authFile.toString());

		provision(config);

		assertEquals(List.of("HEAD ps-eu as writer"), extension.requests);
	}

	@Test
	void aOneNameListIsRejectedWithAnAuthFileOnlyOnceASecondNameIsAdded() throws Exception {
		final var authFile = Files.writeString(tempDir.resolve("auth.csv"), "/ps-eu,writer,secret-w\n");
		final var one = stepConfig("create", "/placeholder", "ps-eu");
		one.val("storage-auth-file", authFile.toString());
		provision(one);
		extension.requests.clear();

		final var two = stepConfig("create", "/placeholder", "ps-eu", "ps-us");
		two.val("storage-auth-file", authFile.toString());

		assertRejected(two, List.of(extension), "cannot be combined with storage.auth.file");
		assertTrue(extension.requests.isEmpty(), extension.requests.toString());
	}

	@Test
	void aOneNameListWithAPlaceholderEqualToTheNameIsTodaysSingleBucket() throws Exception {
		provision(stepConfig("create", "/ps-eu/prefix", "ps-eu"));
		final var listed = List.copyOf(extension.requests);
		extension.requests.clear();

		provision(TargetBucketProvisioningTest.stepConfig("create", "/ps-eu/prefix"));

		assertEquals(extension.requests, listed);
	}

	@Test
	void aOneNameListChecksTheListedNameNotThePlaceholder() throws Exception {
		extension.check("ps-eu", TargetBucketResponse.notFound());

		provision(stepConfig("create", "/placeholder/prefix", "ps-eu"));

		assertEquals(List.of("HEAD ps-eu", "PUT ps-eu"), extension.requests);
	}

	@Test
	void aOneNameListUsesAnAwsInvalidNameAsWritten() throws Exception {
		provision(stepConfig("create", "/placeholder", "UPPER_case"));

		assertEquals(List.of("HEAD UPPER_case"), extension.requests,
						"a listed name is used as written, so the AWS naming rules do not apply");
	}

	@Test
	void anEmptyListFallsBackToTheGeneratedCountExactlyAsBefore() throws Exception {
		final var config = TargetBucketProvisioningTest.stepConfig("create", "/bench");
		config.val(TargetBucketSet.COUNT, 2);
		config.val(TargetBucketSet.NAMES, List.of());

		provision(config);

		assertEquals(List.of("HEAD bench-0", "HEAD bench-1"), extension.requests);
	}

	private static void assertRejected(final Config config, final List<Extension> extensions, final String reason) {
		final var failure = assertThrows(RunTerminalException.class,
						() -> TargetBucketProvisioning.provision("step", config, null, extensions));
		assertEquals(Category.CONFIGURATION, failure.category(), failure.getMessage());
		assertTrue(failure.getMessage().contains(reason), failure.getMessage());
	}
}
