package com.dell.spt.base.load.step.client;

import static com.dell.spt.base.Constants.APP_NAME;
import static com.dell.spt.base.Constants.PATH_DEFAULTS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.Main;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.logging.Loggers;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A copy step whose constant output path overlaps the storage path it lists: refused at start when
 * every copy would land on its own source, warned about when only keys already under the output path
 * would, and left alone otherwise.
 */
class CopyIntoItsSourceTest {

	private static final String STEP = "copy-step";
	private static final String WARNING = "lies under the item input path";

	private CapturingAppender appender;
	private org.apache.logging.log4j.core.Logger logger;

	@BeforeEach
	void attachAppender() throws InterruptedException {
		appender = new CapturingAppender();
		appender.start();
		logger = LoggerContext.getContext(false).getLogger(Loggers.MSG.getName());
		logger.addAppender(appender);
		// Engine loggers are asynchronous: count only what follows a marker logged after attaching.
		Loggers.MSG.warn(appender.startMarker);
		awaitMarker(() -> appender.armed);
	}

	@AfterEach
	void detachAppender() {
		logger.removeAppender(appender);
		appender.stop();
	}

	// ---- s3 and the other drivers: refuse equal paths, warn when the output is under the input ----

	@Test
	void equalPathsAreRefusedNamingBothPaths() {
		final var failure = refused("s3", "/src", "/src");

		assertEquals(
						"The copy output path \"/src\" is the item input path \"/src\": every object would be copied onto "
										+ "itself. Choose a different output path",
						failure.getMessage());
		assertEquals(IntegrityTerminalException.Category.CONFIGURATION, failure.category());
		assertEquals(STEP, failure.stepId());
	}

	@Test
	void trailingAndMissingLeadingSlashesCompareEqual() {
		for (final var pair : List.of(
						List.of("/src", "/src/"), List.of("/src/", "/src"), List.of("/src", "src"),
						List.of("src", "/src"), List.of("src/", "src"), List.of("/src/dir", "src/dir/"))) {
			final var failure = refused("s3", pair.get(0), pair.get(1));

			assertTrue(failure.getMessage().contains("output path \"" + pair.get(1) + "\""), failure.getMessage());
			assertTrue(failure.getMessage().contains("input path \"" + pair.get(0) + "\""), failure.getMessage());
		}
	}

	@Test
	void theRefusalAppliesToEveryNonFsDriver() {
		for (final var driverType : List.of("s3-aws", "s3-rdma", "swift", "atmos")) {
			refused(driverType, "/src", "/src");
		}
	}

	@Test
	void anOutputPathUnderTheInputPathWarnsOnceAndPasses() throws Exception {
		check("s3", "/src", "/src/dir");

		final var warnings = warningsSoFar();
		assertEquals(1, warnings.size(), warnings::toString);
		assertEquals(
						"WARN copy-step: the copy output path \"/src/dir\" lies under the item input path \"/src\": the "
										+ "listing also returns keys already under the output path, such as an earlier run's "
										+ "copies, and those are copied onto themselves",
						warnings.get(0));
	}

	@Test
	void anOutputPathUnderTheInputPathWarnsWhateverItsSlashes() throws Exception {
		check("s3-aws", "src/", "/src/dir/");

		assertEquals(1, warningsSoFar().size());
	}

	@Test
	void aSiblingWithASharedStringPrefixIsNeitherRefusedNorWarned() throws Exception {
		check("s3", "/pp", "/ppx");
		check("s3", "/ppx", "/pp");
		check("s3", "/pp", "/ppx/sub");

		assertEquals(List.of(), warningsSoFar());
	}

	@Test
	void aDifferentBucketOrAnAncestorIsNeitherRefusedNorWarned() throws Exception {
		check("s3", "/pp", "/dst");
		check("s3", "/src/a", "/dst/a");
		// the reverse-prefix layout is not a listing on s3; fs refuses it below
		check("s3", "/src/dir", "/src");

		assertEquals(List.of(), warningsSoFar());
	}

	// ---- fs: refuse an output path that is the input path or one of its parents, as written ----

	@Test
	void fsRefusesAnOutputPathEqualToTheInputPath() {
		final var failure = refused("fs", "/D/src", "/D/src");

		assertEquals(
						"The copy output path \"/D/src\" is the item input path \"/D/src\" or one of its parent directories: "
										+ "each copy would write over its own source file and empty it. Choose an output path "
										+ "outside the input path",
						failure.getMessage());
		assertEquals(IntegrityTerminalException.Category.CONFIGURATION, failure.category());
	}

	@Test
	void fsRefusesAnOutputPathAboveTheInputPathNamingBothPaths() {
		for (final var output : List.of("/D", "/D/", "/")) {
			final var failure = refused("fs", "/D/src", output);

			assertTrue(failure.getMessage().contains("output path \"" + output + "\""), failure.getMessage());
			assertTrue(failure.getMessage().contains("input path \"/D/src\""), failure.getMessage());
		}
	}

	@Test
	void fsRefusesTrailingSlashes() {
		refused("fs", "/D/src/", "/D/");
		refused("fs", "/D/src/", "/D/src");
		refused("fs", "/D/src", "/D/src/");
	}

	@Test
	void fsComparesThePathsAsWritten() throws Exception {
		// a relative output path resolves against the working directory, and dot segments name other
		// directories: each copies correctly, so none is refused
		check("fs", "/D/src", "D");
		check("fs", "/D/src", "D/src");
		check("fs", "/D/src", "/D/src/..");
		check("fs", "/D/src", "/D/./src");
		// a relative input path lists absolute names, which a textual compare says nothing about
		check("fs", "D/src", "D");

		assertEquals(List.of(), warningsSoFar());
	}

	@Test
	void fsNeitherRefusesNorWarnsASiblingOrAChildOutputPath() throws Exception {
		check("fs", "/pp", "/ppx");
		check("fs", "/D/src", "/D/dst");
		// an fs listing is one directory deep, so an earlier run's copies under the input are never listed
		check("fs", "/D/src", "/D/src/sub");

		assertEquals(List.of(), warningsSoFar());
	}

	// ---- what cannot be checked at start ----

	@Test
	void anItemsFileInputIsNotChecked() throws Exception {
		for (final var driverType : List.of("s3", "fs")) {
			for (final var output : List.of("/D", "/D/src", "/D/src/sub")) {
				final var config = config(driverType, "/D/src", output);
				config.val("item-input-file", "/var/tmp/items.csv");
				CopyIntoItsSource.check(STEP, config);
			}
		}

		assertEquals(List.of(), warningsSoFar());
	}

	@Test
	void anOutputPathExpressionIsNotChecked() throws Exception {
		check("s3", "/src", "/src${''}");
		check("s3", "/src", "/src/#{''}");
		check("fs", "/D/src", "/D%{''}");

		assertEquals(List.of(), warningsSoFar());
	}

	@Test
	void aTargetBucketSetOfSeveralBucketsIsNotChecked() throws Exception {
		final var count = config("s3", "/src", "/src");
		count.val("storage-targetBucket-count", 2);
		CopyIntoItsSource.check(STEP, count);
		final var names = config("s3", "/src", "/src");
		names.val("storage-targetBucket-names", List.of("src", "other"));
		CopyIntoItsSource.check(STEP, names);

		assertEquals(List.of(), warningsSoFar());
	}

	@Test
	void aOneNameBucketListIsCheckedAtThatName() {
		final var config = config("s3", "/src", "/placeholder");
		config.val("storage-targetBucket-names", List.of("src"));

		final var failure = assertThrows(RunTerminalException.class, () -> CopyIntoItsSource.check(STEP, config));
		assertTrue(failure.getMessage().contains("output path \"/src\""), failure.getMessage());
	}

	@Test
	void stepsOtherThanADataCreateAreNotChecked() throws Exception {
		for (final var opType : List.of("read", "update", "delete", "noop", "list")) {
			for (final var config : List.of(config("s3", "/src", "/src"), config("fs", "/D/src", "/D"))) {
				config.val("load-op-type", opType);
				CopyIntoItsSource.check(STEP, config);
			}
		}
		final var paths = config("fs", "/D/src", "/D");
		paths.val("item-type", "path");
		CopyIntoItsSource.check(STEP, paths);

		assertEquals(List.of(), warningsSoFar());
	}

	@Test
	void aCreateWithoutAnInputPathIsNotChecked() throws Exception {
		check("s3", null, "/src");
		check("s3", "", "/src");
		check("fs", null, "/D");

		assertEquals(List.of(), warningsSoFar());
	}

	// ---- helpers ----

	private static RunTerminalException refused(final String driverType, final String input, final String output) {
		return assertThrows(RunTerminalException.class,
						() -> CopyIntoItsSource.check(STEP, config(driverType, input, output)),
						driverType + " " + input + " -> " + output);
	}

	private static void check(final String driverType, final String input, final String output) {
		CopyIntoItsSource.check(STEP, config(driverType, input, output));
	}

	/** The shipped schema and defaults with a copy step's paths, as the step client receives them. */
	private static Config config(final String driverType, final String inputPath, final String outputPath) {
		try {
			final var schema = SchemaProvider.resolve(APP_NAME, Thread.currentThread().getContextClassLoader())
							.stream()
							.findFirst()
							.orElseThrow();
			final var defaultsFile = Paths.get(Main.class.getResource("/" + PATH_DEFAULTS).toURI()).toFile();
			final var config = ConfigUtil.loadConfig(defaultsFile, schema);
			config.val("storage-driver-type", driverType);
			config.val("load-op-type", "create");
			config.val("item-input-path", inputPath);
			config.val("item-output-path", outputPath);
			return config;
		} catch (final Exception e) {
			throw new AssertionError(e);
		}
	}

	/** Returns the copy warnings logged so far, once a marker logged after them has arrived. */
	private List<String> warningsSoFar() throws InterruptedException {
		Loggers.MSG.warn(appender.endMarker);
		awaitMarker(() -> appender.ended);
		synchronized (appender.warnings) {
			return List.copyOf(appender.warnings);
		}
	}

	private static void awaitMarker(final BooleanSupplier arrived) throws InterruptedException {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (!arrived.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		assertTrue(arrived.getAsBoolean(), "the marker was not logged");
	}

	private static final class CapturingAppender extends AbstractAppender {
		final List<String> warnings = Collections.synchronizedList(new ArrayList<>());
		final String startMarker = "CopyIntoItsSourceTest start " + System.nanoTime();
		final String endMarker = "CopyIntoItsSourceTest end " + System.nanoTime();
		volatile boolean armed;
		volatile boolean ended;

		CapturingAppender() {
			super("testCopyIntoItsSource", null, null, true, Property.EMPTY_ARRAY);
		}

		@Override
		public void append(final LogEvent event) {
			final String message = event.getMessage().getFormattedMessage();
			if (message.equals(startMarker)) {
				armed = true;
			} else if (message.equals(endMarker)) {
				ended = true;
			} else if (armed && !ended && message.contains(WARNING)) {
				// the level leads the message, so an assertion on the text also checks the level
				warnings.add(event.getLevel() + " " + message);
			}
		}
	}
}
