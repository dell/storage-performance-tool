package com.dell.spt.base;

import static com.dell.spt.base.Constants.APP_NAME;
import static com.dell.spt.base.Constants.PATH_DEFAULTS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.config.CliArgumentException;
import com.dell.spt.base.config.ConfigUtil;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The engine's command-line arguments, applied to the shipped schema and defaults: an argument that
 * cannot be applied raises {@link CliArgumentException}, which {@code Main} maps to exit status 2
 * before it starts a node or a scenario. The configuration may hold the arguments applied before the
 * bad one; the engine stops without using it, so the tests do not check that nothing was applied.
 */
class MainArgumentErrorTest {

	/** The shipped base schema with the shipped defaults, as {@code Main} loads them before the arguments. */
	private static Config shippedDefaults() throws Exception {
		final var schema = SchemaProvider.resolve(APP_NAME, Thread.currentThread().getContextClassLoader())
						.stream()
						.findFirst()
						.orElseThrow();
		final var defaultsFile = Paths.get(Main.class.getResource("/" + PATH_DEFAULTS).toURI()).toFile();
		return ConfigUtil.loadConfig(defaultsFile, schema);
	}

	private static Config apply(final String... args) throws Exception {
		return Main.applyArgsToConfig(args, shippedDefaults(), "initial-step");
	}

	private static String argumentError(final String... args) {
		return assertThrows(CliArgumentException.class, () -> apply(args)).getMessage();
	}

	@Test
	void anUnknownOptionIsAnArgumentError() {
		assertEquals("Invalid configuration option: \"bogus\"; --help lists the options", argumentError("--bogus"));
	}

	@Test
	void dryRunBesideTargetArgumentsIsAnArgumentError() {
		assertEquals(
						"Invalid configuration option: \"dry-run\"; --help lists the options",
						argumentError(
										"--storage-driver-type=s3",
										"--storage-auth-uid=uid",
										"--item-output-path=/bucket",
										"--load-op-limit-count=500",
										"--dry-run"));
	}

	@Test
	void anArgumentWithoutThePrefixIsAnArgumentErrorThatHidesItsValue() {
		assertEquals(
						"Invalid argument: \"load-op-limit-count\": an argument takes the form --<name>=<value>; --help lists the options",
						argumentError("load-op-limit-count=10"));
		final var message = argumentError("--storage-driver-type=s3", "storage-auth-secret=s3cr3t-value");
		assertTrue(message.contains("\"storage-auth-secret\""), message);
		assertFalse(message.contains("s3cr3t-value"), message);
	}

	@Test
	void aValueGivenAfterASpaceIsAnArgumentErrorThatHidesIt() {
		assertEquals(
						"Invalid argument after \"--storage-auth-secret\", not shown in case it is a value: an argument takes"
										+ " the form --<name>=<value>; --help lists the options",
						argumentError("--storage-driver-type=s3", "--storage-auth-secret", "s3cr3t-value"));
		final var afterAValue = argumentError("--storage-auth-uid=uid-value", "s3cr3t-value");
		assertTrue(afterAValue.startsWith("Invalid argument after \"--storage-auth-uid\", not shown"), afterAValue);
		assertFalse(afterAValue.contains("uid-value"), afterAValue);
		final var first = argumentError("s3cr3t-value", "--storage-driver-type=s3");
		assertTrue(first.startsWith("Invalid first argument, not shown in case it is a value: "), first);
		assertFalse(first.contains("s3cr3t-value"), first);
	}

	@Test
	void aValueWithAnEqualsSignGivenAfterASpaceIsHiddenWhole() {
		// A base64 secret ends in = padding, and a header value can hold one.
		final var padded = argumentError("--storage-auth-secret", "pEauABL07ujkF2hJN7r6wA9/HPs=");
		assertTrue(padded.startsWith("Invalid argument after \"--storage-auth-secret\", not shown"), padded);
		assertFalse(padded.contains("pEauABL07ujkF2hJN7r6wA9"), padded);
		final var header = argumentError("--storage-driver-type=s3", "--storage-net-http-headers-x-auth", "token=abc");
		assertTrue(header.startsWith("Invalid argument after \"--storage-net-http-headers-x-auth\", not shown"), header);
		assertFalse(header.contains("token"), header);
		// After an argument that has its value, one with = is still named by the part before it.
		final var mistyped = argumentError("--storage-driver-type=s3", "storage-auth-secret=s3cr3t-value");
		assertTrue(mistyped.startsWith("Invalid argument: \"storage-auth-secret\""), mistyped);
	}

	@Test
	void aValueOfTheWrongTypeIsAnArgumentError() {
		final var message = argumentError("--storage-net-nodeInterfaces=lo");
		assertTrue(
						message.startsWith("Invalid configuration value type for the option \"storage-net-nodeInterfaces\""),
						message);
	}

	@Test
	void anUnparseableNumberIsAnArgumentError() {
		assertEquals(
						"Invalid value for the option \"load-op-limit-count\": For input string: \"5OO\"",
						argumentError("--storage-driver-type=dummy-mock", "--load-op-limit-count=5OO"));
	}

	@Test
	void aDeprecatedAliasWithNoTargetIsAnArgumentError() {
		assertEquals(
						"The argument \"test-step-distributed\" is deprecated",
						argumentError("--test-step-distributed", "--load-op-limit-count=10"));
	}

	@Test
	void theOldRecycleFlagOfTheDocsIsAnArgumentError() {
		assertEquals(
						"Invalid configuration option: \"load-op-recycle\"; --help lists the options",
						argumentError("--read", "--load-op-recycle=true"));
	}

	@Test
	void storageDriverRemoteIsRefusedWithSptsModel() {
		final var expected = "The argument \"storage-driver-remote\" is not supported: SPT has no remote driver"
						+ " services. Start each --storage-driver-addrs host as a worker engine (run.sh --run-node=true)"
						+ " and run this command without --storage-driver-remote. The engine you start also runs its"
						+ " share of the load.";
		assertEquals(expected, argumentError("--storage-driver-remote"));
		// The field report's Mongoose 3.6.2 command shape: the refusal comes first, whatever the hash order
		assertEquals(
						expected,
						argumentError(
										"--storage-driver-remote",
										"--storage-driver-addrs=10.0.0.1,10.0.0.2,10.0.0.3,10.0.0.4",
										"--load-limit-count=1000",
										"--storage-driver-type=dummy-mock",
										"--load-type=create",
										"--test-step-id=field-report"));
	}

	@Test
	void mongooseOptionsWithoutAnSptNameAreArgumentErrors() {
		assertEquals(
						"Invalid configuration option: \"load-limit-count\"; --help lists the options",
						argumentError("--storage-driver-type=dummy-mock", "--load-limit-count=10"));
		assertEquals(
						"Invalid configuration option: \"storage-net-http-namespace\"; --help lists the options",
						argumentError("--storage-driver-type=dummy-mock", "--storage-net-http-namespace=ns1"));
	}

	@ParameterizedTest
	@ValueSource(strings = {"yes", "no", "1", "0", "", " true", "on"
	})
	void aBooleanOptionRefusesAValueOtherThanTrueOrFalse(final String value) {
		assertEquals(
						"Invalid value for the option \"run-node\": \"" + value + "\" is not true or false",
						argumentError("--run-node=" + value, "--run-port=20800"));
	}

	@Test
	void aBooleanOptionSetThroughAnAliasIsNamedAsGiven() {
		assertEquals(
						"Invalid value for the option \"load-generator-shuffle\": \"yes\" is not true or false",
						argumentError("--load-generator-shuffle=yes"));
	}

	@ParameterizedTest
	@ValueSource(strings = {"true", "TRUE", "True", "false", "FALSE"
	})
	void aBooleanOptionTakesTrueOrFalseInAnyCase(final String value) throws Exception {
		final var config = apply("--load-op-shuffle=" + value);
		assertEquals(Boolean.parseBoolean(value), config.boolVal("load-op-shuffle"));
	}

	@Test
	void aBooleanOptionWithNoValueIsTrue() throws Exception {
		assertTrue(apply("--load-op-shuffle").boolVal("load-op-shuffle"));
	}

	@Test
	void aMapOptionTakesAFreeKey() throws Exception {
		final var config = apply("--storage-net-nodeInterfaces-entry=lo");
		assertEquals(Map.of("entry", "lo"), config.mapVal("storage-net-nodeInterfaces"));
	}

	@Test
	void aValidSetAppliesAsBefore() throws Exception {
		final var config = apply(
						"--storage-driver-type=dummy-mock",
						"--load-op-limit-count=10",
						"--read",
						"--item-naming-length=15",
						"--load-op-recycle-mode=true",
						"--load-op-shuffle=false",
						"--load-step-id=probe");
		assertEquals("dummy-mock", config.stringVal("storage-driver-type"));
		assertEquals(10L, config.longVal("load-op-limit-count"));
		assertEquals("read", config.stringVal("load-op-type"));
		assertEquals(15, config.intVal("item-naming-length"));
		assertTrue(config.boolVal("load-op-recycle-mode"));
		assertFalse(config.boolVal("load-op-shuffle"));
		assertEquals("probe", config.stringVal("load-step-id"));
	}

	@Test
	void aMissingScenarioFileIsAReportedStartFailure(@TempDir final Path dir) {
		final var failure = assertThrows(
						Main.ReportedFailure.class, () -> Main.readScenarioFile(dir.resolve("missing.js")));
		assertTrue(failure.getCause() instanceof java.nio.file.NoSuchFileException, String.valueOf(failure.getCause()));
	}

	@Test
	void aScenarioFileIsRead(@TempDir final Path dir) throws Exception {
		final var scenario = dir.resolve("ok.js");
		Files.writeString(scenario, "Load.run();\n", StandardCharsets.UTF_8);
		assertEquals("Load.run();" + System.lineSeparator(), Main.readScenarioFile(scenario));
	}

	@Test
	void theHelpTextListsTheOptionsAndTheAliasesReadably() throws Exception {
		final var text = Main.helpText();
		assertTrue(text.startsWith("Usage: java -jar spt.jar [--<option>=<value> ...]\n"), text);
		assertTrue(text.contains("Exit status: 0 success; 1 a run, start or scenario failure; 2 an argument error."), text);
		assertTrue(text.contains("\n\t--load-op-limit-count=<long>\n"), text);
		assertTrue(text.contains("\n\t--run-node=<true|false>\n"), text);
		assertTrue(text.contains("\n\t--item-naming-length=<int>\n"), text);
		assertTrue(text.contains("\n\t--storage-driver-type=<string>\n"), text);
		assertTrue(text.contains("\n\t--load-step-node-addrs=<list>\n"), text);
		// The other map-typed options are in the extensions' schemas, which the engine canary checks
		assertTrue(text.contains("\n\t--storage-net-nodeInterfaces-<key>=<value>\n"), text);
		assertTrue(text.contains("\n\t--read  same as --load-op-type=read\n"), text);
		assertTrue(text.contains("\n\t--load-type  same as --load-op-type (deprecated)\n"), text);
		assertTrue(text.contains("\n\t--test-step-distributed  no longer supported\n"), text);
		assertTrue(text.contains("\n\t--storage-driver-remote  not supported: SPT has no remote driver services."), text);
		assertFalse(text.contains("class java."), text);
		assertFalse(text.contains("interface java."), text);
		final var options = text.substring(text.indexOf("Options:\n"));
		options.lines()
						.filter(line -> !line.isEmpty() && !line.endsWith(":"))
						.forEach(line -> assertTrue(line.startsWith("\t--"), line));
	}

	@Test
	void theHelpRequestPrintsTheHelpTextToStandardOutput() throws Exception {
		final var originalOut = System.out;
		final var captured = new ByteArrayOutputStream();
		try {
			System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
			final Method method = Main.class.getDeclaredMethod("handleHelpRequest");
			method.setAccessible(true);
			method.invoke(null);
		} finally {
			System.setOut(originalOut);
		}
		assertEquals(Main.helpText(), captured.toString(StandardCharsets.UTF_8));
	}

	@Test
	void theExitStatusesAreTheContract() {
		assertEquals(0, Main.EXIT_SUCCESS);
		assertEquals(1, Main.EXIT_FAILURE);
		assertEquals(2, Main.EXIT_ARGUMENT_ERROR);
		assertTrue(Main.HELP_ARGS.contains("--help"));
		assertTrue(Main.HELP_ARGS.contains("-h"));
	}
}
