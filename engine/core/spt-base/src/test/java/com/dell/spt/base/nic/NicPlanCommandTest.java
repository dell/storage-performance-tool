package com.dell.spt.base.nic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** The image command's arguments, streams and exit codes. */
class NicPlanCommandTest {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	@TempDir
	Path root;

	private FakeHost host;
	private final List<Boolean> readerRequests = new ArrayList<>();
	private final ByteArrayOutputStream out = new ByteArrayOutputStream();
	private final ByteArrayOutputStream err = new ByteArrayOutputStream();

	@BeforeEach
	void createHost() {
		host = new FakeHost(root);
		host.physical("ens2np0", "0000:17:00.0").address("192.0.2.11/24").speed(400000);
	}

	@Test
	void inventoryAloneIsOneJsonLineOnStdout() throws Exception {
		assertEquals(NicPlanCommand.EXIT_OK, run());

		final var output = stdout();
		assertTrue(output.endsWith("\n") && output.indexOf('\n') == output.length() - 1, "one line");
		final var document = MAPPER.readTree(output);
		assertEquals("spt.nic-plan", document.get("format").asText());
		assertEquals(1, document.get("version").asInt());
		assertEquals("client-a", document.get("host").asText());
		assertTrue(document.at("/plan").isMissingNode());
		assertEquals("", stderr());
		assertEquals(List.of(false), readerRequests);
	}

	@Test
	void planWithBothArgumentForms() throws Exception {
		assertEquals(NicPlanCommand.EXIT_OK, run("--interfaces=auto", "--endpoint", "192.0.2.101:9000", "--endpoint=192.0.2.102:9000"));

		final var plan = MAPPER.readTree(stdout()).get("plan");
		assertEquals("auto", plan.get("mode").asText());
		assertEquals(List.of("ens2np0"), MAPPER.convertValue(plan.get("nicSet"), List.class));
		assertEquals(2, plan.at("/groups/0/paths").size());
	}

	@Test
	void aListIsCommaSeparated() throws Exception {
		host.physical("ens3np0", "0000:18:00.0").address("192.0.2.12/24").speed(400000);

		run("--interfaces", "ens3np0, ens2np0", "--endpoint", "192.0.2.101:9000");

		final var plan = MAPPER.readTree(stdout()).get("plan");
		assertEquals("list", plan.get("mode").asText());
		assertEquals(List.of("ens3np0", "ens2np0"), MAPPER.convertValue(plan.get("nicSet"), List.class));
	}

	@Test
	void aPlanWithFailuresStillExitsZero() throws Exception {
		assertEquals(NicPlanCommand.EXIT_OK, run("--interfaces", "eth9", "--endpoint", "192.0.2.101:9000"));

		final var plan = MAPPER.readTree(stdout()).get("plan");
		assertFalse(plan.get("ok").asBoolean());
		assertEquals("entry_unresolved", plan.at("/failures/0/code").asText());
	}

	@Test
	void rdmaAsksForTheProbingReader() {
		run("--rdma", "--interfaces", "auto", "--endpoint", "192.0.2.101:9000");

		assertEquals(List.of(true), readerRequests);
	}

	@Test
	void prettyIndents() {
		run("--pretty");

		assertTrue(stdout().startsWith("{\n  \"format\""), this::stdout);
	}

	@ParameterizedTest
	@ValueSource(strings = {"--bogus", "--interfaces", "--interfaces=", "--endpoint=", "--endpoint", "--interfaces=auto --interfaces=auto", "plain"
	})
	void usageErrorsExitTwoWithNothingOnStdout(final String args) {
		assertEquals(NicPlanCommand.EXIT_USAGE, run(args.split(" ")));

		assertEquals("", stdout());
		assertTrue(stderr().startsWith("spt-nic-plan: "), this::stderr);
		assertTrue(stderr().contains(NicPlanCommand.USAGE), this::stderr);
		assertTrue(readerRequests.isEmpty());
	}

	@Test
	void helpGoesToStderr() {
		assertEquals(NicPlanCommand.EXIT_OK, run("--interfaces", "auto", "-h"));

		assertEquals("", stdout());
		assertEquals(NicPlanCommand.USAGE + System.lineSeparator(), stderr());
	}

	@Test
	void anInternalErrorExitsOne() {
		final var command = new NicPlanCommand(
						rdma -> {
							throw new UncheckedIOException(new java.io.IOException("sysfs went away"));
						},
						NicPlannerTest.RESOLVER,
						new NicPlanJson.Engine("5.17.0", "abc"));

		assertEquals(NicPlanCommand.EXIT_INTERNAL, command.run(new String[0], print(out), print(err)));

		assertEquals("", stdout());
		assertTrue(stderr().startsWith("spt-nic-plan: internal error: "), this::stderr);
		assertTrue(stderr().contains("sysfs went away"));
	}

	private int run(final String... args) {
		final var command = new NicPlanCommand(
						rdma -> {
							readerRequests.add(rdma);
							return host.reader(rdma);
						},
						NicPlannerTest.RESOLVER,
						new NicPlanJson.Engine("5.17.0", "abc"));
		return command.run(args, print(out), print(err));
	}

	private String stdout() {
		return out.toString(StandardCharsets.UTF_8);
	}

	private String stderr() {
		return err.toString(StandardCharsets.UTF_8);
	}

	private static PrintStream print(final ByteArrayOutputStream bytes) {
		return new PrintStream(bytes, true, StandardCharsets.UTF_8);
	}
}
