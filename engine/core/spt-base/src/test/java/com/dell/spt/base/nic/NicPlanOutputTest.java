package com.dell.spt.base.nic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion.VersionFlag;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The {@code spt.nic-plan} output: golden documents shared with the CLI's tests, and the
 * version 1 schema that pins the field names a version 1 reader relies on.
 */
class NicPlanOutputTest {

	private static final Path FIXTURES = Path.of(System.getProperty("spt.test.nic-plan-fixtures"));
	private static final NicPlanJson.Engine ENGINE = new NicPlanJson.Engine("5.17.0", "0123456789abcdef0123456789abcdef01234567");
	private static final ObjectMapper MAPPER = new ObjectMapper();

	@TempDir
	Path root;

	@Test
	void sc26AutoRdmaPlanMatchesTheGolden() throws Exception {
		final var host = new FakeHost(root);
		final var args = new ArrayList<>(List.of("--interfaces", "auto", "--rdma", "--pretty"));
		for (final var endpoint : Layouts.sc26(host)) {
			args.add("--endpoint");
			args.add(endpoint);
		}

		assertGolden("sc26-auto-rdma.json", run(host, args));
	}

	@Test
	void sc26InventoryMatchesTheGolden() throws Exception {
		final var host = new FakeHost(root);
		Layouts.sc26(host);

		assertGolden("sc26-inventory.json", run(host, List.of("--pretty")));
	}

	@Test
	void goldensFollowTheVersionOneSchema() throws Exception {
		for (final var golden : List.of("sc26-auto-rdma.json", "sc26-inventory.json")) {
			assertValid(MAPPER.readTree(Files.readString(FIXTURES.resolve(golden))));
		}
	}

	@Test
	void everyOptionalFieldFollowsTheSchema() throws Exception {
		final var host = new FakeHost(root);
		host.sysctlAll(0, 1, 2);
		host.physical("ens2np0", "0000:17:00.0").speed(400000);
		host.vlan("ens2np0.100", "ens2np0").address("192.0.2.21/24").speed(400000);
		host.vlan("ens2np0.200", "ens2np0").address("192.0.2.22/24").speed(400000);
		host.physical("ens3np0", "0000:18:00.0").address("192.0.2.12/24").speed(200000);

		final var auto = MAPPER.readTree(run(host, List.of("--interfaces", "auto", "--endpoint", "192.0.2.101:9000", "--endpoint", "nohost.example:9000")));
		final var list = MAPPER.readTree(run(host, List.of("--interfaces", "ens2np0.100,ens2np0.200,eth9", "--endpoint", "192.0.2.101:9000")));

		assertValid(auto);
		assertValid(list);
		assertEquals("ens2np0.100", auto.at("/plan/nics/2/reason/with").asText(), "a reason's with");
		assertEquals("ens2np0.100", auto.at("/plan/endpoints/0/reductions/0/with").asText());
		assertTrue(list.at("/plan/failures/0/nic").isMissingNode(), "absent, not null, when a finding names no NIC");
		assertFalse(list.at("/plan/failures/1/fix").isMissingNode());
	}

	@Test
	void versionOneIsCurrent() throws Exception {
		assertEquals("spt.nic-plan", NicPlanJson.FORMAT);
		assertEquals(1, NicPlanJson.VERSION);
		assertEquals(NicPlanJson.VERSION, schemaNode().at("/properties/version/const").asInt());
		assertEquals(NicPlanJson.FORMAT, schemaNode().at("/properties/format/const").asText());
	}

	@Test
	void additionsKeepVersionOne() throws Exception {
		final var document = (ObjectNode) golden("sc26-auto-rdma.json");
		document.put("addedLater", true);
		((ObjectNode) document.at("/inventory/nics/0")).put("addedLater", "x");
		((ArrayNode) document.at("/plan/warnings")).addObject().put("code", "a_code_added_later").put("text", "a later warning");

		assertValid(document);
	}

	@Test
	void removalsRenamesAndAnotherVersionFailVersionOne() throws Exception {
		final var removed = (ObjectNode) golden("sc26-auto-rdma.json");
		((ObjectNode) removed.at("/plan/endpoints/0")).remove("tierSpeedMbps");
		final var renamed = (ObjectNode) golden("sc26-auto-rdma.json");
		final var nic = (ObjectNode) renamed.at("/inventory/nics/0");
		nic.set("speed", nic.remove("speedMbps"));
		final var newer = (ObjectNode) golden("sc26-auto-rdma.json");
		newer.put("version", 2);

		for (final var document : List.of(removed, renamed, newer)) {
			assertFalse(schema().validate(document).isEmpty());
		}
	}

	@Test
	void codesAreStableIdentifiers() {
		final var ids = Arrays.stream(NicCode.values()).map(NicCode::id).toList();

		assertEquals(ids.size(), new HashSet<>(ids).size());
		assertTrue(ids.stream().allMatch(id -> id.matches("[a-z0-9_]+")), ids::toString);
		assertTrue(Arrays.stream(NicCode.values()).noneMatch(code -> code.text().isBlank()));
		assertEquals("shares_port", NicCode.SHARES_PORT.id());
		assertEquals("rocev2_gid_not_port_1", NicCode.ROCEV2_GID_NOT_PORT_1.id());
	}

	private String run(final FakeHost host, final List<String> args) {
		final var out = new ByteArrayOutputStream();
		final var err = new ByteArrayOutputStream();
		final var command = new NicPlanCommand(host::reader, NicPlannerTest.RESOLVER, ENGINE);

		final int status = command.run(args.toArray(String[]::new), print(out), print(err));

		assertEquals(NicPlanCommand.EXIT_OK, status, err::toString);
		assertEquals("", err.toString(StandardCharsets.UTF_8));
		return out.toString(StandardCharsets.UTF_8);
	}

	/** Compares with the shared golden; on a difference, writes the actual output under build/ for review. */
	private static void assertGolden(final String name, final String actual) throws Exception {
		final var golden = FIXTURES.resolve(name);
		final var expected = Files.exists(golden) ? Files.readString(golden).replace("\r\n", "\n") : null;
		if (!actual.equals(expected)) {
			final var written = Path.of("build", "nic-plan-actual", name).toAbsolutePath();
			Files.createDirectories(written.getParent());
			Files.writeString(written, actual);
			assertEquals(expected, actual, "differs from " + golden + "; the actual output is in " + written);
		}
	}

	private static JsonNode golden(final String name) throws Exception {
		return MAPPER.readTree(Files.readString(FIXTURES.resolve(name)));
	}

	private static void assertValid(final JsonNode document) throws Exception {
		final var errors = schema().validate(document);
		assertTrue(errors.isEmpty(), errors::toString);
	}

	private static JsonSchema schema() throws Exception {
		final var schema = JsonSchemaFactory.getInstance(VersionFlag.V202012).getSchema(schemaNode());
		schema.initializeValidators();
		return schema;
	}

	private static JsonNode schemaNode() throws Exception {
		return MAPPER.readTree(Files.readString(FIXTURES.resolve("nic-plan-v1.schema.json")));
	}

	private static PrintStream print(final ByteArrayOutputStream bytes) {
		return new PrintStream(bytes, true, StandardCharsets.UTF_8);
	}
}
