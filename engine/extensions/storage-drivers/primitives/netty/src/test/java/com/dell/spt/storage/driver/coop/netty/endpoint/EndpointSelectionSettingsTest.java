package com.dell.spt.storage.driver.coop.netty.endpoint;

import static com.dell.spt.base.Constants.APP_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.env.Extension;
import com.dell.spt.storage.driver.coop.netty.endpoint.EndpointSelectionSettings.Mode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Parses settings from the real merged schema and shipped defaults, overridden like CLI flags. */
class EndpointSelectionSettingsTest {

	@Test
	void shippedDefaultsSelectTheDefaultMode() throws Exception {
		final var root = shippedConfig();
		final var settings = EndpointSelectionSettings.fromStorage(root.configVal("storage"));

		assertEquals(Mode.DEFAULT, settings.mode());
		assertEquals("default", root.stringVal("storage-net-endpoint-selection"));
		assertEquals(5_000, root.intVal("storage-net-endpoint-dns-timeoutMilliSec"));
		assertEquals(30_000, root.intVal("storage-net-endpoint-connect-timeoutMilliSec"));
	}

	@Test
	void defaultModeRejectsSettingsOfOtherModes() throws Exception {
		assertRejected(Map.of("net-endpoint-hostname", "s3.example.test"), "requires round-robin");
		assertRejected(Map.of("net-endpoint-dns-server", "10.0.0.53"), "requires per-request-dns");
	}

	@Test
	void rejectsUnknownMode() throws Exception {
		assertRejected(Map.of("net-endpoint-selection", "random"), "must be default, round-robin or per-request-dns");
	}

	@Test
	void roundRobinKeepsOrderAndPerEntryPorts() throws Exception {
		final var settings = parse(Map.of(
						"net-endpoint-selection", "round-robin",
						"net-node-addrs", List.of("10.0.0.2", "10.0.0.1:9021", "10.0.0.2:9021"),
						"net-node-port", 9020,
						"net-endpoint-hostname", "S3.Example.Test."));

		assertEquals(Mode.ROUND_ROBIN, settings.mode());
		assertEquals(List.of(
						new InetSocketAddress("10.0.0.2", 9020),
						new InetSocketAddress("10.0.0.1", 9021),
						new InetSocketAddress("10.0.0.2", 9021)), settings.destinations());
		assertEquals(Optional.of("s3.example.test"), settings.hostname());
		assertEquals(30_000, settings.connectTimeoutMillis());
	}

	@Test
	void roundRobinHostnameIsOptional() throws Exception {
		final var settings = parse(Map.of(
						"net-endpoint-selection", "round-robin",
						"net-node-addrs", List.of("10.0.0.1"),
						"net-node-port", 9020));

		assertEquals(Optional.empty(), settings.hostname());
	}

	@Test
	void roundRobinRejectsInvalidCombinations() throws Exception {
		final Map<String, Object> rr = Map.of("net-endpoint-selection", "round-robin", "net-node-port", 9020);
		assertRejected(with(rr, "net-node-addrs", List.of("s3.example.test")), "must be IPv4 addresses");
		assertRejected(with(rr, "net-node-addrs", List.of("10.0.0.1", "10.0.0.1:9020")), "Duplicate");
		assertRejected(with(rr, "net-node-addrs", List.of("[::1]:9020")), "IPv6");
		assertRejected(with(rr, "net-node-addrs", List.of("10.0.0.1:0")), "invalid port");
		assertRejected(with(with(rr, "net-node-addrs", List.of("10.0.0.1")), "net-node-slice", true), "slice");
		assertRejected(with(with(rr, "net-node-addrs", List.of("10.0.0.1")), "net-endpoint-dns-server", "10.0.0.53"),
						"requires per-request-dns");
		assertRejected(with(with(rr, "net-node-addrs", List.of("10.0.0.1")), "net-endpoint-hostname", "10.0.0.9"),
						"not an IP address");
		assertRejected(with(with(rr, "net-node-addrs", List.of("10.0.0.1")), "net-endpoint-hostname", "bad_name.test"),
						"not a valid hostname");
		assertRejected(with(with(rr, "net-node-addrs", List.of("10.0.0.1")), "net-endpoint-connect-timeoutMilliSec", 0),
						"between 1 and");
	}

	@Test
	void perRequestDnsUsesTheSingleEndpointHostname() throws Exception {
		final var settings = parse(Map.of(
						"net-endpoint-selection", "per-request-dns",
						"net-node-addrs", List.of("S3.Example.Test:9021")));

		assertEquals(Mode.PER_REQUEST_DNS, settings.mode());
		assertEquals(Optional.of("s3.example.test"), settings.hostname());
		assertEquals(9021, settings.port());
		assertEquals(Optional.empty(), settings.dnsServer());
		assertEquals(5_000, settings.dnsTimeoutMillis());
		assertTrue(settings.destinations().isEmpty());
	}

	@Test
	void perRequestDnsParsesAnExplicitServer() throws Exception {
		final Map<String, Object> dns = Map.of(
						"net-endpoint-selection", "per-request-dns",
						"net-node-addrs", List.of("s3.example.test"),
						"net-node-port", 9020);

		assertEquals(Optional.of(new InetSocketAddress("10.0.0.53", 53)),
						parse(with(dns, "net-endpoint-dns-server", "10.0.0.53")).dnsServer());
		assertEquals(Optional.of(new InetSocketAddress("10.0.0.53", 5353)),
						parse(with(dns, "net-endpoint-dns-server", "10.0.0.53:5353")).dnsServer());
	}

	@Test
	void perRequestDnsRejectsInvalidCombinations() throws Exception {
		final Map<String, Object> dns = Map.of("net-endpoint-selection", "per-request-dns", "net-node-port", 9020);
		assertRejected(with(dns, "net-node-addrs", List.of("a.example.test", "b.example.test")), "exactly one");
		assertRejected(with(dns, "net-node-addrs", List.of("10.0.0.1")), "not an IP address");
		assertRejected(with(dns, "net-node-addrs", List.of("-bad.example.test")), "not a valid hostname");
		final var one = with(dns, "net-node-addrs", List.of("s3.example.test"));
		assertRejected(with(one, "net-endpoint-hostname", "s3.example.test"), "applies only to round-robin");
		assertRejected(with(one, "net-endpoint-dns-server", "dns.example.test"), "must be an IPv4 address");
		assertRejected(with(one, "net-endpoint-dns-server", "10.0.0.53:0"), "invalid port");
		assertRejected(with(one, "net-endpoint-dns-timeoutMilliSec", 0), "between 1 and");
		assertRejected(with(one, "net-node-slice", true), "slice");
	}

	@Test
	void missingSubtreeMeansDefault() {
		final var storage = new BasicConfig("-", Map.of("net", Map.of("node", Map.of("addrs", "list"))));
		assertEquals(Mode.DEFAULT, EndpointSelectionSettings.fromStorage(storage).mode());
	}

	private static EndpointSelectionSettings parse(final Map<String, Object> overrides) throws Exception {
		final var root = shippedConfig();
		overrides.forEach((path, value) -> root.val("storage-" + path, value));
		return EndpointSelectionSettings.fromStorage(root.configVal("storage"));
	}

	private static void assertRejected(final Map<String, Object> overrides, final String messagePart) {
		final var failure = assertThrows(IllegalConfigurationException.class, () -> parse(overrides));
		assertTrue(failure.getMessage().contains(messagePart), failure.getMessage());
	}

	private static Map<String, Object> with(final Map<String, Object> base, final String path, final Object value) {
		final Map<String, Object> copy = new java.util.HashMap<>(base);
		copy.put(path, value);
		return copy;
	}

	@SuppressWarnings("unchecked")
	private static Config shippedConfig() throws Exception {
		final var loader = EndpointSelectionSettingsTest.class.getClassLoader();
		final List<Map<String, Object>> schemas = new ArrayList<>();
		for (final var extension : Extension.load(loader)) {
			final var provider = extension.schemaProvider();
			if (provider != null) {
				schemas.add(provider.schema());
			}
		}
		SchemaProvider.resolve(APP_NAME, loader).stream().findFirst().ifPresent(schemas::add);
		final var yaml = new YAMLMapper();
		final List<Map<String, Object>> defaults = new ArrayList<>();
		for (final var resource : List.of("/config/defaults.yaml", "/config/defaults-storage-net.yaml")) {
			try (final var in = Objects.requireNonNull(EndpointSelectionSettingsTest.class.getResourceAsStream(resource),
							resource)) {
				defaults.add(yaml.readValue(in, Map.class));
			}
		}
		return new BasicConfig("-", TreeUtil.reduceForest(schemas), TreeUtil.reduceForest(defaults));
	}
}
