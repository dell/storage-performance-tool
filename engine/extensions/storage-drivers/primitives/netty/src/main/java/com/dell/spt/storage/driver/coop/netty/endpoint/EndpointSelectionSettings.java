package com.dell.spt.storage.driver.coop.netty.endpoint;

import com.dell.spt.base.config.EndpointSelectionConfig;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.exceptions.InvalidValuePathException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

/**
 * Validated endpoint-selection settings, parsed once per driver construction from the storage
 * configuration. Validation messages name the engine path and the matching CLI flag.
 */
public final class EndpointSelectionSettings {

	/** How the driver chooses the connect destination of each request attempt. */
	public enum Mode {
		DEFAULT(EndpointSelectionConfig.DEFAULT_SELECTION), ROUND_ROBIN("round-robin"), PER_REQUEST_DNS("per-request-dns");

		private final String configValue;

		Mode(final String configValue) {
			this.configValue = configValue;
		}

		public String configValue() {
			return configValue;
		}

		static Mode parse(final String value) {
			for (final var mode : values()) {
				if (mode.configValue.equals(value)) {
					return mode;
				}
			}
			throw new IllegalConfigurationException("storage.net.endpoint.selection (--endpoint-selection) must be "
							+ "default, round-robin or per-request-dns, not \"" + value + "\"");
		}
	}

	static final String HOSTNAME_PATH = "net-endpoint-hostname";
	static final String DNS_SERVER_PATH = "net-endpoint-dns-server";
	static final String DNS_TIMEOUT_PATH = "net-endpoint-dns-timeoutMilliSec";
	static final String CONNECT_TIMEOUT_PATH = "net-endpoint-connect-timeoutMilliSec";
	private static final String NODE_ADDRS_PATH = "net-node-addrs";
	private static final String NODE_PORT_PATH = "net-node-port";
	private static final String NODE_SLICE_PATH = "net-node-slice";

	private static final String HOSTNAME_SETTING = "storage.net.endpoint.hostname (--endpoint-hostname)";
	private static final String DNS_SERVER_SETTING = "storage.net.endpoint.dns.server (--dns-server)";
	private static final String DNS_TIMEOUT_SETTING = "storage.net.endpoint.dns.timeoutMilliSec (--dns-timeout)";
	private static final String CONNECT_TIMEOUT_SETTING = "storage.net.endpoint.connect.timeoutMilliSec (--endpoint-connect-timeout)";

	private static final int MAX_PORT = 65_535;
	private static final int MAX_HOSTNAME_LENGTH = 253;
	private static final int MAX_LABEL_LENGTH = 63;

	private static final EndpointSelectionSettings DEFAULT = new EndpointSelectionSettings(Mode.DEFAULT, List.of(), null, 0, null, 0, 0);

	private final Mode mode;
	private final List<InetSocketAddress> destinations;
	private final String hostname;
	private final int port;
	private final InetSocketAddress dnsServer;
	private final int dnsTimeoutMillis;
	private final int connectTimeoutMillis;

	private EndpointSelectionSettings(
					final Mode mode,
					final List<InetSocketAddress> destinations,
					final String hostname,
					final int port,
					final InetSocketAddress dnsServer,
					final int dnsTimeoutMillis,
					final int connectTimeoutMillis) {
		this.mode = mode;
		this.destinations = destinations;
		this.hostname = hostname;
		this.port = port;
		this.dnsServer = dnsServer;
		this.dnsTimeoutMillis = dnsTimeoutMillis;
		this.connectTimeoutMillis = connectTimeoutMillis;
	}

	/** Parses and validates the settings; a missing subtree means the default mode. */
	public static EndpointSelectionSettings fromStorage(final Config storage) {
		final var mode = Mode.parse(EndpointSelectionConfig.selection(storage));
		final var hostnameSetting = optionalString(storage, HOSTNAME_PATH);
		final var dnsServerSetting = optionalString(storage, DNS_SERVER_PATH);
		if (mode == Mode.DEFAULT) {
			require(hostnameSetting == null, HOSTNAME_SETTING + " requires round-robin selection");
			require(dnsServerSetting == null, DNS_SERVER_SETTING + " requires per-request-dns selection");
			return DEFAULT;
		}
		require(!Boolean.TRUE.equals(optionalValue(storage, NODE_SLICE_PATH)),
						"storage.net.node.slice (--slice-endpoints) cannot be combined with "
										+ mode.configValue() + " selection");
		final var connectTimeout = positiveMillis(storage, CONNECT_TIMEOUT_PATH, CONNECT_TIMEOUT_SETTING);
		final var endpoints = endpoints(storage);
		if (mode == Mode.ROUND_ROBIN) {
			require(dnsServerSetting == null, DNS_SERVER_SETTING + " requires per-request-dns selection");
			require(!endpoints.isEmpty(), "Round-robin selection requires at least one endpoint");
			final List<InetSocketAddress> destinations = new ArrayList<>(endpoints.size());
			final Set<InetSocketAddress> seen = new HashSet<>();
			for (final var endpoint : endpoints) {
				final var address = Ipv4Literal.parse(endpoint.host())
								.orElseThrow(() -> new IllegalConfigurationException(
												"Round-robin endpoints must be IPv4 addresses, not \"" + endpoint + "\""));
				final var destination = new InetSocketAddress(address, endpoint.port());
				require(seen.add(destination), "Duplicate round-robin endpoint \"" + endpoint + "\"");
				destinations.add(destination);
			}
			final var hostname = hostnameSetting == null ? null : hostname(hostnameSetting, HOSTNAME_SETTING);
			return new EndpointSelectionSettings(mode, List.copyOf(destinations), hostname, 0, null, 0, connectTimeout);
		}
		require(hostnameSetting == null, HOSTNAME_SETTING
						+ " applies only to round-robin selection; per-request DNS uses the endpoint hostname");
		require(endpoints.size() == 1,
						"Per-request DNS selection requires exactly one endpoint hostname, found " + endpoints.size());
		final var endpoint = endpoints.get(0);
		final var hostname = hostname(endpoint.host(), "The per-request DNS endpoint");
		final var dnsTimeout = positiveMillis(storage, DNS_TIMEOUT_PATH, DNS_TIMEOUT_SETTING);
		final var dnsServer = dnsServerSetting == null ? null : dnsServer(dnsServerSetting);
		return new EndpointSelectionSettings(mode, List.of(), hostname, endpoint.port(), dnsServer, dnsTimeout,
						connectTimeout);
	}

	public Mode mode() {
		return mode;
	}

	public boolean isDefault() {
		return mode == Mode.DEFAULT;
	}

	/** Round robin: the destinations in rotation order. Empty in other modes. */
	public List<InetSocketAddress> destinations() {
		return destinations;
	}

	/**
	 * The logical hostname for Host, signing and SNI: the endpoint hostname in per-request DNS mode,
	 * the optional configured hostname in round robin.
	 */
	public Optional<String> hostname() {
		return Optional.ofNullable(hostname);
	}

	/** Per-request DNS: the endpoint port used for every resolved address. */
	public int port() {
		return port;
	}

	/** Per-request DNS: the explicit server, or empty to use the host resolver configuration. */
	public Optional<InetSocketAddress> dnsServer() {
		return Optional.ofNullable(dnsServer);
	}

	public int dnsTimeoutMillis() {
		return dnsTimeoutMillis;
	}

	public int connectTimeoutMillis() {
		return connectTimeoutMillis;
	}

	private record Endpoint(String host, int port) {
		@Override
		public String toString() {
			return host + ":" + port;
		}
	}

	private static List<Endpoint> endpoints(final Config storage) {
		final var raw = optionalValue(storage, NODE_ADDRS_PATH);
		if (!(raw instanceof List<?> entries)) {
			return List.of();
		}
		final var sharedPort = optionalValue(storage, NODE_PORT_PATH) instanceof Number n ? n.intValue() : 0;
		final List<Endpoint> endpoints = new ArrayList<>(entries.size());
		for (final var entry : entries) {
			final var text = String.valueOf(entry).strip();
			final var colon = text.indexOf(':');
			if (colon != text.lastIndexOf(':')) {
				throw new IllegalConfigurationException("IPv6 endpoints are not supported by endpoint selection: \""
								+ text + "\"");
			}
			final var host = colon < 0 ? text : text.substring(0, colon);
			final var port = colon < 0 ? sharedPort : port(text.substring(colon + 1), "Endpoint \"" + text + "\"");
			require(port >= 1 && port <= MAX_PORT, "Endpoint \"" + text + "\" has no valid port");
			endpoints.add(new Endpoint(host, port));
		}
		return endpoints;
	}

	private static InetSocketAddress dnsServer(final String text) {
		final var colon = text.indexOf(':');
		final var host = colon < 0 ? text : text.substring(0, colon);
		final var address = Ipv4Literal.parse(host)
						.orElseThrow(() -> new IllegalConfigurationException(
										DNS_SERVER_SETTING + " must be an IPv4 address with an optional port, not \"" + text + "\""));
		final var port = colon < 0 ? EndpointSelectionConstants.DNS_PORT : port(text.substring(colon + 1), DNS_SERVER_SETTING);
		return new InetSocketAddress(address, port);
	}

	/** Validates a DNS hostname and returns it lowercased without a trailing dot. */
	static String hostname(final String text, final String what) {
		var name = text.strip();
		if (name.endsWith(".")) {
			name = name.substring(0, name.length() - 1);
		}
		name = name.toLowerCase(Locale.ROOT);
		require(Ipv4Literal.parse(name).isEmpty(), what + " must be a hostname, not an IP address: \"" + text + "\"");
		require(!name.isEmpty() && name.length() <= MAX_HOSTNAME_LENGTH, what + " is not a valid hostname: \"" + text + "\"");
		for (final var label : name.split("\\.", -1)) {
			require(isLabel(label), what + " is not a valid hostname: \"" + text + "\"");
		}
		return name;
	}

	private static boolean isLabel(final String label) {
		if (label.isEmpty() || label.length() > MAX_LABEL_LENGTH || label.startsWith("-") || label.endsWith("-")) {
			return false;
		}
		for (var i = 0; i < label.length(); i++) {
			final var c = label.charAt(i);
			if (!(c >= 'a' && c <= 'z') && !(c >= '0' && c <= '9') && c != '-') {
				return false;
			}
		}
		return true;
	}

	private static int port(final String text, final String what) {
		try {
			final var port = Integer.parseInt(text);
			require(port >= 1 && port <= MAX_PORT, what + " has an invalid port \"" + text + "\"");
			return port;
		} catch (final NumberFormatException e) {
			throw new IllegalConfigurationException(what + " has an invalid port \"" + text + "\"", e);
		}
	}

	private static int positiveMillis(final Config storage, final String path, final String what) {
		final var value = optionalValue(storage, path);
		require(value instanceof Number, what + " is not set");
		final var millis = ((Number) value).longValue();
		require(millis >= 1 && millis <= Integer.MAX_VALUE, what + " must be between 1 and " + Integer.MAX_VALUE
						+ " ms, not " + millis);
		return (int) millis;
	}

	private static String optionalString(final Config storage, final String path) {
		final var value = optionalValue(storage, path);
		if (value == null) {
			return null;
		}
		final var text = String.valueOf(value).strip();
		return text.isEmpty() ? null : text;
	}

	private static Object optionalValue(final Config storage, final String path) {
		try {
			return storage.val(path);
		} catch (final InvalidValuePathException | NoSuchElementException e) {
			return null;
		}
	}

	private static void require(final boolean condition, final String message) {
		if (!condition) {
			throw new IllegalConfigurationException(message);
		}
	}
}
