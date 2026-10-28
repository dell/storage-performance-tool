package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The network-namespace test's topology. This JVM's namespace holds the client NICs, two veth ends
 * on one subnet with the shared-subnet sysctls pinned ({@code arp_ignore=1},
 * {@code arp_announce=2}, {@code rp_filter=2}). Their peers sit on one bridge in a server namespace,
 * which holds one address per stub endpoint; {@link NetnsStubServer} serves them from its own
 * process there, and that process holds the namespace.
 *
 * <p>It needs {@code NET_ADMIN} and {@code SYS_ADMIN} over this JVM's namespace, a sysfs mounted in
 * it, and {@code ip}, {@code ss}, {@code unshare} and {@code nsenter} on the path. Run it with
 * {@code engine/tools/netns-test.sh}, or in a container started with {@code --cap-add NET_ADMIN
 * --cap-add SYS_ADMIN}, as CI does. A missing requirement fails the test instead of skipping it.
 */
final class NetnsTopology implements AutoCloseable {

	static final String NIC_A = "sptc0";
	static final String NIC_B = "sptc1";
	static final String ADDRESS_A = "192.0.2.11";
	static final String ADDRESS_B = "192.0.2.12";
	static final List<String> ENDPOINTS = List.of("192.0.2.21:9000", "192.0.2.22:9000", "192.0.2.23:9000", "192.0.2.24:9000");

	private static final String PEER_A = "spts0";
	private static final String PEER_B = "spts1";
	private static final String BRIDGE = "sptbr";
	private static final String PREFIX = "/24";
	private static final long COMMAND_TIMEOUT_SECONDS = 10;
	private static final String REQUIREMENTS = "The network-namespace tests need NET_ADMIN and SYS_ADMIN over their network "
					+ "namespace and a sysfs mounted in it: run engine/tools/netns-test.sh, or a container started with "
					+ "--cap-add NET_ADMIN --cap-add SYS_ADMIN --cap-drop NET_RAW";

	private final Process stub;
	private final BufferedWriter commands;
	private final BlockingQueue<String> replies = new LinkedBlockingQueue<>();
	private final List<String> blackHoles = new ArrayList<>();

	/** One established TCP connection of this namespace: its source, the device it is bound to and its peer. */
	record Connection(String source, String device, String peer) {}

	private NetnsTopology(final Process stub) {
		this.stub = stub;
		this.commands = new BufferedWriter(new OutputStreamWriter(stub.getOutputStream(), StandardCharsets.UTF_8));
		final var reader = new Thread(() -> {
			try (var lines = new BufferedReader(new InputStreamReader(stub.getInputStream(), StandardCharsets.UTF_8))) {
				String line;
				while ((line = lines.readLine()) != null) {
					replies.add(line);
				}
			} catch (final IOException ignored) {
				// The stub exited.
			}
			replies.add("exited");
		}, "netns-stub-reader");
		reader.setDaemon(true);
		reader.start();
	}

	/** Builds the topology and starts the stub's listeners. */
	static NetnsTopology create() throws Exception {
		for (final var tool : List.of("ip", "ss", "unshare", "nsenter")) {
			try {
				run(tool, "-V");
			} catch (final IOException e) {
				fail(tool + " is not on the path; install iproute2 and util-linux. " + REQUIREMENTS);
			}
		}
		deleteQuietly(NIC_A);
		deleteQuietly(NIC_B);
		try {
			run("ip", "link", "add", NIC_A, "type", "veth", "peer", "name", PEER_A);
			run("ip", "link", "add", NIC_B, "type", "veth", "peer", "name", PEER_B);
		} catch (final IllegalStateException e) {
			fail(e.getMessage() + ". " + REQUIREMENTS);
		}
		if (!Files.isDirectory(Path.of("/sys/class/net", NIC_A))) {
			deleteQuietly(NIC_A);
			deleteQuietly(NIC_B);
			fail("/sys/class/net does not show this network namespace's interfaces. " + REQUIREMENTS);
		}
		final var topology = new NetnsTopology(startStub());
		try {
			topology.await("started");
			topology.configure();
			topology.command("listen");
			return topology;
		} catch (final Exception | AssertionError e) {
			topology.close();
			throw e;
		}
	}

	private static Process startStub() throws Exception {
		final var java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
		final var classes = Path.of(NetnsStubServer.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toString();
		final var command = new ArrayList<>(List.of("unshare", "-n", java, "-cp", classes, NetnsStubServer.class.getName()));
		command.addAll(ENDPOINTS);
		return new ProcessBuilder(command).redirectError(ProcessBuilder.Redirect.INHERIT).start();
	}

	private void configure() throws Exception {
		final var pid = Long.toString(stub.pid());
		run("ip", "link", "set", PEER_A, "netns", pid);
		run("ip", "link", "set", PEER_B, "netns", pid);
		server("link", "set", "lo", "up");
		server("link", "add", BRIDGE, "type", "bridge");
		server("link", "set", PEER_A, "master", BRIDGE);
		server("link", "set", PEER_B, "master", BRIDGE);
		for (final var endpoint : ENDPOINTS) {
			server("addr", "add", host(endpoint) + PREFIX, "dev", BRIDGE);
		}
		for (final var link : List.of(BRIDGE, PEER_A, PEER_B)) {
			server("link", "set", link, "up");
		}
		run("ip", "addr", "add", ADDRESS_A + PREFIX, "dev", NIC_A);
		run("ip", "addr", "add", ADDRESS_B + PREFIX, "dev", NIC_B);
		pinSysctls();
		run("ip", "link", "set", NIC_A, "up");
		run("ip", "link", "set", NIC_B, "up");
	}

	/**
	 * Pins the shared-subnet sysctls on the client NICs. Where {@code /proc/sys} is read-only, as in a
	 * container, the effective values (the higher of {@code all} and the NIC's) must already hold.
	 */
	private static void pinSysctls() throws IOException {
		final Map<String, Integer> pinned = Map.of("arp_ignore", 1, "arp_announce", 2, "rp_filter", 2);
		try {
			for (final var nic : List.of(NIC_A, NIC_B)) {
				for (final var sysctl : pinned.entrySet()) {
					Files.writeString(conf(nic, sysctl.getKey()), sysctl.getValue().toString());
				}
			}
		} catch (final IOException readOnly) {
			final List<String> missing = new ArrayList<>();
			for (final var nic : List.of(NIC_A, NIC_B)) {
				for (final var sysctl : pinned.entrySet()) {
					final var effective = Math.max(sysctl(conf("all", sysctl.getKey())), sysctl(conf(nic, sysctl.getKey())));
					if (effective != sysctl.getValue()) {
						missing.add("net.ipv4.conf.all." + sysctl.getKey() + "=" + sysctl.getValue());
					}
				}
			}
			if (!missing.isEmpty()) {
				fail("/proc/sys is read-only (" + readOnly.getMessage() + ") and the effective sysctls differ; start the container "
								+ "with --sysctl for each of " + missing.stream().distinct().sorted().toList());
			}
		}
	}

	/** Connects to endpoint {@code index} are refused, and its connections close. */
	void refuse(final int index) throws Exception {
		command("refuse " + index);
	}

	/** Endpoint {@code index} listens again. */
	void serve(final int index) throws Exception {
		command("serve " + index);
	}

	/**
	 * The server namespace drops everything endpoint {@code index} sends, so a connect to it times out
	 * and its established connections fall silent.
	 */
	void blackHole(final int index) throws Exception {
		final var address = host(ENDPOINTS.get(index));
		server("rule", "add", "from", address, "blackhole");
		blackHoles.add(address);
	}

	/**
	 * This namespace aborts its connections to endpoint {@code index}, which fail at once. After
	 * {@link #blackHole}, the reconnects then time out, with no connection left to fall silent.
	 */
	static void abortConnections(final int index) throws Exception {
		final var endpoint = ENDPOINTS.get(index);
		run("ss", "-K", "-tn", "dst", endpoint);
		if (established().stream().anyMatch(connection -> connection.peer().equals(endpoint))) {
			throw new IllegalStateException("ss -K left connections to " + endpoint + " open; the kernel needs "
							+ "CONFIG_INET_DIAG_DESTROY");
		}
	}

	/** Lifts every black hole. */
	void liftBlackHoles() throws Exception {
		for (final var address : blackHoles) {
			server("rule", "del", "from", address, "blackhole");
		}
		blackHoles.clear();
	}

	/** The client addresses of the PUTs each endpoint served, by endpoint index. */
	Map<Integer, Set<String>> sources() throws Exception {
		final Map<Integer, Set<String>> sources = new HashMap<>();
		for (final var line : command("sources")) {
			final var fields = line.split(" ", -1);
			sources.put(Integer.parseInt(fields[1]), fields[2].isEmpty() ? Set.of() : Set.of(fields[2].split(",", -1)));
		}
		return sources;
	}

	/** This namespace's established connections to the stub endpoints. */
	static List<Connection> established() throws Exception {
		final List<Connection> connections = new ArrayList<>();
		for (final var line : run("ss", "-Htn", "state", "established").split("\n", -1)) {
			final var fields = line.trim().split("\\s+", -1);
			if (fields.length < 4 || !ENDPOINTS.contains(fields[3])) {
				continue;
			}
			// A socket bound to a device shows as address%device:port.
			final var local = fields[2].substring(0, fields[2].lastIndexOf(':'));
			final var percent = local.indexOf('%');
			connections.add(percent < 0
							? new Connection(local, null, fields[3])
							: new Connection(local.substring(0, percent), local.substring(percent + 1), fields[3]));
		}
		return connections;
	}

	/** This namespace's connects to the stub endpoints that are waiting for an answer. */
	static List<String> synSent() throws Exception {
		final List<String> peers = new ArrayList<>();
		for (final var line : run("ss", "-Htn", "state", "syn-sent").split("\n", -1)) {
			final var fields = line.trim().split("\\s+", -1);
			if (fields.length >= 4 && ENDPOINTS.contains(fields[3])) {
				peers.add(fields[3]);
			}
		}
		return peers;
	}

	/** Runs {@code ip} with {@code args} in this JVM's namespace, the client's. */
	static void client(final String... args) throws Exception {
		run(Stream.concat(Stream.of("ip"), Arrays.stream(args)).toArray(String[]::new));
	}

	@Override
	public void close() throws Exception {
		try {
			commands.close();
		} catch (final IOException ignored) {
			// The stub already exited.
		}
		if (!stub.waitFor(5, TimeUnit.SECONDS)) {
			stub.destroyForcibly().waitFor(5, TimeUnit.SECONDS);
		}
		// The server namespace went with the stub, and each veth with its peer there.
		deleteQuietly(NIC_A);
		deleteQuietly(NIC_B);
	}

	private void server(final String... args) throws Exception {
		run(Stream.concat(Stream.of("nsenter", "-t", Long.toString(stub.pid()), "-n", "ip"), Arrays.stream(args))
						.toArray(String[]::new));
	}

	/** Sends one command to the stub and returns the lines it printed before {@code ok}. */
	private List<String> command(final String command) throws Exception {
		commands.write(command);
		commands.newLine();
		commands.flush();
		final List<String> lines = new ArrayList<>();
		while (true) {
			final var line = replies.poll(COMMAND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			if (line == null || line.equals("exited") || line.startsWith("error")) {
				throw new IllegalStateException("stub command '" + command + "' failed: " + line);
			}
			if (line.equals("ok")) {
				return lines;
			}
			lines.add(line);
		}
	}

	private void await(final String expected) throws InterruptedException {
		final var line = replies.poll(COMMAND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
		if (!expected.equals(line)) {
			throw new IllegalStateException("the stub printed " + line + " instead of " + expected);
		}
	}

	/** Runs a command, returning its output; a non-zero exit throws with the output. */
	private static String run(final String... command) throws IOException, InterruptedException {
		final var process = new ProcessBuilder(command).redirectErrorStream(true).start();
		final String output;
		try (var in = process.getInputStream()) {
			output = new String(in.readAllBytes(), StandardCharsets.UTF_8);
		}
		if (!process.waitFor(COMMAND_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
			process.destroyForcibly();
			throw new IllegalStateException(String.join(" ", command) + " did not finish");
		}
		if (process.exitValue() != 0) {
			throw new IllegalStateException(String.join(" ", command) + " exited with " + process.exitValue() + ": " + output.trim());
		}
		return output;
	}

	private static void deleteQuietly(final String link) {
		try {
			run("ip", "link", "del", link);
		} catch (final IOException | IllegalStateException ignored) {
			// Already gone.
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	private static Path conf(final String nic, final String sysctl) {
		return Path.of("/proc/sys/net/ipv4/conf", nic, sysctl);
	}

	private static int sysctl(final Path path) throws IOException {
		return Integer.parseInt(Files.readString(path).trim());
	}

	static String host(final String endpoint) {
		return endpoint.substring(0, endpoint.lastIndexOf(':'));
	}

	/** The endpoints by index, joined for messages. */
	static String describe(final List<Integer> indexes) {
		return indexes.stream().map(ENDPOINTS::get).collect(Collectors.joining(", "));
	}
}
