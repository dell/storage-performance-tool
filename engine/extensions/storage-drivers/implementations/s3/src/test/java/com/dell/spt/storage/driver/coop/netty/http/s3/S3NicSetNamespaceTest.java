package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.ITEM_SIZE;
import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.dataOp;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NetnsTopology.ADDRESS_A;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NetnsTopology.ADDRESS_B;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NetnsTopology.ENDPOINTS;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NetnsTopology.NIC_A;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NetnsTopology.NIC_B;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.stoppedRows;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.totals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import com.dell.spt.base.nic.NicSetResources;
import com.dell.spt.storage.driver.coop.netty.nic.NicBinding;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetConnectionPool;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetSettings;
import com.github.akurilov.confuse.Config;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelOption;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.unix.RawUnixChannelOption;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

/**
 * Real S3 drivers with a NIC set over real NICs: two veth ends on one subnet in this JVM's network
 * namespace, the device binding, and four stub endpoints in a server namespace (see
 * {@link NetnsTopology}). The NICs pair with the endpoints as {@code sptc0} to endpoints 0 and 2 and
 * {@code sptc1} to endpoints 1 and 3.
 *
 * <p>Tagged {@code netns}: the default test task leaves it out, and {@code netnsTest} runs it under
 * {@code engine/tools/netns-test.sh} or in CI's privileged container. It runs without
 * {@code CAP_NET_RAW}, as a non-root process does, and fails rather than skips when the environment
 * is missing.
 */
@Tag("netns")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(120)
final class S3NicSetNamespaceTest {

	/** {@code CAP_NET_RAW}'s bit in the capability sets. */
	private static final int CAP_NET_RAW = 13;
	private static final int TIMEOUT_MILLIS = 5_000;
	private static final String NO_ROUTE_ENDPOINT = "198.51.100.20:9000";
	private static final String NO_ROUTE_PREFIX = "198.51.100.0/24";
	/** On the client subnet, where nothing answers. */
	private static final String ABSENT_GATEWAY = "192.0.2.99";

	// The reconnect storm (P5).
	private static final int STORM_CONCURRENCY = 32;
	private static final int STORM_OUTSTANDING = 64;
	private static final long WARM_UP_MILLIS = 2_000;
	private static final long WINDOW_MILLIS = 10_000;
	/** Feeding goes on past the window so the dispatches at its end complete. */
	private static final long DRAIN_MILLIS = 300;
	private static final int STORM_RUNS = 3;
	private static final double MIN_HEALTHY_SHARE = 0.8;
	private static final long MAX_HEALTHY_GAP_MICROS = 250_000;
	private static final long MAX_FAILING_CONNECTS = 8;
	private static final long SLOW_SETUP_NANOS = TimeUnit.SECONDS.toNanos(1);

	private NetnsTopology topology;
	private final List<AutoCloseable> resources = new ArrayList<>();
	private List<Window> baseline;

	private enum Failure {
		NONE,
		/** Connects are refused; the endpoint's connections close. */
		REFUSED,
		/** Connects time out; the endpoint's connections are aborted and its peer answers nothing. */
		BLACK_HOLED
	}

	/** One completed operation of a storm run; times in the operations' microseconds. */
	private record Completion(String endpoint, Operation.Status status, long reqTimeStart, long respTimeDone,
					long latencyNanos) {}

	/** One storm run's measurements over its window. */
	private record Window(Map<String, Integer> completions, long longestHealthyGapMicros, Map<String, Long> failingConnects,
					int slowSetupFailures) {

		int completions(final List<String> endpoints) {
			return endpoints.stream().mapToInt(endpoint -> completions.getOrDefault(endpoint, 0)).sum();
		}
	}

	private record Run(S3NicSetDriver<DataItem, Operation<DataItem>> driver, EndpointSelectionTestSupport.Results results) {

		Operation<?> execute(final Operation<DataItem> op) throws Exception {
			assertTrue(driver.put(op));
			return results.await();
		}

		NicSetConnectionPool pool() {
			return driver.connectionPool();
		}
	}

	@BeforeAll
	void createTopology() throws Exception {
		assertTrue(Epoll.isAvailable(), "the network-namespace tests need the epoll transport");
		if (hasNetRaw()) {
			fail("This JVM has CAP_NET_RAW, so a second SO_BINDTODEVICE would succeed and a non-root binding is not "
							+ "tested; drop it with --cap-drop NET_RAW or engine/tools/netns-test.sh");
		}
		topology = NetnsTopology.create();
	}

	@AfterAll
	void closeTopology() throws Exception {
		if (topology != null) {
			topology.close();
		}
	}

	@AfterEach
	void stopDrivers() throws Exception {
		try {
			for (var i = resources.size() - 1; i >= 0; i--) {
				resources.get(i).close();
			}
		} finally {
			resources.clear();
			topology.liftBlackHoles();
			for (var i = 0; i < ENDPOINTS.size(); i++) {
				topology.serve(i);
			}
		}
		awaitNoNicIoThreads();
	}

	@Test
	void eachConnectionLeavesOnItsPathsNicWithThatNicsAddress() throws Exception {
		final var run = started(ENDPOINTS, List.of(NIC_A, NIC_B), 8);

		assertEquals(Map.of(NIC_A, List.of(ENDPOINTS.get(0), ENDPOINTS.get(2)), NIC_B, List.of(ENDPOINTS.get(1), ENDPOINTS.get(3))),
						paths(run.pool()));
		assertEquals(new NicSetResources(1, 1, List.of(NIC_A, NIC_B)), run.driver().nicSetResources(), "the step's NIC set report");
		final var opened = bound();
		assertEquals(8, opened.size(), () -> "pre-opened before the first operation: " + opened);
		for (final var connection : opened) {
			final var nic = ENDPOINTS.indexOf(connection.peer()) % 2 == 0 ? NIC_A : NIC_B;
			assertEquals(nic, connection.device(), connection::toString);
			assertEquals(NIC_A.equals(nic) ? ADDRESS_A : ADDRESS_B, connection.source(), connection::toString);
		}

		final long sentA = txBytes(NIC_A);
		final long sentB = txBytes(NIC_B);
		for (var i = 0; i < 8; i++) {
			final var result = run.execute(dataOp(OpType.CREATE, "bound-" + i));
			assertEquals(Operation.Status.SUCC, result.status());
		}

		// Two PUTs per endpoint, each with its 1 KiB body, left on the endpoint's NIC.
		assertTrue(txBytes(NIC_A) - sentA >= 4L * ITEM_SIZE, NIC_A + " carried its endpoints' bodies");
		assertTrue(txBytes(NIC_B) - sentB >= 4L * ITEM_SIZE, NIC_B + " carried its endpoints' bodies");
		final var sources = topology.sources();
		for (var i = 0; i < ENDPOINTS.size(); i++) {
			assertEquals(Set.of(i % 2 == 0 ? ADDRESS_A : ADDRESS_B), sources.get(i), "PUT sources of " + ENDPOINTS.get(i));
		}
		assertEquals(opened.size(), bound().size(), "the operations reused the pre-opened connections");
		assertEquals(8, run.pool().state().stream().flatMap(nic -> nic.paths().stream())
						.mapToLong(NicSetConnectionPool.PathState::connectsStarted).sum());
	}

	@Test
	void preOpenedConnectionsFollowEachNicsShareOfTheConcurrencyLimit() throws Exception {
		// P >= N: floor(P/N) per NIC, one more for the first P mod N; P < N: one per NIC;
		// unlimited: one per Path. Within a NIC the connections spread over its Paths in rotation.
		final Map<Integer, List<Integer>> expected = new TreeMap<>(Map.of(
						8, List.of(2, 2, 2, 2),
						3, List.of(1, 1, 1, 0),
						1, List.of(1, 1, 0, 0),
						0, List.of(1, 1, 1, 1)));
		for (final var limit : expected.entrySet()) {
			final var run = started(ENDPOINTS, List.of(NIC_A, NIC_B), limit.getKey());
			final var perEndpoint = bound().stream()
							.collect(Collectors.groupingBy(NetnsTopology.Connection::peer, Collectors.counting()));
			final var counts = ENDPOINTS.stream().map(endpoint -> perEndpoint.getOrDefault(endpoint, 0L).intValue()).toList();
			assertEquals(limit.getValue(), counts, "pre-opened per endpoint with concurrency limit " + limit.getKey());
			run.driver().close();
			waitFor(() -> bound().isEmpty(), "the driver closed its connections");
		}
	}

	@Test
	void aPathWithNoAnsweringRouteFailsTheStartNamingTheNicAndTheEndpoint() throws Exception {
		NetnsTopology.client("route", "add", NO_ROUTE_PREFIX, "via", ABSENT_GATEWAY, "dev", NIC_B);
		try {
			final var driver = created(List.of(NO_ROUTE_ENDPOINT), List.of(NIC_B), 2);

			final var thrown = assertThrows(RuntimeException.class, driver::start);

			final var terminal = RunTerminalException.find(thrown);
			assertNotNull(terminal, () -> "not a run terminal failure: " + thrown);
			assertEquals(IntegrityTerminalException.Category.EXECUTION, terminal.category());
			final var message = terminal.getMessage();
			assertTrue(message.contains("NIC " + NIC_B + " (" + ADDRESS_B + ") cannot connect to endpoint " + NO_ROUTE_ENDPOINT),
							message);
			assertTrue(message.contains("no route to host"), message);
			awaitNoNicIoThreads();
		} finally {
			NetnsTopology.client("route", "del", NO_ROUTE_PREFIX);
		}
	}

	@Test
	void theDeviceBindingNeedsNoNetRawBecauseEachSocketSetsItOnce() throws Exception {
		// Without CAP_NET_RAW the kernel refuses to change a socket's device once set, so a binding
		// applied twice would fail its connect.
		final var name = NIC_A.getBytes(StandardCharsets.US_ASCII);
		final var option = new RawUnixChannelOption("SO_BINDTODEVICE", NicBinding.SOL_SOCKET, NicBinding.SO_BINDTODEVICE,
						name.length);
		final var channel = new EpollSocketChannel();
		try {
			assertTrue(channel.config().setOption(option, ByteBuffer.wrap(name)));
			final var second = assertThrows(ChannelException.class, () -> channel.config().setOption(option, ByteBuffer.wrap(name)));
			assertTrue(messages(second).contains("not permitted"), () -> "not EPERM: " + messages(second));
		} finally {
			channel.unsafe().closeForcibly();
		}

		final var run = started(ENDPOINTS, List.of(NIC_A, NIC_B), 4);
		for (var i = 0; i < 8; i++) {
			assertEquals(Operation.Status.SUCC, run.execute(dataOp(OpType.CREATE, "once-" + i)).status());
		}
		assertTrue(bound().stream().allMatch(connection -> connection.device() != null), () -> "unbound: " + bound());
	}

	@Test
	void theNioTransportIsRejectedBeforeTheDriverExists() {
		final var storage = storage(ENDPOINTS, List.of(NIC_A, NIC_B), 2);
		storage.val("net-transport", "nio");

		final var thrown = assertThrows(IllegalConfigurationException.class,
						() -> new S3StorageDriverExtension<>().create("netns", null, storage, false, 1));

		assertTrue(thrown.getMessage().contains("needs the epoll transport"), thrown.getMessage());
	}

	@Test
	void pooledConnectionsKeepTheReleasesSocketOptions() throws Exception {
		final var storage = storage(ENDPOINTS, List.of(NIC_A, NIC_B), 2);
		storage.val("net-rcvBuf", 65_536);
		storage.val("net-sndBuf", 65_536);
		storage.val("net-tcpNoDelay", false);
		final var run = started(storage);

		final var channel = run.pool().lease();
		final var reference = new EpollSocketChannel();
		try {
			// The kernel reports double a requested size; compare with a socket given the same sizes.
			reference.config().setReceiveBufferSize(65_536);
			reference.config().setSendBufferSize(65_536);
			assertEquals(reference.config().getReceiveBufferSize(), channel.config().getOption(ChannelOption.SO_RCVBUF));
			assertEquals(reference.config().getSendBufferSize(), channel.config().getOption(ChannelOption.SO_SNDBUF));
			assertEquals(Boolean.TRUE, channel.config().getOption(ChannelOption.SO_KEEPALIVE));
			// Epoll enables TCP_NODELAY by default; turning it off shows the setting reaches the socket.
			assertEquals(Boolean.FALSE, channel.config().getOption(ChannelOption.TCP_NODELAY));
		} finally {
			reference.unsafe().closeForcibly();
			run.pool().release(channel);
		}
	}

	@Test
	void duringBackOffAnAttemptNeedingANewConnectionFailsAtOnce() throws Exception {
		final var run = started(List.of(ENDPOINTS.get(0)), List.of(NIC_A), 2);
		silence(run, 0);

		// The first connect times out, which starts the back-off.
		final var first = run.execute(dataOp(OpType.CREATE, "first"));
		assertEquals(Operation.Status.FAIL_IO, first.status());
		assertEquals(0, first.reqTimeStart(), "the request was never sent");
		assertTrue(path(run, 0).inBackoff());
		// After the 100 ms interval one trial connect starts; it stays pending.
		Thread.sleep(150);
		final long before = connectsStarted(run, 0);
		assertTrue(run.driver().put(dataOp(OpType.CREATE, "trial")));
		waitFor(() -> connectsStarted(run, 0) == before + 1, "the trial connect started");

		final long began = System.nanoTime();
		for (var i = 0; i < 10; i++) {
			final var result = run.execute(dataOp(OpType.CREATE, "rejected-" + i));
			assertEquals(Operation.Status.FAIL_IO, result.status());
			assertEquals(0, result.reqTimeStart());
		}
		final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - began);

		assertTrue(millis < 1_000, () -> "ten rejected attempts took " + millis + " ms; a connect waits " + TIMEOUT_MILLIS);
		assertEquals(before + 1, connectsStarted(run, 0), "no connect started besides the trial");
		// Only the PUT before the silence was sent; the trial ends by its timeout or by the stop.
		final var totals = totals(stoppedRows(run.driver(), run.driver()));
		assertEquals(List.of(13L, 1L, 0L, (long) ITEM_SIZE), List.of(totals.get(0), totals.get(1), totals.get(4), totals.get(5)),
						totals::toString);
		assertTrue(totals.get(2) >= 11 && totals.get(2) + totals.get(3) == 12, totals::toString);
	}

	@Test
	void stopDuringAPendingConnectNeitherWaitsForItNorStartsAnother() throws Exception {
		final var run = started(List.of(ENDPOINTS.get(0)), List.of(NIC_A), 2);
		silence(run, 0);
		assertTrue(run.driver().put(dataOp(OpType.CREATE, "pending")));
		waitFor(() -> run.pool().pendingCount() == 1 && synSent().size() == 1, "the connect is pending");
		final long connects = connectsStarted(run, 0);

		final long began = System.nanoTime();
		run.driver().stop();
		final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - began);

		assertTrue(millis < 1_000, () -> "stop took " + millis + " ms; the connect timeout is " + TIMEOUT_MILLIS);
		assertEquals(0, run.pool().pendingCount());
		waitFor(() -> synSent().isEmpty(), "the pending connect was cancelled");
		Thread.sleep(500);
		assertEquals(connects, connectsStarted(run, 0), "no connect started after admission closed");
		assertTrue(synSent().isEmpty());
	}

	@Test
	void withoutALimitASilentPathHoldsOnlyItsNicsWaitingAttemptsAndAStopRecoversThem() throws Exception {
		final var run = started(ENDPOINTS.subList(0, 2), List.of(NIC_A, NIC_B), 0);
		silence(run, 0);
		final var admission = run.pool().admission();
		final long connects = connectsStarted(run, 0);

		for (var i = 0; i < 40; i++) {
			assertTrue(run.driver().put(dataOp(OpType.CREATE, "unlimited-" + i)));
		}

		// The rotation alternates until the silent Path's NIC holds its bound, then skips it.
		final var waiting = NicSetSettings.UNLIMITED_WAITING_LIMIT;
		for (var i = 0; i < 40 - waiting; i++) {
			final var result = run.results().await();
			assertEquals(Operation.Status.SUCC, result.status());
			assertEquals(ENDPOINTS.get(1), result.nodeAddr(), "only the answering Path serves");
		}
		waitFor(() -> admission.count(0) == waiting && run.pool().pendingCount() == waiting && synSent().size() == waiting,
						"the silent Path's NIC holds its bound of connects");
		assertEquals(0, admission.count(1));
		assertEquals(connects + waiting, connectsStarted(run, 0), "one connect per waiting attempt, no more");
		assertNull(run.results().poll(300), "the waiting attempts neither complete nor fail before their timeout");

		// The step's stop: admission closes and the queue is recovered before the driver stops.
		run.driver().closeAdmission();
		final var recovered = run.driver().recoverQueuedOperations();
		assertEquals(waiting, recovered.size(), "the waiting operations are still queued");
		final var lifecycle = run.driver().operationLifecycle();
		recovered.forEach(op -> assertEquals(OperationLifecycleState.UNATTEMPTED, lifecycle.stateOf(op)));
		run.driver().stop();

		waitFor(() -> admission.count(0) == 0 && run.pool().pendingCount() == 0, "the cancelled connects released their NIC");
		waitFor(() -> synSent().isEmpty(), "the pending connects were cancelled");
		assertNull(run.results().poll(300), "a recovered operation publishes no result");
		assertEquals(0, lifecycle.counters().failed());
		assertEquals(0, lifecycle.snapshot().inFlight());
	}

	@Test
	@Timeout(300)
	void aRefusingPathLeavesTheHealthyPathsServing() throws Exception {
		storm(Failure.REFUSED, List.of(2));
	}

	@Test
	@Timeout(300)
	void twoRefusingPathsOneOnEachNicLeaveTheHealthyPathsServing() throws Exception {
		storm(Failure.REFUSED, List.of(2, 3));
	}

	@Test
	@Timeout(300)
	void aBlackHoledPathLeavesTheHealthyPathsServing() throws Exception {
		storm(Failure.BLACK_HOLED, List.of(2));
	}

	@Test
	@Timeout(300)
	void twoBlackHoledPathsOneOnEachNicLeaveTheHealthyPathsServing() throws Exception {
		storm(Failure.BLACK_HOLED, List.of(2, 3));
	}

	/**
	 * The reconnect storm (P5): {@value #STORM_RUNS} runs of a {@value #WINDOW_MILLIS} ms window that
	 * starts as {@code failing} fail, after a {@value #WARM_UP_MILLIS} ms warm-up. The healthy Paths'
	 * median completions stay at least {@value #MIN_HEALTHY_SHARE} of the same Paths' median without a
	 * failure, no healthy Path waits more than {@value #MAX_HEALTHY_GAP_MICROS} µs for a dispatch, and
	 * a failing Path starts at most {@value #MAX_FAILING_CONNECTS} connects.
	 */
	private void storm(final Failure failure, final List<Integer> failing) throws Exception {
		final var failingEndpoints = failing.stream().map(ENDPOINTS::get).toList();
		final var healthy = ENDPOINTS.stream().filter(endpoint -> !failingEndpoints.contains(endpoint)).toList();
		if (baseline == null) {
			final List<Window> runs = new ArrayList<>();
			for (var i = 0; i < STORM_RUNS; i++) {
				runs.add(window(Failure.NONE, List.of()));
			}
			baseline = runs;
		}
		final List<Window> runs = new ArrayList<>();
		for (var i = 0; i < STORM_RUNS; i++) {
			runs.add(window(failure, failing));
		}

		final long expected = median(baseline, window -> window.completions(healthy));
		final long served = median(runs, window -> window.completions(healthy));
		final var report = failure + " " + failingEndpoints + ": healthy completions " + served + " vs baseline " + expected
						+ "; runs " + runs + "; baseline " + baseline;
		System.out.println("storm " + report);
		assertTrue(served >= MIN_HEALTHY_SHARE * expected, report);
		for (final var run : runs) {
			assertTrue(run.longestHealthyGapMicros() <= MAX_HEALTHY_GAP_MICROS, report);
			assertTrue(run.failingConnects().values().stream().allMatch(connects -> connects <= MAX_FAILING_CONNECTS), report);
			final long connects = run.failingConnects().values().stream().mapToLong(Long::longValue).sum();
			assertTrue(run.slowSetupFailures() <= connects, () -> "only an attempt that started a connect waits on it: " + report);
		}
	}

	private Window window(final Failure failure, final List<Integer> failing) throws Exception {
		final var failingEndpoints = failing.stream().map(ENDPOINTS::get).toList();
		final var healthy = ENDPOINTS.stream().filter(endpoint -> !failingEndpoints.contains(endpoint)).toList();
		final var run = started(storage(ENDPOINTS, List.of(NIC_A, NIC_B), STORM_CONCURRENCY));
		final var feeder = new Feeder(run);
		try {
			feeder.feedUntil(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WARM_UP_MILLIS));
			final long injected = System.nanoTime();
			final var before = connectsByEndpoint(run);
			for (final var index : failing) {
				if (failure == Failure.REFUSED) {
					topology.refuse(index);
				} else {
					topology.blackHole(index);
					NetnsTopology.abortConnections(index);
				}
			}
			final long end = injected + TimeUnit.MILLISECONDS.toNanos(WINDOW_MILLIS);
			feeder.feedUntil(end);
			final var after = connectsByEndpoint(run);
			feeder.feedUntil(end + TimeUnit.MILLISECONDS.toNanos(DRAIN_MILLIS));
			final Map<String, Long> failingConnects = new TreeMap<>();
			failingEndpoints.forEach(endpoint -> failingConnects.put(endpoint, after.get(endpoint) - before.get(endpoint)));
			return feeder.window(micros(injected), micros(end), healthy, failingConnects);
		} finally {
			topology.liftBlackHoles();
			for (final var index : failing) {
				topology.serve(index);
			}
			run.driver().close();
			awaitNoNicIoThreads();
		}
	}

	/** Keeps at most {@value #STORM_OUTSTANDING} operations outstanding from one thread and records each result. */
	private static final class Feeder {

		private final Run run;
		/** By item: the driver publishes a copy of each operation, with the same item, as its result. */
		private final IdentityHashMap<Object, Long> putNanos = new IdentityHashMap<>();
		private final List<Completion> completions = new ArrayList<>();
		private int next;

		Feeder(final Run run) {
			this.run = run;
		}

		void feedUntil(final long deadlineNanos) throws InterruptedException {
			while (System.nanoTime() - deadlineNanos < 0) {
				while (putNanos.size() < STORM_OUTSTANDING) {
					final var op = dataOp(OpType.CREATE, "storm-" + next++);
					putNanos.put(op.item(), System.nanoTime());
					assertTrue(run.driver().put(op), "the driver accepts the operation");
				}
				var result = run.results().poll(1);
				while (result != null) {
					final var put = putNanos.remove(result.item());
					assertNotNull(put, "a result of an operation this feeder put");
					completions.add(new Completion(result.nodeAddr(), result.status(), result.reqTimeStart(), result.respTimeDone(),
									System.nanoTime() - put));
					result = run.results().poll(0);
				}
			}
		}

		Window window(final long fromMicros, final long toMicros, final List<String> healthy,
						final Map<String, Long> failingConnects) {
			final Map<String, Integer> done = new TreeMap<>();
			final List<Long> dispatches = new ArrayList<>(List.of(fromMicros, toMicros));
			var slowSetupFailures = 0;
			for (final var completion : completions) {
				final var endpoint = completion.endpoint();
				final var sent = completion.reqTimeStart();
				if (completion.status() == Operation.Status.SUCC && completion.respTimeDone() >= fromMicros
								&& completion.respTimeDone() <= toMicros) {
					done.merge(endpoint, 1, Integer::sum);
				}
				if (sent >= fromMicros && sent <= toMicros && healthy.contains(endpoint)) {
					dispatches.add(sent);
				}
				if (completion.status() == Operation.Status.FAIL_IO && sent == 0 && completion.latencyNanos() > SLOW_SETUP_NANOS) {
					slowSetupFailures++;
				}
			}
			Collections.sort(dispatches);
			var longestGap = 0L;
			for (var i = 1; i < dispatches.size(); i++) {
				longestGap = Math.max(longestGap, dispatches.get(i) - dispatches.get(i - 1));
			}
			return new Window(done, longestGap, failingConnects, slowSetupFailures);
		}
	}

	private Run started(final List<String> endpoints, final List<String> interfaces, final int concurrency) throws Exception {
		return started(storage(endpoints, interfaces, concurrency));
	}

	private Run started(final Config storage) throws Exception {
		final var driver = created(storage);
		final var results = new EndpointSelectionTestSupport.Results();
		driver.operationResultOutput(results);
		driver.start();
		return new Run(driver, results);
	}

	private S3NicSetDriver<DataItem, Operation<DataItem>> created(final List<String> endpoints, final List<String> interfaces,
					final int concurrency) throws Exception {
		return created(storage(endpoints, interfaces, concurrency));
	}

	private S3NicSetDriver<DataItem, Operation<DataItem>> created(final Config storage) throws Exception {
		// The product's environment: this namespace's sysfs, the system resolver and the device binding.
		final S3NicSetDriver<DataItem, Operation<DataItem>> driver = S3NicSetDriver.create("netns-test",
						new SeedDataInput(1, ITEM_SIZE, 1, true), storage, false, 16,
						NicSetSettings.fromStorage(storage).orElseThrow());
		resources.add(driver);
		return driver;
	}

	private static Config storage(final List<String> endpoints, final List<String> interfaces, final int concurrency) {
		final Config root = S3StorageDriverTest.baseConfig(false, 4, false, null, endpoints.get(0));
		root.val("storage-net-transport", "epoll");
		root.val("storage-net-interfaces", interfaces);
		root.val("storage-net-node-addrs", endpoints);
		root.val("storage-net-timeoutMilliSec", TIMEOUT_MILLIS);
		// The shipped default; the base fixture's Nagle holds each small PUT body for a delayed ACK.
		root.val("storage-net-tcpNoDelay", true);
		root.val("storage-net-http-headers", Map.of());
		root.val("storage-driver-limit-concurrency", concurrency);
		root.val("storage-driver-threads", 1);
		return root.configVal("storage");
	}

	/**
	 * Makes endpoint {@code index} silent for a driver with one Path to it: its connections close,
	 * and a connect to it then times out. One operation succeeds first, so the bucket check, which
	 * the first operation makes on the kernel route and waits for, is done.
	 */
	private void silence(final Run run, final int index) throws Exception {
		assertEquals(Operation.Status.SUCC, run.execute(dataOp(OpType.CREATE, "before-silence")).status());
		topology.refuse(index);
		waitFor(() -> run.pool().state().get(0).open() == 0, "the closed connections are dropped");
		topology.blackHole(index);
	}

	private static Map<String, List<String>> paths(final NicSetConnectionPool pool) {
		return pool.state().stream().collect(Collectors.toMap(NicSetConnectionPool.NicState::nic,
						nic -> nic.paths().stream().map(NicSetConnectionPool.PathState::endpoint).toList()));
	}

	private static NicSetConnectionPool.PathState path(final Run run, final int endpoint) {
		return run.pool().state().stream().flatMap(nic -> nic.paths().stream())
						.filter(path -> path.endpoint().equals(ENDPOINTS.get(endpoint)))
						.findFirst()
						.orElseThrow();
	}

	private static long connectsStarted(final Run run, final int endpoint) {
		return path(run, endpoint).connectsStarted();
	}

	private static Map<String, Long> connectsByEndpoint(final Run run) {
		return run.pool().state().stream().flatMap(nic -> nic.paths().stream())
						.collect(Collectors.toMap(NicSetConnectionPool.PathState::endpoint, NicSetConnectionPool.PathState::connectsStarted));
	}

	/** The established connections bound to a device; the bucket check uses the kernel route. */
	private static List<NetnsTopology.Connection> bound() {
		try {
			return NetnsTopology.established().stream().filter(connection -> connection.device() != null).toList();
		} catch (final Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private static List<String> synSent() {
		try {
			return NetnsTopology.synSent();
		} catch (final Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private static long txBytes(final String nic) throws Exception {
		return Long.parseLong(Files.readString(Path.of("/sys/class/net", nic, "statistics", "tx_bytes")).trim());
	}

	private static long micros(final long nanos) {
		return Operation.START_OFFSET_MICROS + nanos / 1_000;
	}

	private static long median(final List<Window> windows, final Function<Window, Integer> value) {
		final var values = windows.stream().map(value).sorted().toList();
		return values.get(values.size() / 2);
	}

	private static String messages(final Throwable thrown) {
		final var text = new StringBuilder();
		for (var cause = thrown; cause != null; cause = cause.getCause()) {
			text.append(cause).append("; ");
		}
		return text.toString();
	}

	private static boolean hasNetRaw() throws Exception {
		for (final var line : Files.readAllLines(Path.of("/proc/self/status"))) {
			if (line.startsWith("CapEff:")) {
				return new BigInteger(line.substring("CapEff:".length()).trim(), 16).testBit(CAP_NET_RAW);
			}
		}
		throw new IllegalStateException("no CapEff in /proc/self/status");
	}

	private static void waitFor(final BooleanSupplier condition, final String what) throws InterruptedException {
		final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		assertTrue(condition.getAsBoolean(), what);
	}

	private static List<String> nicIoThreads(final String nic) {
		return Thread.getAllStackTraces().keySet().stream()
						.filter(Thread::isAlive)
						.map(Thread::getName)
						.filter(name -> name.startsWith("ioWorker-" + nic))
						.toList();
	}

	private static void awaitNoNicIoThreads() throws InterruptedException {
		waitFor(() -> nicIoThreads(NIC_A).isEmpty() && nicIoThreads(NIC_B).isEmpty(),
						"the NICs' event loops stopped: " + nicIoThreads(NIC_A) + nicIoThreads(NIC_B));
	}
}
