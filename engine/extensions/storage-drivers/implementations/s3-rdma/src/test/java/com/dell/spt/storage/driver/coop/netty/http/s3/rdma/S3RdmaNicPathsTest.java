package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.BUCKET;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.NICS;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.THRESHOLD;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.closedRows;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.op;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.rdma;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.BundledDefaultsProvider;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.control.run.RunImpl;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.load.step.ScenarioUtil;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.nic.NicPathRows;
import com.dell.spt.base.nic.NicPathsArtifact;
import com.dell.spt.base.storage.driver.PoolDemandDriver;
import com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.Run;
import com.github.akurilov.confuse.Config;
import io.netty.channel.epoll.Epoll;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The per-path CSV's schema 2 for the real S3-RDMA driver, on the stub transport (item 18, E8, K3). */
@Timeout(90)
final class S3RdmaNicPathsTest {

	private final RdmaNicLoopback stub = new RdmaNicLoopback();
	private final List<AutoCloseable> resources = new ArrayList<>();

	@BeforeEach
	void requireEpoll() {
		assumeTrue(Epoll.isAvailable(), "a NIC set needs the epoll transport");
	}

	@AfterEach
	void stop() throws Exception {
		stub.release();
		for (var i = resources.size() - 1; i >= 0; i--) {
			resources.get(i).close();
		}
		stub.close();
	}

	private Run started(final Config root) throws Exception {
		final var driver = stub.driver(root);
		resources.add(driver);
		driver.directMemoryLimitBytes = 64L << 30;
		return RdmaNicLoopback.started(driver);
	}

	private static List<NicPathRows.Row> rows(final NicPathRows rows, final NicPathRows.Kind kind) {
		return rows.rows().stream().filter(row -> row.kind() == kind).toList();
	}

	private static long sum(final List<NicPathRows.Row> rows, final java.util.function.ToLongFunction<NicPathRows.Row> column) {
		return rows.stream().mapToLong(column).sum();
	}

	@Test
	void eachSuccessCountsByHowItsPayloadMovedAndAnAttemptWithNoPayloadByNeither() throws Exception {
		final var run = started(RdmaNicLoopback.root(rdma(true, true, 1), stub.listeners(1), NICS.subList(0, 1), 1));

		// Two payloads over RDMA.
		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.CREATE, "rdma")).status());
		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.READ, "rdma")).status());
		// Three over HTTP: below the threshold, declined, and after a per-operation fallback.
		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.CREATE, "below", THRESHOLD - 1)).status());
		stub.decline = request -> true;
		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.READ, "rdma")).status());
		stub.decline = request -> false;
		stub.transports.get(0).setFailTokenGeneration(true);
		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.CREATE, "fallback")).status());
		stub.transports.get(0).setFailTokenGeneration(false);
		// No payload at all.
		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.DELETE, "fallback")).status());

		final var rows = closedRows(run.driver());
		final var paths = rows(rows, NicPathRows.Kind.PATH);
		assertEquals(6, sum(paths, NicPathRows.Row::succeeded));
		assertEquals(2, sum(paths, NicPathRows.Row::rdmaSucceeded));
		assertEquals(3, sum(paths, NicPathRows.Row::httpSucceeded));
		// The whole driver agrees, as the flat statistics count the same attempts.
		assertEquals(2, run.nic(0).stats.rdmaTransferred.sum());
	}

	@Test
	void theRdmaColumnsAreNumbersOnEveryRowAndThePoolColumnsOnlyOnNicRows() throws Exception {
		final var run = started(RdmaNicLoopback.root(rdma(false, true, 1), stub.listeners(2), NICS.subList(0, 2), 2));
		run.executeAlone(op(OpType.CREATE, "one"));
		final var rows = closedRows(run.driver());
		for (final var row : rows.rows()) {
			assertNotNull(row.rdmaSucceeded(), row.toString());
			assertNotNull(row.httpSucceeded(), row.toString());
			if (row.kind() == NicPathRows.Kind.NIC) {
				assertNotNull(row.pool(), row.toString());
			} else {
				assertNull(row.pool(), row.toString());
			}
		}
	}

	@Test
	void thePoolColumnsMatchTheFlatStatisticsSummedOverTheNics() throws Exception {
		final var run = started(RdmaNicLoopback.root(rdma(false, true, 1), stub.listeners(2), NICS.subList(0, 2), 2));
		run.driver().startPrewarm(List.of(new PoolDemandDriver.PrewarmOrder(NICS.get(0), 1, 1 << 20))).get(0).get(30, TimeUnit.SECONDS);
		for (var i = 0; i < 4; i++) {
			run.executeAlone(op(OpType.CREATE, "object-" + i));
		}
		// The two counts a loopback run cannot reach cheaply: their mapping is what matters here.
		run.nic(0).pool.exhausted.add(3);
		run.nic(1).pool.unpooled.add(2);

		final var rows = closedRows(run.driver());
		final var nics = rows(rows, NicPathRows.Kind.NIC);
		assertEquals(2, nics.size());
		final var counters = run.driver().driverStats().counters();
		assertEquals(counters.get(RdmaDriverStats.REGISTRATIONS_POOLED), sum(nics, row -> row.pool().created()));
		assertEquals(counters.get(RdmaDriverStats.POOL_HITS), sum(nics, row -> row.pool().hits()));
		assertEquals(counters.get(RdmaDriverStats.POOL_EXHAUSTED), sum(nics, row -> row.pool().exhausted()));
		assertEquals(counters.get(RdmaDriverStats.POOL_OVERSIZE), sum(nics, row -> row.pool().unpooled()));
		assertEquals(3, nics.get(0).pool().exhausted());
		assertEquals(2, nics.get(1).pool().unpooled());
		assertEquals(1, nics.get(0).pool().prewarmed());
		assertEquals(0, nics.get(1).pool().prewarmed());
		assertTrue(sum(nics, row -> row.pool().hits()) > 0, "the pre-warmed buffer was reused");
	}

	@Test
	void anRdmaPreparationFailureIsAFailedAttemptOnItsNicsRow() throws Exception {
		final var run = started(RdmaNicLoopback.root(rdma(false, true, 1), stub.listeners(1), NICS.subList(0, 1), 1));
		// The pool's registration and the per-operation one both fail, so preparation does.
		stub.transports.get(0).setFailNextRegistrations(2);
		assertEquals(Operation.Status.FAIL_IO, run.executeAlone(op(OpType.CREATE, "unprepared")).status());

		final var rows = closedRows(run.driver());
		final var nic = rows(rows, NicPathRows.Kind.NIC).get(0);
		assertEquals(1, nic.attempts());
		assertEquals(1, nic.failed());
		assertNull(nic.endpoint(), "the endpoint columns are empty");
		assertEquals("lo-a", nic.nic().iface());
		assertEquals(0, sum(rows(rows, NicPathRows.Kind.PATH), NicPathRows.Row::attempts));
	}

	@Test
	void withoutRdmaEnabledTheColumnsAreStillNumbers() throws Exception {
		final var disabled = new RdmaConfig(false, THRESHOLD, false, "auto", "", "WARN");
		final var run = started(RdmaNicLoopback.root(disabled, stub.listeners(1), NICS.subList(0, 1), 1));
		run.executeAlone(op(OpType.CREATE, "http"));
		final var rows = closedRows(run.driver());
		final var paths = rows(rows, NicPathRows.Kind.PATH);
		assertEquals(0, sum(paths, NicPathRows.Row::rdmaSucceeded));
		assertEquals(1, sum(paths, NicPathRows.Row::httpSucceeded));
		assertEquals(new NicPathRows.PoolCounts(0, 0, 0, 0, 0), rows(rows, NicPathRows.Kind.NIC).get(0).pool());
	}

	/** Ticket 11's pipeline canary, with both drivers on the same NIC names (K3). */
	@Test
	void aPipelineWritesOneFileAndEachContextsPoolColumnsMatchItsOwnDriver() throws Exception {
		final var factory = stub.new Factory();
		final var stepId = "rdma-nic-paths-" + System.nanoTime();
		runScenario(factory, stepId, """
						PipelineLoad
							.config({"load": {"step": {"limit": {"time": "30s"}}}})
							.append({"storage": {"net": {"interfaces": "lo-a,lo-b"}, "driver": {"limit": {"concurrency": 2}}}})
							.append({
								"load": {"op": {"type": "read"}},
								"storage": {"net": {"interfaces": "lo-a,lo-b"}, "driver": {"limit": {"concurrency": 4}}}
							})
							.run();
						""");
		final var drivers = factory.drivers.stream().filter(driver -> driver.openRdmaResources() != null).toList();
		assertEquals(2, drivers.size(), "one started driver per context");

		final var file = Path.of(FileManager.INSTANCE.logFileName(Loggers.NIC_PATHS.getName(), stepId));
		assertTrue(Files.exists(file), () -> "the one file: " + file);
		final var lines = Files.readAllLines(file);
		assertEquals(NicPathsArtifact.HEADER, lines.get(0));
		final var columns = lines.get(0).split(",", -1);
		// No field of this fixture holds a comma.
		final var records = lines.subList(1, lines.size()).stream().map(line -> {
			final var fields = line.split(",", -1);
			final Map<String, String> record = new HashMap<>();
			for (var i = 0; i < columns.length; i++) {
				record.put(columns[i], fields[i]);
			}
			return record;
		}).toList();
		assertEquals("end", records.get(records.size() - 1).get("row"));
		records.forEach(record -> assertEquals("2", record.get("schema_version")));

		for (var context = 0; context < drivers.size(); context++) {
			final var index = Integer.toString(context);
			final var nicRows = records.stream()
							.filter(record -> index.equals(record.get("context_index")) && "nic".equals(record.get("row")))
							.toList();
			assertEquals(List.of("lo-a", "lo-b"), nicRows.stream().map(record -> record.get("interface")).collect(Collectors.toList()));
			final var counters = drivers.get(context).driverStats().counters();
			assertEquals(counters.get(RdmaDriverStats.REGISTRATIONS_POOLED), total(nicRows, "pool_created"), "context " + index);
			assertEquals(counters.get(RdmaDriverStats.POOL_HITS), total(nicRows, "pool_hits"), "context " + index);
			assertEquals(counters.get(RdmaDriverStats.POOL_EXHAUSTED), total(nicRows, "pool_exhausted"), "context " + index);
			assertEquals(counters.get(RdmaDriverStats.POOL_OVERSIZE), total(nicRows, "pool_unpooled"), "context " + index);
		}
		// The share of each differs, so its pre-warm tells the two contexts' pools apart.
		assertEquals(2, total(records.stream()
						.filter(record -> "0".equals(record.get("context_index")) && "nic".equals(record.get("row"))).toList(), "pool_prewarmed"));
		assertEquals(4, total(records.stream()
						.filter(record -> "1".equals(record.get("context_index")) && "nic".equals(record.get("row"))).toList(), "pool_prewarmed"));
	}

	private static long total(final List<Map<String, String>> records, final String column) {
		return records.stream().mapToLong(record -> Long.parseLong(record.get(column))).sum();
	}

	private void runScenario(final RdmaNicLoopback.Factory factory, final String stepId, final String scenario) throws Exception {
		final var root = RdmaNicLoopback.root(rdma(false, true, 1), stub.listeners(2), NICS.subList(0, 2), 1);
		final Config config = ConfigUtil.merge("-", List.of(new BundledDefaultsProvider().config("-", root.schema()), root));
		config.val("run-id", 782L);
		config.val("load-step-id", stepId);
		config.val("load-step-idAutoGenerated", false);
		config.val("load-op-limit-count", 8);
		config.val("item-data-size", "4KB");
		config.val("item-output-path", BUCKET);
		config.val("item-output-delay", 0);
		config.val("item-output-queue-limit", 1_000);
		config.val("output-color", false);
		config.val("storage-driver-type", RdmaNicLoopback.DRIVER_TYPE);
		final var classLoader = Thread.currentThread().getContextClassLoader();
		final List<Extension> extensions = new ArrayList<>(Extension.load(classLoader));
		extensions.add(factory);
		try (final var metrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR)) {
			final var engine = ScenarioUtil.scriptEngineByDefault(classLoader);
			assertNotNull(engine, "the default JavaScript engine is available");
			ScenarioUtil.configure(engine, extensions, config, metrics);
			new RunImpl("RDMA NIC paths canary", scenario, engine, 782L).run();
		}
	}
}
