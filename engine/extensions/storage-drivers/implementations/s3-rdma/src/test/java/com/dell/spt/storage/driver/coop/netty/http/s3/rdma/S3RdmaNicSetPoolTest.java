package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.BUCKET;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.NICS;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.closedRows;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.op;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.rdma;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.waitFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.BundledDefaultsProvider;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.control.run.RunImpl;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.load.step.ScenarioUtil;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.nic.pool.PoolDemand;
import com.dell.spt.base.nic.pool.PoolSizeClass;
import com.dell.spt.base.storage.driver.PoolDemandDriver;
import com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.Run;
import com.github.akurilov.confuse.Config;
import io.netty.channel.epoll.Epoll;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The pool check and Pool Pre-warm of the real S3-RDMA driver on the stub transport (items 14a and
 * 14c): what is registered on which lane, a failure, a stop, and a step through the barrier.
 */
@Timeout(90)
final class S3RdmaNicSetPoolTest {

	private static final long GIB = 1L << 30;

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

	private Run started(final int nics, final int concurrency) throws Exception {
		return started(RdmaNicLoopback.root(rdma(false, true, 1), stub.listeners(2), NICS.subList(0, nics), concurrency));
	}

	private Run started(final Config root) throws Exception {
		final var driver = stub.driver(root);
		resources.add(driver);
		driver.directMemoryLimitBytes = 64 * GIB;
		return RdmaNicLoopback.started(driver);
	}

	private static List<PoolDemandDriver.PrewarmResult> await(final List<CompletableFuture<PoolDemandDriver.PrewarmResult>> running) throws Exception {
		final var results = new ArrayList<PoolDemandDriver.PrewarmResult>();
		for (final var future : running) {
			results.add(future.get(30, TimeUnit.SECONDS));
		}
		return results;
	}

	@Test
	void theLayoutGivesTheResolvedPlanAndEachNicsLimitsAgreeWithTheEstimate() throws Exception {
		final var run = started(4, 96);
		final var layout = run.driver().poolLayout();
		assertEquals(NICS, layout.nics());
		assertEquals(96, layout.concurrency());
		assertEquals(64 * GIB, layout.limitBytes());
		assertTrue(layout.failOnShortfall());
		final var estimate = PoolDemand.estimate(
						new PoolDemand.Workload(96, RdmaNicLoopback.THRESHOLD, 0, 0,
										List.of(new PoolDemand.Operation(PoolDemand.OpKind.CREATE, 0, false, new PoolDemand.Range(GIB, GIB, "item.data.size")))),
						layout.nics());
		for (var i = 0; i < NICS.size(); i++) {
			final var pool = run.nic(i).pool;
			final var limits = RdmaNicPoolLimits.of(96, 4, i, 32 * GIB, S3RdmaStorageDriver.UNLIMITED_CONCURRENCY_POOL_LIMIT);
			assertEquals(limits.minimum(), estimate.nics().get(i).minimum());
			assertEquals(limits.cap(), estimate.nics().get(i).cap());
			assertEquals(limits.cap(), pool.maxBuffersPerClass());
			assertEquals(limits.budgetBytes(), PoolDemand.nicBudgetBytes(64 * GIB, 4));
			assertEquals(limits.budgetBytes(), pool.levels().budgetBytes());
		}
		assertEquals(PoolDemand.UNLIMITED_CAP, S3RdmaStorageDriver.UNLIMITED_CONCURRENCY_POOL_LIMIT);
	}

	@Test
	void theEstimatesSizeClassesAndShareAreThePoolsOwn() {
		assertEquals(RdmaBufferPool.MIN_CLASS_BYTES, PoolSizeClass.MIN_BYTES);
		assertEquals(RdmaBufferPool.MAX_CLASS_BYTES, PoolSizeClass.MAX_BYTES);
		assertEquals(RdmaBufferPool.DIRECT_MEMORY_SHARE_DIVISOR, PoolDemand.DIRECT_MEMORY_SHARE_DIVISOR);
		for (final int size : List.of(1, 4096, 1 << 20, (1 << 20) + 1, 3 << 20, 1 << 29, (1 << 29) + 1, 1 << 30)) {
			assertEquals(RdmaBufferPool.capacityFor(size), PoolSizeClass.of(size), "size " + size);
		}
		for (final int concurrency : List.of(0, 1, 2, 3, 4, 7, 96, 97)) {
			for (final int nics : List.of(1, 2, 4)) {
				for (var index = 0; index < nics; index++) {
					final var limits = RdmaNicPoolLimits.of(concurrency, nics, index, 1L << 40, PoolDemand.UNLIMITED_CAP);
					assertEquals(limits.minimum(), PoolDemand.minimum(concurrency, nics, index), concurrency + " on " + nics);
					assertEquals(limits.cap(), PoolDemand.cap(concurrency, nics, index), concurrency + " on " + nics);
				}
			}
		}
	}

	@Test
	void eachLaneRegistersItsMinimumOnItsOwnThreadAtOnce() throws Exception {
		final var run = started(2, 4);
		final Set<String> threads = ConcurrentHashMap.newKeySet();
		final var bothLanes = new CountDownLatch(2);
		stub.beforeRegistration = () -> {
			if (threads.add(Thread.currentThread().getName())) {
				bothLanes.countDown();
			}
			try {
				// Each lane waits for the other, so a pre-warm of one lane after the other would hang here.
				assertTrue(bothLanes.await(20, TimeUnit.SECONDS), "every lane registers at once");
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		};
		final var results = await(run.driver().startPrewarm(List.of(
						new PoolDemandDriver.PrewarmOrder(NICS.get(0), 2, 4 << 20),
						new PoolDemandDriver.PrewarmOrder(NICS.get(1), 2, 4 << 20))));
		assertEquals(2, threads.size(), threads::toString);
		assertTrue(threads.contains(RdmaPreparationLane.threadName("rdma-nic-test", NICS.get(0))), threads::toString);
		for (var i = 0; i < 2; i++) {
			assertEquals(2, results.get(i).buffers());
			assertNull(results.get(i).failure());
			assertFalse(results.get(i).stopped());
			final var pool = run.nic(i).pool;
			assertEquals(2, pool.liveBuffers(), "registered only on its own NIC");
			assertEquals(2, pool.idleBuffers(), "ready for the load");
			assertEquals(2, pool.created.sum());
			assertEquals(0, pool.hits.sum(), "a pre-warm reuses nothing");
			assertEquals(2, run.nic(i).prewarmedBuffers);
		}
	}

	@Test
	void theFirstLoadReusesWhatWasPreWarmed() throws Exception {
		final var run = started(2, 2);
		await(run.driver().startPrewarm(List.of(
						new PoolDemandDriver.PrewarmOrder(NICS.get(0), 1, 1 << 20),
						new PoolDemandDriver.PrewarmOrder(NICS.get(1), 1, 1 << 20))));
		run.executeAlone(op(OpType.CREATE, "warm-0"));
		run.executeAlone(op(OpType.CREATE, "warm-1"));
		assertEquals(1, run.nic(0).pool.created.sum(), "no new buffer");
		assertEquals(1, run.nic(1).pool.created.sum(), "no new buffer");
		assertEquals(2, run.nic(0).pool.hits.sum() + run.nic(1).pool.hits.sum());
	}

	@Test
	void aRegistrationFailureSaysSoAndKeepsWhatWasRegistered() throws Exception {
		final var run = started(2, 4);
		stub.transports.get(1).setFailAfterNRegistrations(1);
		final var results = await(run.driver().startPrewarm(List.of(
						new PoolDemandDriver.PrewarmOrder(NICS.get(0), 2, 4 << 20),
						new PoolDemandDriver.PrewarmOrder(NICS.get(1), 2, 4 << 20))));
		assertNull(results.get(0).failure());
		assertEquals("buffer registration failed", results.get(1).failure());
		assertEquals(1, results.get(1).buffers());
		assertEquals(1, run.nic(1).pool.idleBuffers(), "what was registered stays");
	}

	@Test
	void anAllocationFailureSaysSo() throws Exception {
		final var run = started(1, 2);
		run.nic(0).pool.allocator = size -> {
			throw new OutOfMemoryError("Direct buffer memory");
		};
		final var results = await(run.driver().startPrewarm(List.of(new PoolDemandDriver.PrewarmOrder(NICS.get(0), 2, 4 << 20))));
		assertEquals("direct memory exhausted", results.get(0).failure());
		assertEquals(0, results.get(0).buffers());
	}

	@Test
	void aStopDuringPreWarmStopsTheLaneAndReleasesItsBuffersBeforeTheContextCloses() throws Exception {
		final var run = started(1, 2);
		final var transport = stub.transports.get(0);
		final var registering = new CountDownLatch(1);
		final var proceed = new CountDownLatch(1);
		stub.beforeRegistration = () -> {
			registering.countDown();
			try {
				proceed.await(20, TimeUnit.SECONDS);
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		};
		final var running = run.driver().startPrewarm(List.of(new PoolDemandDriver.PrewarmOrder(NICS.get(0), 8, 4 << 20)));
		assertTrue(registering.await(20, TimeUnit.SECONDS));
		final var closer = Thread.ofPlatform().start(() -> {
			try {
				run.driver().close();
			} catch (final Exception e) {
				throw new IllegalStateException(e);
			}
		});
		waitFor(() -> run.lane(0).cancelled(), "the lane is cancelled");
		proceed.countDown();
		final var result = running.get(0).get(30, TimeUnit.SECONDS);
		assertTrue(result.stopped());
		assertEquals(1, result.buffers(), "it stopped at the next buffer");
		closer.join(30_000);
		assertFalse(closer.isAlive());
		assertTrue(run.nic(0).closed());
		assertEquals(0, run.nic(0).pool.liveBuffers(), "its buffers went back and close released them");
		assertFalse(transport.isAvailable(), "the context closed after the lane ended");
	}

	@Test
	void aPreWarmAfterALaneHasStoppedReportsItStopped() throws Exception {
		final var run = started(1, 2);
		run.driver().close();
		final var result = run.driver().startPrewarm(List.of(new PoolDemandDriver.PrewarmOrder(NICS.get(0), 1, 4 << 20))).get(0).get(5, TimeUnit.SECONDS);
		assertTrue(result.stopped());
	}

	@Test
	void withoutThePoolOrWithoutRdmaTheDriverHasNoLayout() throws Exception {
		final var noPool = started(RdmaNicLoopback.root(rdma(false, false, 1), stub.listeners(2), NICS.subList(0, 2), 2));
		assertNull(noPool.driver().poolLayout(), "--rdma-buffer-pool=false");
		final var disabled = new RdmaConfig(false, RdmaNicLoopback.THRESHOLD, false, "auto", "", "WARN");
		final var off = started(RdmaNicLoopback.root(disabled, stub.listeners(2), NICS.subList(0, 2), 2));
		assertNull(off.driver().poolLayout(), "storage.rdma.enabled=false");
	}

	@Test
	void theSettingIsProvenThroughTheSchemaDefaultsAndGetter() throws Exception {
		final var root = RdmaNicLoopback.root(rdma(false, true, 1), stub.listeners(2), NICS.subList(0, 2), 2);
		final var shipped = java.nio.file.Path.of(getClass().getResource("/config/defaults-storage-s3-rdma.yaml").toURI());
		final Config defaults = ConfigUtil.loadConfig(shipped.toFile(), root.schema());
		assertEquals("fail", defaults.stringVal("storage-rdma-poolShortfall"), "the shipped default");

		root.val("storage-rdma-poolShortfall", "warn");
		final var warn = started(root);
		assertFalse(warn.driver().poolLayout().failOnShortfall());

		final var invalid = RdmaNicLoopback.root(rdma(false, true, 1), stub.listeners(2), NICS.subList(0, 2), 2);
		invalid.val("storage-rdma-poolShortfall", "maybe");
		final var refused = assertThrows(IllegalConfigurationException.class, () -> stub.driver(invalid));
		assertTrue(refused.getMessage().contains("storage.rdma.poolShortfall"), refused.getMessage());

		// Without a NIC set nothing reads it, so a bad value changes nothing on the 5.16 route.
		final var defaultRoute = new RdmaConfig(invalid.configVal("storage-rdma"));
		assertEquals("maybe", defaultRoute.getPoolShortfall());
		assertEquals("fail", new RdmaConfig(null).getPoolShortfall());
	}

	@Test
	void theCheckAndPreWarmRunInAStepThroughTheBarrierBeforeAnyObjectRequest() throws Exception {
		final var factory = stub.new Factory();
		final var stepId = "rdma-pool-step-" + System.nanoTime();
		final var outcome = runScenario(factory, stepId, "Load.config({\"storage\": {\"driver\": {\"limit\": {\"concurrency\": 2}}}}).run();\n");
		assertNull(outcome, () -> "the scenario finished: " + outcome);
		final var driver = factory.drivers.stream().filter(one -> one.openRdmaResources() != null).findFirst().orElseThrow();
		final var open = driver.openRdmaResources();
		assertEquals(1, open.nics().get(0).prewarmedBuffers, "each NIC's share of the limit");
		assertEquals(1, open.nics().get(1).prewarmedBuffers);
		assertEquals(1, open.nics().get(0).pool.created.sum(), "no buffer beyond the pre-warmed one");
		assertTrue(open.nics().get(0).pool.hits.sum() > 0, "the load reused it");
		assertNotNull(closedRows(driver));
	}

	@Test
	void aShortBudgetFailsTheStepBeforeAnyObjectOperation() throws Exception {
		final var factory = stub.new Factory();
		// 2 NICs with 1 MiB each do not fit in half of 2 MiB, split in two.
		factory.directMemoryLimitBytes = 1L << 20;
		final var stepId = "rdma-pool-short-" + System.nanoTime();
		final var outcome = runScenario(factory, stepId, "Load.config({\"storage\": {\"driver\": {\"limit\": {\"concurrency\": 2}}}}).run();\n");
		assertNotNull(outcome);
		assertTrue(outcome.contains("NIC lo-a's pool needs 1MiB"), outcome);
		assertTrue(stub.objectRequests().isEmpty(), () -> "no object request: " + stub.objectRequests());
	}

	@Test
	void aPipelineFittingEachBudgetButNotTheLimitFailsTheProcessCheck() throws Exception {
		final var factory = stub.new Factory();
		factory.directMemoryLimitBytes = 8 * GIB;
		final var stepId = "rdma-pool-pipeline-" + System.nanoTime();
		final var outcome = runScenario(factory, stepId, """
						PipelineLoad
							.append({"storage": {"net": {"interfaces": "lo-a,lo-b"}, "driver": {"limit": {"concurrency": 2}}}})
							.append({
								"load": {"op": {"type": "read"}},
								"storage": {"net": {"interfaces": "lo-c,lo-d"}, "driver": {"limit": {"concurrency": 4}}}
							})
							.run();
						""");
		assertNotNull(outcome);
		assertTrue(outcome.contains("concurrent RDMA drivers need 6MiB plus 8GiB headroom"), outcome);
		assertFalse(outcome.contains("pool needs"), outcome);
		assertTrue(stub.objectRequests().isEmpty(), () -> "no object request: " + stub.objectRequests());
	}

	@Test
	void aShortBudgetUnderWarnRunsOnAndThePoolGoesOverToPerOperationBuffers() throws Exception {
		final var factory = stub.new Factory();
		factory.directMemoryLimitBytes = 1L << 20;
		final var stepId = "rdma-pool-warn-" + System.nanoTime();
		final var outcome = runScenario(factory, stepId, """
						Load.config({"storage": {"rdma": {"poolShortfall": "warn"}, "driver": {"limit": {"concurrency": 2}}}}).run();
						""");
		assertNull(outcome, () -> "the run went on: " + outcome);
		assertFalse(stub.objectRequests().isEmpty());
		final var driver = factory.drivers.stream().filter(one -> one.openRdmaResources() != null).findFirst().orElseThrow();
		assertEquals(0, driver.openRdmaResources().nics().get(0).prewarmedBuffers, "nothing fits its budget");
		assertTrue(driver.openRdmaResources().nics().get(0).pool.exhausted.sum() > 0, "the 5.16 fallback");
	}

	@Test
	void aBarrierTimeoutDuringPreWarmFailsTheStepAndReleasesItsBuffers() throws Exception {
		final var factory = stub.new Factory();
		final var stepId = "rdma-pool-timeout-" + System.nanoTime();
		final var holding = new CountDownLatch(1);
		final var proceed = new CountDownLatch(1);
		stub.beforeRegistration = () -> {
			if (Thread.currentThread().getName().contains("rdma-lane")) {
				holding.countDown();
				try {
					proceed.await(20, TimeUnit.SECONDS);
				} catch (final InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		};
		final var outcome = runScenario(factory, stepId, """
						Load.config({"load": {"step": {"preparation": {"timeout": "2s"}}}, "storage": {"driver": {"limit": {"concurrency": 2}}}}).run();
						""");
		assertTrue(holding.await(1, TimeUnit.SECONDS), "the pre-warm had begun");
		proceed.countDown();
		assertNotNull(outcome);
		assertTrue(outcome.contains("did not finish within") && outcome.contains("pre-warming pools"), outcome);
		assertTrue(stub.objectRequests().isEmpty(), () -> "no object request: " + stub.objectRequests());
		final var open = factory.drivers.stream().filter(one -> one.openRdmaResources() != null).findFirst().orElseThrow().openRdmaResources();
		for (final var nic : open.nics()) {
			waitFor(() -> nic.closed() && nic.pool.liveBuffers() == 0, "NIC " + nic.name + " released its buffers and closed");
		}
		open.lanes().forEach(lane -> assertTrue(lane.exited(), "every lane ended"));
	}

	@Test
	void withoutANicSetTheDriverHasNoNicPools() throws Exception {
		final var transport = new FakeRdmaTransport(rdma(false, true, 1));
		final var driver = S3RdmaStorageDriverTestSupport.newDriver(rdma(false, true, 1), transport);
		resources.add(driver);
		assertFalse(driver instanceof PoolDemandDriver, "the default route is checked and warmed by nothing");
	}

	/** Runs {@code scenario}; returns the failure it ended with, or {@code null}. */
	private String runScenario(
					final RdmaNicLoopback.Factory factory, final String stepId, final String scenario) throws Exception {
		final var root = RdmaNicLoopback.root(rdma(false, true, 1), stub.listeners(2), NICS.subList(0, 2), 1);
		final Config config = ConfigUtil.merge("-", List.of(new BundledDefaultsProvider().config("-", root.schema()), root));
		config.val("run-id", 781L);
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
			new RunImpl("RDMA pool canary", scenario, engine, 781L).run();
			return null;
		} catch (final RuntimeException | Error failure) {
			final var messages = new ArrayList<String>();
			for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
				messages.add(String.valueOf(cause.getMessage()));
			}
			return String.join(" | ", messages);
		}
	}
}
