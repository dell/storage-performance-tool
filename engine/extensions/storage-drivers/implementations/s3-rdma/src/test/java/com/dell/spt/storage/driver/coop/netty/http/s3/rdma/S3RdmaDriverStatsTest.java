package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.metrics.snapshot.DriverStatsSnapshot;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** The statistics the S3-RDMA driver reports about itself, without a server. */
final class S3RdmaDriverStatsTest {

	private static final long THRESHOLD = 1_048_576L;

	/** The published counter names, in name order. A change here changes the public metrics JSON. */
	private static final List<String> COUNTERS = List.of(
					"allocation_failed_on_demand",
					"allocation_failed_pooled",
					"http_below_threshold",
					"http_fallback",
					"http_ineligible",
					"http_oversize",
					"http_rdma_unavailable",
					"pool_discarded",
					"pool_evicted",
					"pool_exhausted",
					"pool_hits",
					"pool_invalid_returns",
					"pool_oversize",
					"prepare_failed",
					"rdma_bytes_assumed",
					"rdma_declined_failed",
					"rdma_declined_served_over_http",
					"rdma_protocol_error",
					"rdma_request_failed",
					"rdma_requests_sent",
					"rdma_timed_out",
					"rdma_transferred",
					"registration_failed_on_demand",
					"registration_failed_pooled",
					"registrations_on_demand",
					"registrations_pooled",
					"token_generation_failed");

	/** The published gauge names, in name order. */
	private static final List<String> GAUGES = List.of(
					"drivers",
					"pool_budget_bytes",
					"pool_buffers",
					"pool_idle_buffers",
					"pool_peak_reserved_bytes",
					"pool_reserved_bytes",
					"transport_available");

	private static final int MIB = 1 << 20;

	@AfterEach
	void closeDrivers() throws Exception {
		S3RdmaStorageDriverTestSupport.closeCreatedDrivers();
	}

	@Test
	void newDriverReportsEveryNameWithZeroCounts() throws Exception {
		final var driver = newDriver(config(true, true), false);

		final DriverStatsSnapshot stats = driver.driverStats();

		assertEquals("s3-rdma", stats.driver());
		assertEquals(COUNTERS, List.copyOf(stats.counters().keySet()));
		stats.counters().forEach((name, value) -> assertEquals(0L, value, name));
		assertEquals(GAUGES, List.copyOf(stats.gauges().keySet()));
		assertEquals(1L, stats.gauges().get("drivers"));
		assertEquals(1L, stats.gauges().get("transport_available"));
		assertEquals(RdmaBufferPool.defaultMaxPooledBytes(), stats.gauges().get("pool_budget_bytes"));
		for (final String level : List.of(
						"pool_reserved_bytes", "pool_buffers", "pool_idle_buffers", "pool_peak_reserved_bytes")) {
			assertEquals(0L, stats.gauges().get(level), level);
		}
	}

	@Test
	void driverWithoutABufferPoolReportsTheSameNames() throws Exception {
		final var driver = newDriver(config(true, false), false);
		assertNull(driver.bufferPool());

		final DriverStatsSnapshot stats = driver.driverStats();

		assertEquals(COUNTERS, List.copyOf(stats.counters().keySet()));
		assertEquals(GAUGES, List.copyOf(stats.gauges().keySet()));
		stats.counters().forEach((name, value) -> assertEquals(0L, value, name));
		// No pool: no budget and no level.
		stats.gauges().forEach((name, value) -> assertEquals(
						"drivers".equals(name) || "transport_available".equals(name) ? 1L : 0L, value, name));
	}

	@Test
	void transportThatDidNotInitializeIsReportedUnavailable() throws Exception {
		final var driver = newDriver(config(true, true), true);

		final DriverStatsSnapshot stats = driver.driverStats();

		assertEquals(0L, stats.gauges().get("transport_available"));
		assertEquals(1L, stats.gauges().get("drivers"));
		assertEquals(COUNTERS, List.copyOf(stats.counters().keySet()));
	}

	@Test
	void countsAppearUnderTheirPublishedNames() throws Exception {
		final var driver = newDriver(config(true, true), false);
		final RdmaPathStats path = driver.pathStats();
		final RdmaBufferPool pool = driver.bufferPool();
		// One distinct value per counter, so a name mapped to the wrong counter cannot pass.
		path.rdmaTransferred.add(1);
		path.rdmaBytesAssumed.add(2);
		path.rdmaHttpError.add(3);
		path.rdmaProtocolError.add(4);
		path.rdmaTimedOut.add(5);
		path.httpRdmaUnavailable.add(6);
		path.httpBelowThreshold.add(7);
		path.httpIneligible.add(8);
		path.httpOversize.add(9);
		path.httpFallback.add(10);
		path.prepareFailed.add(11);
		pool.created.add(12);
		pool.hits.add(13);
		pool.exhausted.add(14);
		pool.unpooled.add(15);
		pool.evicted.add(16);
		pool.discarded.add(17);
		pool.invalidReturns.add(18);
		path.rdmaRequestsSent.add(19);
		path.rdmaDeclinedServedOverHttp.add(20);
		path.rdmaDeclinedFailed.add(21);
		path.registrationsOnDemand.add(22);
		path.registrationFailedOnDemand.add(23);
		path.allocationFailedOnDemand.add(24);
		path.tokenGenerationFailed.add(25);
		pool.registrationFailed.add(26);
		pool.allocationFailed.add(27);

		final Map<String, Long> expected = new TreeMap<>();
		expected.put("rdma_transferred", 1L);
		expected.put("rdma_bytes_assumed", 2L);
		expected.put("rdma_request_failed", 3L);
		expected.put("rdma_protocol_error", 4L);
		expected.put("rdma_timed_out", 5L);
		expected.put("http_rdma_unavailable", 6L);
		expected.put("http_below_threshold", 7L);
		expected.put("http_ineligible", 8L);
		expected.put("http_oversize", 9L);
		expected.put("http_fallback", 10L);
		expected.put("prepare_failed", 11L);
		expected.put("registrations_pooled", 12L);
		expected.put("pool_hits", 13L);
		expected.put("pool_exhausted", 14L);
		expected.put("pool_oversize", 15L);
		expected.put("pool_evicted", 16L);
		expected.put("pool_discarded", 17L);
		expected.put("pool_invalid_returns", 18L);
		expected.put("rdma_requests_sent", 19L);
		expected.put("rdma_declined_served_over_http", 20L);
		expected.put("rdma_declined_failed", 21L);
		expected.put("registrations_on_demand", 22L);
		expected.put("registration_failed_on_demand", 23L);
		expected.put("allocation_failed_on_demand", 24L);
		expected.put("token_generation_failed", 25L);
		expected.put("registration_failed_pooled", 26L);
		expected.put("allocation_failed_pooled", 27L);
		assertEquals(COUNTERS.size(), expected.size(), "a value for every published counter");

		assertEquals(expected, driver.driverStats().counters());
	}

	@Test
	void poolLevelsAppearUnderTheirPublishedNames() throws Exception {
		final var driver = newDriver(config(true, true), false);
		final RdmaBufferPool pool = driver.bufferPool();
		// Three buffers reserved at the peak; then one is idle, one leased and one gone.
		final RdmaBufferPool.PooledBuffer idle = pool.acquire(MIB);
		final RdmaBufferPool.PooledBuffer leased = pool.acquire(4 * MIB);
		final RdmaBufferPool.PooledBuffer gone = pool.acquire(2 * MIB);
		pool.release(idle);
		pool.discard(gone);

		final var gauges = driver.driverStats().gauges();

		assertEquals(5L * MIB, gauges.get("pool_reserved_bytes"));
		assertEquals(7L * MIB, gauges.get("pool_peak_reserved_bytes"));
		assertEquals(2L, gauges.get("pool_buffers"));
		assertEquals(1L, gauges.get("pool_idle_buffers"));
		assertEquals(RdmaBufferPool.defaultMaxPooledBytes(), gauges.get("pool_budget_bytes"));
		pool.release(leased);
	}

	@Test
	void readingStatisticsAsksTheTransportOnlyWhetherItIsAvailable() throws Exception {
		final RdmaConfig config = config(true, true);
		final FakeRdmaTransport transport = spy(new FakeRdmaTransport(config));
		final var driver = S3RdmaStorageDriverTestSupport.newDriver(config, transport);
		// The pool holds a leased and an idle buffer, so its levels describe registered memory.
		final RdmaBufferPool.PooledBuffer leased = driver.bufferPool().acquire(MIB);
		driver.bufferPool().release(driver.bufferPool().acquire(2 * MIB));
		clearInvocations(transport);

		for (int i = 0; i < 100; i++) {
			assertEquals(3L * MIB, driver.driverStats().gauges().get("pool_reserved_bytes"));
		}

		verify(transport, atLeastOnce()).isAvailable();
		verifyNoMoreInteractions(transport);
		driver.bufferPool().release(leased);
	}

	@Test
	void statisticsStayReadableAfterTheDriverCloses() throws Exception {
		final RdmaConfig config = config(true, true);
		final FakeRdmaTransport transport = spy(new FakeRdmaTransport(config));
		final var driver = S3RdmaStorageDriverTestSupport.newDriver(config, transport);
		driver.pathStats().rdmaTransferred.add(4);
		driver.bufferPool().hits.add(3);
		driver.bufferPool().release(driver.bufferPool().acquire(MIB));
		driver.close();
		clearInvocations(transport);

		final DriverStatsSnapshot stats = driver.driverStats();

		assertEquals(COUNTERS, List.copyOf(stats.counters().keySet()));
		assertEquals(GAUGES, List.copyOf(stats.gauges().keySet()));
		assertEquals(4L, stats.counters().get("rdma_transferred"));
		assertEquals(3L, stats.counters().get("pool_hits"));
		assertEquals(0L, stats.gauges().get("transport_available"));
		// The closed pool holds nothing; what it once reserved stays on record.
		assertEquals(0L, stats.gauges().get("pool_reserved_bytes"));
		assertEquals(0L, stats.gauges().get("pool_buffers"));
		assertEquals(0L, stats.gauges().get("pool_idle_buffers"));
		assertEquals((long) MIB, stats.gauges().get("pool_peak_reserved_bytes"));
		verify(transport, atLeastOnce()).isAvailable();
		verifyNoMoreInteractions(transport);
	}

	@Test
	void statisticsAreReadableWhileTheDriverCloses() throws Exception {
		final var driver = newDriver(config(true, true), false);
		// Idle buffers for the close to deregister while the levels are read.
		final List<RdmaBufferPool.PooledBuffer> leases = new ArrayList<>();
		for (int i = 0; i < 2; i++) {
			leases.add(driver.bufferPool().acquire(MIB));
		}
		leases.forEach(driver.bufferPool()::release);
		final CountDownLatch reading = new CountDownLatch(1);
		final AtomicBoolean closed = new AtomicBoolean();
		final AtomicReference<Throwable> failure = new AtomicReference<>();
		final Thread reader = new Thread(() -> {
			try {
				long reads = 0;
				// Keep reading until the close has finished, and a while longer.
				for (int afterClose = 0; afterClose < 1_000; reads++) {
					final DriverStatsSnapshot stats = driver.driverStats();
					if (stats.counters().size() != COUNTERS.size() || stats.gauges().size() != GAUGES.size()) {
						throw new AssertionError("incomplete statistics after " + reads + " reads: " + stats);
					}
					reading.countDown();
					if (closed.get()) {
						afterClose++;
					}
				}
			} catch (final Throwable e) {
				failure.set(e);
				reading.countDown();
			}
		}, "driver-stats-reader");
		reader.start();
		assertTrue(reading.await(10, TimeUnit.SECONDS), "the reader did not start");

		driver.close();
		closed.set(true);
		reader.join(TimeUnit.SECONDS.toMillis(30));

		assertTrue(!reader.isAlive(), "the reader did not finish");
		assertNull(failure.get(), () -> "reading statistics failed: " + failure.get());
	}

	private static S3RdmaStorageDriver<Item, Operation<Item>> newDriver(final RdmaConfig config, final boolean initFails)
					throws Exception {
		final FakeRdmaTransport transport = new FakeRdmaTransport(config);
		transport.setFailInit(initFails);
		return S3RdmaStorageDriverTestSupport.newDriver(config, transport);
	}

	private static RdmaConfig config(final boolean fallback, final boolean bufferPool) {
		return new RdmaConfig(true, THRESHOLD, fallback, "auto", "", "WARN", 30_000L, false, bufferPool);
	}
}
