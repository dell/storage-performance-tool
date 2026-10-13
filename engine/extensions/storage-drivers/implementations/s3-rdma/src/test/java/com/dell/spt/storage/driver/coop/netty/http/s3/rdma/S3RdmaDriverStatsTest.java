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
import java.util.List;
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
					"rdma_protocol_error",
					"rdma_request_failed",
					"rdma_timed_out",
					"rdma_transferred",
					"registrations_pooled");

	/** The published gauge names, in name order. */
	private static final List<String> GAUGES = List.of("drivers", "transport_available");

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
	}

	@Test
	void driverWithoutABufferPoolReportsTheSameNames() throws Exception {
		final var driver = newDriver(config(true, false), false);
		assertNull(driver.bufferPool());

		final DriverStatsSnapshot stats = driver.driverStats();

		assertEquals(COUNTERS, List.copyOf(stats.counters().keySet()));
		assertEquals(GAUGES, List.copyOf(stats.gauges().keySet()));
		stats.counters().forEach((name, value) -> assertEquals(0L, value, name));
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

		final var counters = driver.driverStats().counters();

		assertEquals(1L, counters.get("rdma_transferred"));
		assertEquals(2L, counters.get("rdma_bytes_assumed"));
		assertEquals(3L, counters.get("rdma_request_failed"));
		assertEquals(4L, counters.get("rdma_protocol_error"));
		assertEquals(5L, counters.get("rdma_timed_out"));
		assertEquals(6L, counters.get("http_rdma_unavailable"));
		assertEquals(7L, counters.get("http_below_threshold"));
		assertEquals(8L, counters.get("http_ineligible"));
		assertEquals(9L, counters.get("http_oversize"));
		assertEquals(10L, counters.get("http_fallback"));
		assertEquals(11L, counters.get("prepare_failed"));
		assertEquals(12L, counters.get("registrations_pooled"));
		assertEquals(13L, counters.get("pool_hits"));
		assertEquals(14L, counters.get("pool_exhausted"));
		assertEquals(15L, counters.get("pool_oversize"));
		assertEquals(16L, counters.get("pool_evicted"));
		assertEquals(17L, counters.get("pool_discarded"));
		assertEquals(18L, counters.get("pool_invalid_returns"));
	}

	@Test
	void readingStatisticsAsksTheTransportOnlyWhetherItIsAvailable() throws Exception {
		final RdmaConfig config = config(true, true);
		final FakeRdmaTransport transport = spy(new FakeRdmaTransport(config));
		final var driver = S3RdmaStorageDriverTestSupport.newDriver(config, transport);
		clearInvocations(transport);

		for (int i = 0; i < 100; i++) {
			driver.driverStats();
		}

		verify(transport, atLeastOnce()).isAvailable();
		verifyNoMoreInteractions(transport);
	}

	@Test
	void statisticsStayReadableAfterTheDriverCloses() throws Exception {
		final RdmaConfig config = config(true, true);
		final FakeRdmaTransport transport = spy(new FakeRdmaTransport(config));
		final var driver = S3RdmaStorageDriverTestSupport.newDriver(config, transport);
		driver.pathStats().rdmaTransferred.add(4);
		driver.bufferPool().hits.add(3);
		driver.close();
		clearInvocations(transport);

		final DriverStatsSnapshot stats = driver.driverStats();

		assertEquals(COUNTERS, List.copyOf(stats.counters().keySet()));
		assertEquals(4L, stats.counters().get("rdma_transferred"));
		assertEquals(3L, stats.counters().get("pool_hits"));
		assertEquals(0L, stats.gauges().get("transport_available"));
		verify(transport, atLeastOnce()).isAvailable();
		verifyNoMoreInteractions(transport);
	}

	@Test
	void statisticsAreReadableWhileTheDriverCloses() throws Exception {
		final var driver = newDriver(config(true, true), false);
		final CountDownLatch reading = new CountDownLatch(1);
		final AtomicBoolean closed = new AtomicBoolean();
		final AtomicReference<Throwable> failure = new AtomicReference<>();
		final Thread reader = new Thread(() -> {
			try {
				long reads = 0;
				// Keep reading until the close has finished, and a while longer.
				for (int afterClose = 0; afterClose < 1_000; reads++) {
					final DriverStatsSnapshot stats = driver.driverStats();
					if (stats.counters().size() != COUNTERS.size()) {
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
