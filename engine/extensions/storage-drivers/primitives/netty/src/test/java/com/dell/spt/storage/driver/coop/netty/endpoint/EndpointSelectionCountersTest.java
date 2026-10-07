package com.dell.spt.storage.driver.coop.netty.endpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.util.Map;
import org.junit.jupiter.api.Test;

class EndpointSelectionCountersTest {

	@Test
	void selectionsBeyondTheTrackedLimitAreCountedAsOther() {
		final var counters = new EndpointSelectionCounters();
		final var extra = 44;
		for (var i = 0; i < EndpointSelectionConstants.MAX_TRACKED_DESTINATIONS + extra; i++) {
			counters.selected(new InetSocketAddress("10.0." + (i / 256) + "." + (i % 256), 9020));
		}
		counters.selected(new InetSocketAddress("10.0.0.0", 9020));

		final var selections = counters.snapshot().selections();

		assertEquals(EndpointSelectionConstants.MAX_TRACKED_DESTINATIONS + 1, selections.size());
		assertEquals(extra, selections.get(EndpointSelectionCounters.OTHER));
		assertEquals(2, selections.get("10.0.0.0:9020"));
	}

	@Test
	void lookupsAreCountedByOutcomeWithLatency() {
		final var counters = new EndpointSelectionCounters();
		counters.lookupCompleted(1_000_000, null);
		counters.lookupCompleted(3_000_000, new DnsLookupException(DnsLookupException.Kind.TIMEOUT, "h", null));
		counters.lookupCompleted(2_000_000, new DnsLookupException(DnsLookupException.Kind.NOT_FOUND, "h", null));

		final var snapshot = counters.snapshot();

		assertEquals(3, snapshot.lookups());
		assertEquals(Map.of("TIMEOUT", 1L, "NOT_FOUND", 1L), snapshot.lookupFailures());
		assertEquals(1_000_000, snapshot.minLookupNanos());
		assertEquals(2_000_000, snapshot.meanLookupNanos());
		assertEquals(3_000_000, snapshot.maxLookupNanos());
		assertTrue(counters.summary().contains("DNS lookups=3"), counters.summary());
		assertTrue(counters.summary().contains("latency ms min/mean/max=1.00/2.00/3.00"), counters.summary());
	}

	@Test
	void summaryWithoutLookupsReportsConnectionsOnly() {
		final var counters = new EndpointSelectionCounters();
		counters.selected(new InetSocketAddress("10.0.0.1", 9020));
		counters.connected(false);
		counters.connected(true);
		counters.connectFailed();
		counters.closed();

		final var summary = counters.summary();

		assertEquals("selections {10.0.0.1:9020=1}; connections new=1 reused=1 failed=1 closed=1", summary);
		assertFalse(summary.contains("DNS"));
	}
}
