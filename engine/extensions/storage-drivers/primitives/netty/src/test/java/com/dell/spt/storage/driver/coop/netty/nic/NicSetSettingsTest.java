package com.dell.spt.storage.driver.coop.netty.nic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.config.NicSetConfig;
import com.github.akurilov.confuse.Config;
import io.netty.channel.epoll.Epoll;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

@SuppressWarnings({"rawtypes", "unchecked"
})
class NicSetSettingsTest {

	@Test
	void noNicSetHasNoSettings() {
		assertTrue(NicSetSettings.fromStorage(storage(List.of(), "epoll", "default", 0, 30_000, 0)).isEmpty());
	}

	@Test
	void connectTimeoutFollowsTheNetTimeoutAndNeverStaysUnbounded() {
		assumeTrue(Epoll.isAvailable());
		final var configured = NicSetSettings.fromStorage(storage(List.of("ens2np0"), "epoll", "default", 0, 4_000, 0)).orElseThrow();
		assertEquals(4_000, configured.connectTimeoutMillis());
		assertEquals(4_000, configured.probeTimeoutMillis(), "a timeout below 10 s is honored");

		final var shipped = NicSetSettings.fromStorage(storage(List.of("ens2np0"), "epoll", "default", 0, 30_000, 0)).orElseThrow();
		assertEquals(30_000, shipped.connectTimeoutMillis());
		assertEquals(NicSetSettings.MAX_PROBE_TIMEOUT_MILLIS, shipped.probeTimeoutMillis());

		for (final var unbounded : List.of(0, -1)) {
			final var settings = NicSetSettings.fromStorage(storage(List.of("auto"), "", "default", 0, unbounded, 0)).orElseThrow();
			assertEquals(NicSetSettings.DEFAULT_CONNECT_TIMEOUT_MILLIS, settings.connectTimeoutMillis());
			assertEquals(NicSetSettings.MAX_PROBE_TIMEOUT_MILLIS, settings.probeTimeoutMillis());
		}
	}

	@Test
	void explicitDriverThreadsCountPerNic() {
		assumeTrue(Epoll.isAvailable());
		assertEquals(3, NicSetSettings.fromStorage(storage(List.of("ens2np0"), "epoll", "default", 0, 1, 3)).orElseThrow().threadsPerNic());
		assertEquals(NicSetSettings.defaultThreadsPerNic(),
						NicSetSettings.fromStorage(storage(List.of("ens2np0"), "epoll", "default", 0, 1, 0)).orElseThrow().threadsPerNic());
		assertTrue(NicSetSettings.defaultThreadsPerNic() >= 4);
	}

	@Test
	void aTransportThatCannotBindIsRejectedByName() {
		for (final var transport : List.of("nio", "kqueue", "iouring")) {
			final var failure = assertThrows(IllegalConfigurationException.class,
							() -> NicSetSettings.fromStorage(storage(List.of("ens2np0"), transport, "default", 0, 1, 0)));
			assertTrue(failure.getMessage().contains("storage.net.transport is " + transport), failure.getMessage());
		}
	}

	@Test
	void nonDefaultEndpointModesAreRejected() {
		for (final var mode : List.of("round-robin", "per-request-dns")) {
			final var failure = assertThrows(IllegalConfigurationException.class,
							() -> NicSetSettings.fromStorage(storage(List.of("ens2np0"), "epoll", mode, 0, 1, 0)));
			assertTrue(failure.getMessage().contains(mode), failure.getMessage());
		}
	}

	@Test
	void connAttemptsLimitIsRejected() {
		assumeTrue(Epoll.isAvailable());
		final var failure = assertThrows(IllegalConfigurationException.class,
						() -> NicSetSettings.fromStorage(storage(List.of("ens2np0"), "epoll", "default", 3, 1, 0)));
		assertTrue(failure.getMessage().contains("connAttemptsLimit=3"), failure.getMessage());
	}

	@Test
	void openLimitsShareTheConcurrencyLimitAndNeverMultiplyIt() {
		assertEquals(List.of(3, 3, 2, 2), limits(10, 4));
		assertEquals(List.of(8, 8), limits(16, 2));
		assertEquals(List.of(1, 1, 1, 1), limits(4, 4));
		// 0 < P < N: one connection per NIC, so every NIC carries work (R2).
		assertEquals(List.of(1, 1, 1, 1), limits(1, 4));
		assertEquals(List.of(1, 1, 1, 1), limits(3, 4));
		// Unlimited: no cap.
		assertEquals(List.of(0, 0), limits(0, 2));
		for (var p = 4; p <= 64; p++) {
			assertEquals(p, limits(p, 4).stream().mapToInt(Integer::intValue).sum());
		}
	}

	@Test
	void preOpeningFollowsTheSharesAndIsBoundedWhenUnlimited() {
		assertEquals(3, NicSetSettings.preOpenCount(10, 4, 0, 2));
		assertEquals(2, NicSetSettings.preOpenCount(10, 4, 3, 2));
		assertEquals(1, NicSetSettings.preOpenCount(1, 4, 2, 2));
		assertEquals(2, NicSetSettings.preOpenCount(0, 4, 0, 2), "one per Path when unlimited");
		assertEquals(5, NicSetSettings.preOpenCount(-1, 1, 0, 5));
	}

	private static List<Integer> limits(final int concurrency, final int nics) {
		final var limits = new ArrayList<Integer>();
		for (var i = 0; i < nics; i++) {
			limits.add(NicSetSettings.openLimit(concurrency, nics, i));
		}
		return limits;
	}

	private static Config storage(final List<String> interfaces, final String transport, final String selection,
					final int connAttemptsLimit, final int timeoutMillis, final int driverThreads) {
		final Config storage = mock(Config.class);
		final Config net = mock(Config.class);
		when(storage.listVal(NicSetConfig.INTERFACES_PATH)).thenReturn((List) interfaces);
		when(storage.stringVal("net-endpoint-selection")).thenReturn(selection);
		when(storage.configVal("net")).thenReturn(net);
		when(storage.intVal("driver-threads")).thenReturn(driverThreads);
		when(net.stringVal("transport")).thenReturn(transport);
		when(net.intVal("node-connAttemptsLimit")).thenReturn(connAttemptsLimit);
		when(net.intVal("timeoutMilliSec")).thenReturn(timeoutMillis);
		return storage;
	}
}
