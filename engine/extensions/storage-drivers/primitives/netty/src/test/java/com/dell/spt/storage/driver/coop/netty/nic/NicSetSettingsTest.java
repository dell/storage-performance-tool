package com.dell.spt.storage.driver.coop.netty.nic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.config.NicSetConfig;
import com.github.akurilov.commons.concurrent.ThreadUtil;
import com.github.akurilov.confuse.Config;
import io.netty.channel.epoll.Epoll;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntSupplier;
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
	void theConfiguredDriverThreadsAreKeptWithZeroForTheDefault() {
		assumeTrue(Epoll.isAvailable());
		assertEquals(3, NicSetSettings.fromStorage(storage(List.of("ens2np0"), "epoll", "default", 0, 1, 3)).orElseThrow().driverThreads());
		for (final var unset : List.of(0, -2)) {
			final var settings = NicSetSettings.fromStorage(storage(List.of("ens2np0"), "epoll", "default", 0, 1, unset)).orElseThrow();
			assertEquals(0, settings.driverThreads());
			assertEquals(NicSetSettings.defaultThreadsPerNic(), settings.threadsPerNic());
		}
	}

	@Test
	void aDriversDefaultAppliesOnlyWithoutConfiguredDriverThreads() {
		assumeTrue(Epoll.isAvailable());
		final var asked = new int[1];
		final IntSupplier five = () -> {
			asked[0]++;
			return 5;
		};
		assertEquals(5, NicSetSettings.fromStorage(storage(List.of("ens2np0"), "epoll", "default", 0, 1, 0), five).orElseThrow().threadsPerNic());
		assertEquals(1, asked[0]);
		assertEquals(3, NicSetSettings.fromStorage(storage(List.of("ens2np0"), "epoll", "default", 0, 1, 3), five).orElseThrow().threadsPerNic());
		assertFalse(NicSetSettings.fromStorage(storage(List.of(), "epoll", "default", 0, 1, 0), five).isPresent());
		assertEquals(1, asked[0], "the default is asked only when it is used");
	}

	@Test
	void theRdmaDefaultIsAFortyEighthOfTheHardwareThreadsAndNeverBelowFour() {
		final var hardwareThreads = ThreadUtil.getHardwareThreadCount();
		assertEquals(Math.max(4, hardwareThreads / 48), NicSetSettings.defaultRdmaThreadsPerNic());
		assertEquals(Math.max(4, hardwareThreads / 8), NicSetSettings.defaultThreadsPerNic());
		assertTrue(NicSetSettings.defaultRdmaThreadsPerNic() <= NicSetSettings.defaultThreadsPerNic());
		assertEquals(List.of(4, 4, 4, 5, 6), List.of(16, 192, 239, 240, 288).stream().map(NicSetSettings::defaultRdmaThreadsPerNic).toList());
		assertEquals(List.of(4, 24, 29, 30, 36), List.of(16, 192, 239, 240, 288).stream().map(NicSetSettings::defaultThreadsPerNic).toList());
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

	@Test
	void admissionFollowsTheOpenLimitsAndBoundsWaitingAttemptsWhenUnlimited() {
		assertEquals(3, NicSetSettings.admissionLimit(10, 4, 0));
		assertEquals(2, NicSetSettings.admissionLimit(10, 4, 3));
		assertEquals(1, NicSetSettings.admissionLimit(3, 4, 3), "one per NIC in the shared mode");
		assertEquals(NicSetSettings.UNLIMITED_WAITING_LIMIT, NicSetSettings.admissionLimit(0, 4, 2));
		assertEquals(NicSetSettings.UNLIMITED_WAITING_LIMIT, NicSetSettings.admissionLimit(-1, 1, 0));
	}

	@Test
	void onlyAConcurrencyLimitOfAtLeastOnePerNicGivesEveryNicAShare() {
		assertTrue(NicSetSettings.hasShares(4, 4));
		assertTrue(NicSetSettings.hasShares(10, 4));
		assertTrue(NicSetSettings.hasShares(1, 1));
		assertFalse(NicSetSettings.hasShares(3, 4), "the shared mode");
		assertFalse(NicSetSettings.hasShares(0, 1), "unlimited");
		assertFalse(NicSetSettings.hasShares(-1, 1), "unlimited");
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
