package com.dell.spt.storage.driver.coop.netty;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

class EventLoopDefaultsTest {

	private static final List<Integer> HOSTS = List.of(1, 6, 32, 33, 40, 96, 192, 239, 240, 288);

	@Test
	void theDefaultRouteHasOneLoopPer24HardwareThreads() {
		assertEquals(List.of(4, 4, 4, 4, 4, 4, 8, 9, 10, 12), counts(EventLoopDefaults::defaultRoute));
	}

	@Test
	void anHttpNicKeepsItsCount() {
		assertEquals(List.of(4, 4, 4, 4, 5, 12, 24, 29, 30, 36), counts(EventLoopDefaults::httpPerNic));
	}

	@Test
	void anRdmaNicKeepsItsCount() {
		assertEquals(List.of(4, 4, 4, 4, 4, 4, 4, 4, 5, 6), counts(EventLoopDefaults::rdmaPerNic));
	}

	private static List<Integer> counts(final java.util.function.IntUnaryOperator rule) {
		return HOSTS.stream().map(rule::applyAsInt).toList();
	}
}
