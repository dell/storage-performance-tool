package com.dell.spt.base.nic;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class NaturalOrderTest {

	@Test
	void digitRunsCompareAsNumbers() {
		final var names = new ArrayList<>(List.of("ens101np0", "eth10", "ens2np0", "eth2", "ens10np0", "eth02", "bond0", "ens2np0.100", "ens2np0.20"));

		names.sort(NaturalOrder.INSTANCE);

		assertEquals(List.of("bond0", "ens2np0", "ens2np0.20", "ens2np0.100", "ens10np0", "ens101np0", "eth02", "eth2", "eth10"), names);
	}

	@Test
	void equalOnlyForEqualStrings() {
		assertEquals(0, NaturalOrder.INSTANCE.compare("eth2", "eth2"));
		assertEquals(-1, Integer.signum(NaturalOrder.INSTANCE.compare("eth02", "eth2")));
		assertEquals(1, Integer.signum(NaturalOrder.INSTANCE.compare("eth99999999999999999999", "eth9")));
	}
}
