package com.dell.spt.base.item.op.deletion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class DeleteOperationalOutcomeLedgerTest {

	@Test
	void supportsBillionIdentitySelectionWithoutAProportionalHeapArray() throws Exception {
		final long selected = 1_000_000_001L;
		try (var ledger = DeleteOperationalOutcomeLedger.create(selected)) {
			ledger.markDispatched(selected - 1);
			assertEquals(3, ledger.outcome(selected - 1));
			ledger.markTerminal(selected - 1, true);
			assertEquals(1, ledger.outcome(selected - 1));
			assertEquals(selected, ledger.storageBytes());
		}
		assertFalse(Arrays.stream(DeleteOperationalOutcomeLedger.class.getDeclaredFields())
						.filter(field -> !Modifier.isStatic(field.getModifiers()))
						.anyMatch(field -> field.getType().isArray()));
	}
}
