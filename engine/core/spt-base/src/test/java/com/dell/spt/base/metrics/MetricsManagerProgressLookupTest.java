package com.dell.spt.base.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.context.MetricsContext;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** A metrics manager that implements only the step lookup keeps its fallback for a context lookup. */
class MetricsManagerProgressLookupTest {

	@Test
	void contextLookupFallsBackToTheStepLookupOfTheContextsScopeAndType() {
		final MetricsManager mgr = mock(MetricsManager.class, CALLS_REAL_METHODS);
		final TerminalStepEntry entry = entry("legacy-step", OpType.CREATE);
		doReturn(Optional.of(entry)).when(mgr).getLastProgressSnapshot("legacy-step", false);
		doReturn(Optional.empty()).when(mgr).getLastProgressSnapshot("legacy-step", true);

		final MetricsContext<?> create = context(mock(MetricsContext.class), "legacy-step", OpType.CREATE);
		final MetricsContext<?> read = context(mock(MetricsContext.class), "legacy-step", OpType.READ);
		final MetricsContext<?> fleet = context(mock(DistributedMetricsContext.class), "legacy-step", OpType.CREATE);

		assertEquals(entry, mgr.getLastProgressSnapshot(create).orElseThrow());
		assertTrue(mgr.getLastProgressSnapshot(read).isEmpty(), "another type's entry is not this context's");
		assertTrue(mgr.getLastProgressSnapshot(fleet).isEmpty());
		verify(mgr).getLastProgressSnapshot("legacy-step", true);
		assertTrue(mgr.getLastProgressSnapshot((MetricsContext<?>) null).isEmpty());
	}

	@Test
	void contextLookupOfANodeContextFallsBackToTheOneArgumentStepLookup() {
		final MetricsManager mgr = mock(MetricsManager.class, CALLS_REAL_METHODS);
		final TerminalStepEntry entry = entry("node-step", OpType.READ);
		// A manager written against the one-argument lookup alone; the two-argument one keeps its default.
		doReturn(Optional.of(entry)).when(mgr).getLastProgressSnapshot("node-step");

		final MetricsContext<?> read = context(mock(MetricsContext.class), "node-step", OpType.READ);

		assertEquals(entry, mgr.getLastProgressSnapshot(read).orElseThrow());
		verify(mgr).getLastProgressSnapshot("node-step");
	}

	@Test
	void contextLookupOfADistributedContextFallsBackToTheFleetStepLookup() {
		final MetricsManager mgr = mock(MetricsManager.class, CALLS_REAL_METHODS);
		final TerminalStepEntry entry = entry("fleet-step", OpType.READ);
		doReturn(Optional.of(entry)).when(mgr).getLastProgressSnapshot("fleet-step", true);
		doReturn(Optional.of(entry("fleet-step", OpType.READ))).when(mgr).getLastProgressSnapshot("fleet-step");

		final MetricsContext<?> fleet = context(mock(DistributedMetricsContext.class), "fleet-step", OpType.READ);

		assertEquals(entry, mgr.getLastProgressSnapshot(fleet).orElseThrow(), "the fleet entry, not the node one");
		verify(mgr).getLastProgressSnapshot("fleet-step", true);
		verify(mgr, never()).getLastProgressSnapshot("fleet-step");
	}

	private static MetricsContext<?> context(final MetricsContext<?> ctx, final String stepId, final OpType opType) {
		when(ctx.loadStepId()).thenReturn(stepId);
		when(ctx.opType()).thenReturn(opType);
		return ctx;
	}

	private static TerminalStepEntry entry(final String stepId, final OpType opType) {
		return new TerminalStepEntry(
						stepId, opType, 1L, System.currentTimeMillis(), 80L, 0L, 327_680L, 120.0, 130.0, 0L, 0.0, 100L, 0L, 2_000L);
	}
}
