package com.dell.spt.base.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
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
