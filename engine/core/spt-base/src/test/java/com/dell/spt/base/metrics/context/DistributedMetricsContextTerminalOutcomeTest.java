package com.dell.spt.base.metrics.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.DeleteMetricsSnapshot;
import com.github.akurilov.commons.system.SizeInBytes;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** A terminal snapshot carries the DELETE outcome set after it was taken, from the same slice read. */
class DistributedMetricsContextTerminalOutcomeTest {

	private MetricsContextImpl<?> worker;
	private DistributedMetricsContextImpl<?> fleet;

	@AfterEach
	void tearDown() {
		if (fleet != null) {
			fleet.close();
		}
		if (worker != null) {
			worker.close();
		}
	}

	@Test
	void anOutcomeSetAfterTheTerminalSnapshotIsAppliedWithoutReadingTheSlicesAgain() {
		final AtomicInteger reads = new AtomicInteger();
		final AllMetricsSnapshot slice = sliceSnapshot();
		build(() -> {
			// The slice is released after the first read: a second read finds nothing.
			return reads.incrementAndGet() == 1 ? List.of(slice) : null;
		});
		fleet.metadata().put(
						MetricsConstants.METADATA_DELETE_FAILURE_OUTCOME,
						MetricsConstants.DELETE_FAILURE_OUTCOME_COMPLETED_CLEANLY);

		final AllMetricsSnapshot taken = fleet.terminalSnapshot();
		fleet.metadata().put(
						MetricsConstants.METADATA_DELETE_FAILURE_OUTCOME, MetricsConstants.DELETE_FAILURE_OUTCOME_FAILED);
		final AllMetricsSnapshot written = fleet.terminalSnapshot();

		assertEquals(MetricsConstants.DELETE_FAILURE_OUTCOME_COMPLETED_CLEANLY, taken.deleteMetrics().failureOutcome());
		assertEquals(MetricsConstants.DELETE_FAILURE_OUTCOME_FAILED, written.deleteMetrics().failureOutcome());
		assertEquals(10, written.deleteMetrics().objectAccepted());
		assertEquals(1, reads.get(), "the slices are read once");
	}

	@Test
	void anUnchangedOutcomeKeepsTheTerminalSnapshot() {
		final AllMetricsSnapshot slice = sliceSnapshot();
		build(() -> List.of(slice));

		final AllMetricsSnapshot taken = fleet.terminalSnapshot();

		assertEquals(taken, fleet.terminalSnapshot());
	}

	private void build(final Supplier<List<AllMetricsSnapshot>> supplier) {
		fleet = (DistributedMetricsContextImpl<?>) DistributedMetricsContextImpl.builder()
						.loadStepId("terminal-outcome")
						.opType(OpType.DELETE)
						.nodeCountSupplier(() -> 1)
						.concurrencyLimit(1)
						.concurrencyThreshold(0)
						.itemDataSize(new SizeInBytes(0))
						.outputPeriodSec(0)
						.stdOutColorFlag(false)
						.avgPersistFlag(false)
						.sumPersistFlag(false)
						.timingPersistFlag(false)
						.snapshotsSupplier(supplier)
						.quantileValues(List.of(0.5))
						.nodeAddrs(List.of("local"))
						.deleteDetailsExpected(true)
						.comment("entry")
						.runId(1)
						.build();
		fleet.start();
	}

	private AllMetricsSnapshot sliceSnapshot() {
		worker = (MetricsContextImpl<?>) MetricsContextImpl.builder()
						.loadStepId("terminal-outcome")
						.opType(OpType.DELETE)
						.actualConcurrencyGauge(() -> 1)
						.concurrencyLimit(1)
						.concurrencyThreshold(0)
						.itemDataSize(new SizeInBytes(0))
						.outputPeriodSec(0)
						.stdOutColorFlag(false)
						.comment("worker")
						.runId(1)
						.build();
		worker.start();
		worker.markSucc(0, 1, 1);
		worker.refreshLastSnapshot();
		final AllMetricsSnapshot delegate = worker.lastSnapshot();
		final AllMetricsSnapshot snapshot = mock(AllMetricsSnapshot.class);
		when(snapshot.durationSnapshot()).thenReturn(delegate.durationSnapshot());
		when(snapshot.latencySnapshot()).thenReturn(delegate.latencySnapshot());
		when(snapshot.ttfbSnapshot()).thenReturn(delegate.ttfbSnapshot());
		when(snapshot.concurrencySnapshot()).thenReturn(delegate.concurrencySnapshot());
		when(snapshot.failsSnapshot()).thenReturn(delegate.failsSnapshot());
		when(snapshot.corruptSnapshot()).thenReturn(delegate.corruptSnapshot());
		when(snapshot.successSnapshot()).thenReturn(delegate.successSnapshot());
		when(snapshot.byteSnapshot()).thenReturn(delegate.byteSnapshot());
		when(snapshot.elapsedTimeMillis()).thenReturn(delegate.elapsedTimeMillis());
		when(snapshot.deleteMetrics()).thenReturn(DeleteMetricsSnapshot.builder(1)
						.requests(10, 10, 0, 0, 0, 1)
						.objects(10, 10, 10, 0, 0, 0, 0)
						.failureOutcome(MetricsConstants.DELETE_FAILURE_OUTCOME_RUNNING)
						.reconciled(true)
						.build());
		return snapshot;
	}
}
