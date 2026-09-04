package com.dell.spt.base.item.op.deletion;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.load.step.LoadStepBase;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshot;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SeededDeleteCleanupFinalizerTest {

	@Test
	void cleanupFailureNeverChangesSuccessfulBenchmarkVerdict(@TempDir final Path temp) throws Exception {
		assertDoesNotThrow(() -> SeededDeleteCleanupFinalizer.finish(
						"cleanup-step", 12_000_000, null,
						new IllegalStateException("partial cleanup"), null, residual(temp, 2)));
	}

	@Test
	void cleanupInterruptionEscapesBestEffortOutcome(@TempDir final Path temp) throws Exception {
		final var interrupted = new InterruptedException("external stop");

		final var thrown = assertThrows(
						InterruptedException.class,
						() -> SeededDeleteCleanupFinalizer.finish(
										"cleanup-step", 12_000_000, null, interrupted, null, residual(temp, 2)));

		assertSame(interrupted, thrown);
	}

	@Test
	void capturedCleanupLifecycleInterruptionEscapesBestEffortOutcome(@TempDir final Path temp)
					throws Exception {
		final var interrupted = new InterruptedException("external stop");

		final var thrown = assertThrows(
						InterruptedException.class,
						() -> SeededDeleteCleanupFinalizer.finish(
										"cleanup-step", 12_000_000, null, null,
										cleanupStep(0, 0, interrupted), residual(temp, 2)));

		assertSame(interrupted, thrown);
	}

	@Test
	void benchmarkFailureRemainsTheExactTerminalCauseAfterCleanupFailure(@TempDir final Path temp)
					throws Exception {
		final var benchmarkFailure = new IllegalArgumentException("failure budget exceeded");

		final var thrown = assertThrows(
						IllegalArgumentException.class,
						() -> SeededDeleteCleanupFinalizer.finish(
										"cleanup-step", 12_000_000, benchmarkFailure,
										new IllegalStateException("partial cleanup"), null, residual(temp, 2)));

		assertSame(benchmarkFailure, thrown);
	}

	@Test
	void successfulCleanupCompletesWithoutChangingVerdict(@TempDir final Path temp) throws Exception {
		assertDoesNotThrow(() -> SeededDeleteCleanupFinalizer.finish(
						"cleanup-step", 0, null, null, cleanupStep(2, 0, null), residual(temp, 2)));
	}

	@Test
	void ordinaryOperationFailuresProduceAPartialCleanupOutcome(@TempDir final Path temp) throws Exception {
		final var outcome = SeededDeleteCleanupFinalizer.finish(
						"cleanup-step", 12_000_000, null, null,
						cleanupStep(2, 1, null), residual(temp, 3));

		assertEquals(3, outcome.selectedOperations());
		assertEquals(2, outcome.succeededOperations());
		assertEquals(1, outcome.failedOperations());
		assertNotNull(outcome.failure(), "generic operation failure metrics must not be reported as success");
	}

	@Test
	void swallowedOrdinaryLifecycleFailureProducesAFailedCleanupOutcome(@TempDir final Path temp)
					throws Exception {
		final var swallowed = new IllegalStateException("ordinary step failed to start");

		final var outcome = SeededDeleteCleanupFinalizer.finish(
						"cleanup-step", 12_000_000, null, null,
						cleanupStep(0, 0, swallowed), residual(temp, 2));

		assertSame(swallowed, outcome.failure());
	}

	@Test
	void missingTerminalOperationsCannotLookLikeSuccessfulCleanup(@TempDir final Path temp)
					throws Exception {
		final var outcome = SeededDeleteCleanupFinalizer.finish(
						"cleanup-step", 12_000_000, null, null,
						cleanupStep(0, 0, null), residual(temp, 2));

		assertNotNull(outcome.failure());
	}

	private static LoadStepBase cleanupStep(
					final long succeeded, final long failed, final Throwable runFailure) {
		final var cleanupStep = mock(LoadStepBase.class);
		final var snapshot = mock(AllMetricsSnapshot.class);
		final var successes = mock(RateMetricSnapshot.class);
		final var failures = mock(RateMetricSnapshot.class);
		when(successes.count()).thenReturn(succeeded);
		when(failures.count()).thenReturn(failed);
		when(snapshot.successSnapshot()).thenReturn(successes);
		when(snapshot.failsSnapshot()).thenReturn(failures);
		doReturn(List.of(snapshot)).when(cleanupStep).metricsSnapshots();
		when(cleanupStep.runFailure()).thenReturn(runFailure);
		return cleanupStep;
	}

	private static String residual(final Path temp, final int count) throws Exception {
		final var contents = new StringBuilder("bucket,key,size,version_id\n");
		for (var i = 0; i < count; i++) {
			contents.append("bucket,key-").append(i).append(",1,\n");
		}
		final Path residual = temp.resolve("items.csv");
		Files.writeString(residual, contents);
		return residual.toString();
	}
}
