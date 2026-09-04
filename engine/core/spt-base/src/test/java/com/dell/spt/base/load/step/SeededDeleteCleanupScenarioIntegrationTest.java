package com.dell.spt.base.load.step;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.control.run.RunImpl;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.io.IntegrityManifestItemInput;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.deletion.SeededDeleteCleanupFinalizer;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.script.ScriptEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SeededDeleteCleanupScenarioIntegrationTest {

	private static final String CLEANUP_WRAPPER = """
					var benchmarkFailure = null;
					try {
					DeleteLoad.config({"load":{"step":{"id":"delete-step"}}}).run();
					} catch (failure) {
					  com.dell.spt.base.item.op.deletion.SeededDeleteCleanupFinalizer.rethrowIfInterrupted(failure);
					  benchmarkFailure = failure;
					}
					var cleanupFailure = null;
					var cleanupLoad = null;
					var cleanupStartedNanos = java.lang.System.nanoTime();
					try {
					  cleanupLoad = DeleteLoad.config({
					    "item":{"type":"data","input":{"file":residualFile}},
					    "load":{"op":{"type":"delete"},"step":{"id":"cleanup-step"}}
					  });
					  cleanupLoad.run();
					} catch (failure) {
					  cleanupFailure = failure;
					}
					var cleanupOutcome = com.dell.spt.base.item.op.deletion.SeededDeleteCleanupFinalizer.finish(
					    "cleanup-step", java.lang.System.nanoTime() - cleanupStartedNanos,
					    benchmarkFailure, cleanupFailure, cleanupLoad, residualFile);
					""";

	@Test
	void operationalFailureStillRecoversCanonicalResidualWithoutChangingMeasuredOutcome(
					@TempDir final Path temp) throws Exception {
		final Path residual = residual(temp);
		final byte[] frozenResidual = Files.readAllBytes(residual);
		final var benchmarkFailure = new IntegrityTerminalException(
						IntegrityTerminalException.Category.EXECUTION,
						"delete-step",
						"failure budget exceeded",
						null);
		final var deleteLoad = new SequencedDeleteLoad(residual, benchmarkFailure, null);
		final ScriptEngine engine = engine(residual, deleteLoad);

		final var thrown = assertThrows(
						IntegrityTerminalException.class,
						() -> new RunImpl("seeded DELETE cleanup", CLEANUP_WRAPPER, engine, 15L).run());

		assertSame(benchmarkFailure, thrown);
		assertEquals(1, deleteLoad.measuredRequestCount.get());
		assertEquals(
						List.of("bucket/current", "bucket/exact@version-2"),
						deleteLoad.cleanedIdentities);
		assertEquals(2, deleteLoad.calls.get());
		assertArrayEquals(frozenResidual, Files.readAllBytes(residual),
						"cleanup must not rewrite measured residual evidence");
	}

	@Test
	void partialCleanupFailureIsBestEffortAndLeavesRecoveryEvidenceFrozen(
					@TempDir final Path temp) throws Exception {
		final Path residual = residual(temp);
		final byte[] frozenResidual = Files.readAllBytes(residual);
		final var cleanupFailure = new IllegalStateException("partial cleanup failure");
		final var deleteLoad = new SequencedDeleteLoad(residual, null, cleanupFailure);
		final ScriptEngine engine = engine(residual, deleteLoad);

		assertDoesNotThrow(
						() -> new RunImpl("seeded DELETE cleanup", CLEANUP_WRAPPER, engine, 16L).run());

		assertEquals(1, deleteLoad.measuredRequestCount.get());
		assertEquals(List.of("bucket/current"), deleteLoad.cleanedIdentities);
		assertEquals(2, deleteLoad.calls.get());
		final var cleanupOutcome = (SeededDeleteCleanupFinalizer.Outcome) engine.get("cleanupOutcome");
		assertNotNull(cleanupOutcome);
		assertEquals(2, cleanupOutcome.selectedOperations());
		assertEquals(0, cleanupOutcome.succeededOperations());
		assertEquals(1, cleanupOutcome.failedOperations());
		assertNotNull(cleanupOutcome.failure(),
						"ordinary operation failures must not become a false cleanup success");
		assertArrayEquals(frozenResidual, Files.readAllBytes(residual));
	}

	@Test
	void measuredInterruptionSkipsCleanupConfigurationAndExecution(
					@TempDir final Path temp) throws Exception {
		final Path residual = residual(temp);
		final var interrupted = new InterruptedException("external stop");
		final var deleteLoad = new SequencedDeleteLoad(residual, interrupted, null);
		final ScriptEngine engine = engine(residual, deleteLoad);

		final var thrown = assertThrows(
						InterruptedException.class,
						() -> new RunImpl("seeded DELETE cleanup", CLEANUP_WRAPPER, engine, 17L).run());

		assertSame(interrupted, thrown);
		assertEquals(1, deleteLoad.measuredRequestCount.get());
		assertEquals(1, deleteLoad.calls.get(), "cleanup must not even be configured after interruption");
		assertEquals(List.of(), deleteLoad.cleanedIdentities);
	}

	private static ScriptEngine engine(final Path residual, final SequencedDeleteLoad deleteLoad) {
		final ScriptEngine engine = ScenarioUtil.scriptEngineByDefault(
						Thread.currentThread().getContextClassLoader());
		assertNotNull(engine, "default JavaScript engine must be available for scenario execution");
		engine.put("DeleteLoad", deleteLoad);
		engine.put("residualFile", residual.toString());
		return engine;
	}

	private static Path residual(final Path temp) throws Exception {
		final Path residual = temp.resolve("items.csv");
		Files.writeString(
						residual,
						"bucket,key,size,version_id\n"
										+ "bucket,current,1,\n"
										+ "bucket,exact,1,version-2\n");
		return residual;
	}

	public static final class SequencedDeleteLoad {
		private final Path residual;
		private final Throwable benchmarkFailure;
		private final boolean partialCleanupFailure;
		private final AtomicInteger calls = new AtomicInteger();
		private final AtomicInteger measuredRequestCount = new AtomicInteger();
		private final List<String> cleanedIdentities = new ArrayList<>();

		SequencedDeleteLoad(
						final Path residual,
						final Throwable benchmarkFailure,
						final RuntimeException cleanupFailure) {
			this.residual = residual;
			this.benchmarkFailure = benchmarkFailure;
			this.partialCleanupFailure = cleanupFailure != null;
		}

		public Object config(final Object ignored) {
			if (calls.getAndIncrement() == 0) {
				return new MeasuredDeleteStep(measuredRequestCount, benchmarkFailure);
			}
			return new ResidualCleanupStep(
							residual, cleanedIdentities, partialCleanupFailure);
		}
	}

	public static final class MeasuredDeleteStep {
		private final AtomicInteger measuredRequestCount;
		private final Throwable benchmarkFailure;

		private MeasuredDeleteStep(
						final AtomicInteger measuredRequestCount,
						final Throwable benchmarkFailure) {
			this.measuredRequestCount = measuredRequestCount;
			this.benchmarkFailure = benchmarkFailure;
		}

		public void run() {
			measuredRequestCount.incrementAndGet();
			if (benchmarkFailure != null) {
				com.github.akurilov.commons.lang.Exceptions.throwUnchecked(benchmarkFailure);
			}
		}
	}

	private static final class ResidualCleanupStep extends LoadStepBase {
		private final Path residual;
		private final List<String> cleanedIdentities;
		private final boolean partialCleanupFailure;
		private final MetricsContext<AllMetricsSnapshot> cleanupMetrics;

		private ResidualCleanupStep(
						final Path residual,
						final List<String> cleanedIdentities,
						final boolean partialCleanupFailure) {
			super(
							cleanupConfig(),
							Collections.emptyList(),
							Collections.emptyList(),
							mock(MetricsManager.class));
			this.residual = residual;
			this.cleanedIdentities = cleanedIdentities;
			this.partialCleanupFailure = partialCleanupFailure;
			this.cleanupMetrics = MetricsContextImpl.builder()
							.loadStepId("cleanup-step")
							.opType(OpType.DELETE)
							.actualConcurrencyGauge(() -> 0)
							.concurrencyLimit(1)
							.concurrencyThreshold(0)
							.itemDataSize(new SizeInBytes(0))
							.outputPeriodSec(1)
							.stdOutColorFlag(false)
							.runId(16)
							.build();
			metricsContexts.add(cleanupMetrics);
		}

		@Override
		protected void doStartWrapped() {}

		@Override
		protected void init() {}

		@Override
		protected void initMetrics(
						final int originIndex,
						final OpType opType,
						final int concurrency,
						final Config metricsConfig,
						final SizeInBytes itemDataSize,
						final boolean outputColorFlag) {}

		@Override
		public boolean await(final long timeout, final TimeUnit timeUnit) {
			try (var input = new IntegrityManifestItemInput(residual)) {
				for (var item = input.get(); item != null; item = input.get()) {
					final String identity = item.bucket() + '/' + item.name()
									+ (item.versionId() == null ? "" : '@' + item.versionId());
					cleanedIdentities.add(identity);
					if (partialCleanupFailure) {
						cleanupMetrics.markFail();
						return true;
					}
					cleanupMetrics.markSucc(0, 1, 1);
				}
				return true;
			} catch (final IOException e) {
				throw new IllegalStateException("failed to read frozen cleanup residual", e);
			}
		}

		@Override
		public String getTypeName() {
			return "seeded-delete-cleanup-canary";
		}
	}

	private static Config cleanupConfig() {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", "cleanup-step");
		config.val("run-id", 16L);
		config.val("load-op-type", "delete");
		config.val("storage-integrity-mode", "none");
		return config;
	}
}
