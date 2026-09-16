package com.dell.spt.base.control;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.load.step.LoadStepBase;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.metrics.context.DistributedMetricsContextImpl;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class MetricsRunIdentityTest {
	@ParameterizedTest
	@ValueSource(strings = {"", "startup-cluster"
	})
	void apiCreatedBeforeRunsPublishesLiveAndRetainedRunIdentity(final String startupCluster) throws Exception {
		final var startup = TestConfigBuilder.config();
		// A reused API must not label later contexts with its startup identity.
		startup.val("run-cluster-id", startupCluster);
		try (final var manager = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR)) {
			final var responder = new MetricsJsonResponder(manager, startup);
			for (long runId : List.of(123L, 456L)) {
				final var run = TestConfigBuilder.config();
				run.val("run-id", runId);
				run.val("run-cluster-id", "run-cluster-" + runId);
				run.val("load-step-id", "identity-" + runId);
				try (final var step = new IdentityStep(run, manager)) {
					step.start();
					step.local.markSucc(1024, 1000, 500);
					step.local.refreshLastSnapshot();
					assertIdentity(responder.buildNodeMetrics(false), runId);
					assertIdentity(responder.buildClusterMetrics(false), runId);
					step.stop();
					assertTrue(manager.awaitStop(5, TimeUnit.SECONDS), "Metrics task must stop before the next run");
				}
				assertIdentity(responder.buildNodeMetrics(false), runId);
				assertIdentity(responder.buildClusterMetrics(false), runId);
			}
			// Retained rows must keep their original identity after a different run.
			for (long runId : List.of(123L, 456L)) {
				assertIdentity(responder.buildNodeMetrics(false), runId);
				assertIdentity(responder.buildClusterMetrics(false), runId);
			}
		}
	}

	private static void assertIdentity(final JsonNode rows, final long runId) {
		for (final JsonNode row : rows) {
			if (Long.toString(runId).equals(row.path("run_id").asText())) {
				assertEquals("run-cluster-" + runId, row.path("cluster_id").asText());
				assertTrue(row.path("operations").path("success_count").asLong() > 0);
				return;
			}
		}
		fail("No metrics for run " + runId + ": " + rows);
	}

	private static final class IdentityStep extends LoadStepBase {
		private final MetricsContext<?> local;

		IdentityStep(final Config config, final MetricsManager manager) {
			super(config, List.of(), List.of(), manager);
			final var size = new SizeInBytes("1KB");
			local = MetricsContextImpl.builder().loadStepId(loadStepId()).runId(runId())
							.opType(OpType.CREATE).actualConcurrencyGauge(() -> 1).concurrencyLimit(1)
							.concurrencyThreshold(0).itemDataSize(size).outputPeriodSec(0)
							.stdOutColorFlag(false).build();
			metricsContexts.add(local);
			metricsContexts.add(DistributedMetricsContextImpl.builder()
							.loadStepId(loadStepId()).runId(runId()).opType(OpType.CREATE)
							.nodeCountSupplier(() -> 1).concurrencyLimit(1).concurrencyThreshold(0)
							.itemDataSize(size).outputPeriodSec(0).stdOutColorFlag(false)
							.avgPersistFlag(false).sumPersistFlag(false).timingPersistFlag(false)
							.snapshotsSupplier(() -> List.of(local.lastSnapshot()))
							.quantileValues(List.of(0.5)).nodeAddrs(List.of("worker:1099"))
							.build());
		}

		@Override
		protected void init() {}

		@Override
		protected void doStartWrapped() {}

		@Override
		protected void initMetrics(final int originIndex, final OpType opType,
						final int concurrency, final Config metricsConfig, final SizeInBytes itemDataSize,
						final boolean outputColorFlag) {}

		@Override
		public boolean await(final long timeout, final TimeUnit unit) {
			return true;
		}

		@Override
		public String getTypeName() {
			return "identity-test";
		}
	}
}
