package com.dell.spt.base.load.step.local.context;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.PathItem;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.list.ListOperation;
import com.dell.spt.base.item.op.list.ListOperationImpl;
import com.dell.spt.base.item.op.list.shard.ListShard;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.*;
import com.github.akurilov.confuse.impl.BasicConfig;
import com.github.akurilov.confuse.Config;
import java.lang.reflect.Method;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the idle/backlog gate suppresses delimiter split probes when concurrency is at the
 * limit (concCurr >= concLimit). The test invokes the private tryDelimiterSplit via reflection and
 * asserts it returns false without throwing.
 */
public class LoadStepContextSplitGateTest {

	// Minimal fake MetricsContext returning a fixed concurrency limit and snapshot
	static final class FakeMetrics implements MetricsContext<AllMetricsSnapshot> {
		final int limit;
		final long last;

		FakeMetrics(final int limit, final long last) {
			this.limit = limit;
			this.last = last;
		}

		@Override
		public int concurrencyLimit() {
			return limit;
		}

		@Override
		public AllMetricsSnapshot lastSnapshot() {
			return new AllMetricsSnapshot() {
				final ConcurrencyMetricSnapshot conc = new ConcurrencyMetricSnapshot() {
					@Override
					public long last() {
						return last;
					}

					@Override
					public double mean() {
						return last;
					}

					@Override
					public String name() {
						return "conc";
					}
				};

				@Override
				public TimingMetricSnapshot durationSnapshot() {
					return null;
				}

				@Override
				public TimingMetricSnapshot latencySnapshot() {
					return null;
				}

				@Override
				public ConcurrencyMetricSnapshot concurrencySnapshot() {
					return conc;
				}

				@Override
				public RateMetricSnapshot byteSnapshot() {
					return null;
				}

				@Override
				public RateMetricSnapshot successSnapshot() {
					return null;
				}

				@Override
				public RateMetricSnapshot failsSnapshot() {
					return null;
				}

				@Override
				public long elapsedTimeMillis() {
					return 0;
				}
			};
		}

		// Unused interface methods for this test
		@Override
		public Map metadata() {
			return Map.of();
		}

		@Override
		public String loadStepId() {
			return "t";
		}

		@Override
		public long runId() {
			return 0;
		}

		@Override
		public com.dell.spt.base.item.op.OpType opType() {
			return com.dell.spt.base.item.op.OpType.LIST;
		}

		@Override
		public com.github.akurilov.commons.system.SizeInBytes itemDataSize() {
			return null;
		}

		@Override
		public void markSucc(long b, long d, long l) {}

		@Override
		public void markPartSucc(long b, long d, long l) {}

		@Override
		public void markSucc(long c, long b, long[] d, long[] l) {}

		@Override
		public void markPartSucc(long b, long[] d, long[] l) {}

		@Override
		public void markFail() {}

		@Override
		public void markFail(long c) {}

		@Override
		public void start() {}

		@Override
		public boolean isStarted() {
			return true;
		}

		@Override
		public long startTimeStamp() {
			return 0;
		}

		@Override
		public void refreshLastSnapshot() {}

		@Override
		public int concurrencyThreshold() {
			return Integer.MAX_VALUE;
		}

		@Override
		public boolean thresholdStateEntered() {
			return false;
		}

		@Override
		public void enterThresholdState() {}

		@Override
		public boolean thresholdStateExited() {
			return false;
		}

		@Override
		public MetricsContext thresholdMetrics() {
			return this;
		}

		@Override
		public void exitThresholdState() {}

		@Override
		public boolean stdOutColorEnabled() {
			return false;
		}

		@Override
		public boolean avgPersistEnabled() {
			return false;
		}

		@Override
		public boolean sumPersistEnabled() {
			return false;
		}

		@Override
		public boolean timingPersistEnabled() {
			return false;
		}

		@Override
		public long outputPeriodMillis() {
			return 0;
		}

		@Override
		public long lastOutputTs() {
			return 0;
		}

		@Override
		public void lastOutputTs(long ts) {}

		@Override
		public long elapsedTimeMillis() {
			return 0;
		}

		@Override
		public String comment() {
			return "";
		}

		@Override
		public void close() {}

		@Override
		public int compareTo(final MetricsContext o) {
			return 0;
		}
	}

	// Minimal ListOperation carrying pageFirstKey/startAfter and a shard
	// Minimal PathItem impl for tests
	static final class PI implements PathItem {
		private String n;

		PI(String n) {
			this.n = n;
		}

		@Override
		public String name() {
			return n;
		}

		@Override
		public void name(final String name) {
			this.n = name;
		}

		@Override
		public void reset() {}

		@Override
		public String toString(final String itemPath) {
			return n;
		}

		@Override
		public void writeExternal(java.io.ObjectOutput out) {}

		@Override
		public void readExternal(java.io.ObjectInput in) {}
	}

	static ListOperation<?> newListOp(final String srcPath, final String first, final String last, final ListShard shard) {
		final var op = new ListOperationImpl<PathItem>(0, com.dell.spt.base.item.op.OpType.LIST, new PI("/"), null);
		op.srcPath(srcPath);
		op.pageFirstKey(first);
		op.startAfter(last);
		op.shard(shard);
		op.options(op.options().toBuilder().maxKeys(1000).build());
		return op;
	}

	@Test
	public void gateSkipsProbeWhenConcurrencyAtLimit() throws Exception {
		// Arrange a context with concCurr == concLimit so the gate should skip
		final var fakeMetrics = new FakeMetrics(64, 64);
		final var driver = com.dell.spt.base.storage.driver.mock.DummyStorageDriverMock.<Item, Operation<Item>> create();
		final LoadStepContextImpl<Item, Operation<Item>> ctx = new LoadStepContextImpl<>(
						"t",
						null,
						driver,
						fakeMetrics,
						minimalLoadConfig(),
						false);
		// Build a shard/window state
		final ListShard shard = new ListShard("abc/", null, null, null);
		final ListOperation<?> listOp = newListOp("/bucket", "abc/000", "abc/999", shard);
		// Reflectively invoke tryDelimiterSplit
		final Method m = LoadStepContextImpl.class.getDeclaredMethod("tryDelimiterSplit", ListShard.class, ListOperation.class);
		m.setAccessible(true);
		final Object ret = assertDoesNotThrow(() -> m.invoke(ctx, shard, listOp));
		assertEquals(Boolean.FALSE, ret, "Gate should skip split when concurrency is at limit");
	}

	private static Config minimalLoadConfig() {
		final java.util.Map<String, Object> schema = new java.util.HashMap<>();
		// batch-size => batch -> size
		schema.put("batch", java.util.Map.of("size", Integer.class));
		// item.type
		schema.put("item", java.util.Map.of("type", String.class));
		// step-limit => step -> limit
		schema.put("step", java.util.Map.of("limit", java.util.Map.of("size", Object.class, "time", Object.class)));
		// op subtree
		final java.util.Map<String, Object> failSchema = new java.util.HashMap<>();
		failSchema.put("count", Long.class);
		failSchema.put("rate", Boolean.class);
		final java.util.Map<String, Object> limitSchema = new java.util.HashMap<>();
		limitSchema.put("count", Long.class);
		limitSchema.put("recycle", Integer.class);
		limitSchema.put("fail", failSchema);
		final java.util.Map<String, Object> recycleSchema = new java.util.HashMap<>();
		recycleSchema.put("mode", Boolean.class);
		recycleSchema.put("content", java.util.Map.of("update", Boolean.class));
		final java.util.Map<String, Object> waitSchema = new java.util.HashMap<>();
		waitSchema.put("finish", Boolean.class);
		waitSchema.put("limit", Integer.class);
		final java.util.Map<String, Object> outputSchema = new java.util.HashMap<>();
		outputSchema.put("duplicates", Boolean.class);
		final java.util.Map<String, Object> opSchema = new java.util.HashMap<>();
		opSchema.put("type", String.class);
		opSchema.put("limit", limitSchema);
		opSchema.put("recycle", recycleSchema);
		opSchema.put("retry", Boolean.class);
		opSchema.put("shuffle", Boolean.class);
		opSchema.put("wait", waitSchema);
		opSchema.put("output", outputSchema);
		schema.put("op", opSchema);

		final java.util.Map<String, Object> values = new java.util.HashMap<>();
		values.put("batch", java.util.Map.of("size", 1000));
		values.put("step", java.util.Map.of("limit", java.util.Map.of("size", 0, "time", 0)));
		final java.util.Map<String, Object> limitVals = new java.util.HashMap<>();
		limitVals.put("count", 0L);
		limitVals.put("recycle", 0);
		limitVals.put("fail", java.util.Map.of("count", 0L, "rate", false));
		final java.util.Map<String, Object> opVals = new java.util.HashMap<>();
		opVals.put("type", "list");
		opVals.put("limit", limitVals);
		opVals.put("recycle", java.util.Map.of("mode", false, "content", java.util.Map.of("update", false)));
		opVals.put("retry", false);
		opVals.put("shuffle", false);
		opVals.put("wait", java.util.Map.of("finish", false, "limit", 0));
		opVals.put("output", java.util.Map.of("duplicates", false));
		values.put("op", opVals);
		values.put("item", java.util.Map.of("type", "path"));
		return new BasicConfig("-", schema, values);
	}
}
