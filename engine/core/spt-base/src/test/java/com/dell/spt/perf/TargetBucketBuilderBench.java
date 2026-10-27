package com.dell.spt.perf;

import com.dell.spt.base.config.ConstantValueInputImpl;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationsBuilderImpl;
import com.dell.spt.base.item.op.data.DataOperationsBuilderImpl;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Hand-written microbenchmark of operation construction with and without a target bucket set, for
 * the multi-bucket Class B record. It runs as a plain program on the test classpath:
 *
 * <pre>
 * java -cp &lt;test runtime classpath&gt; com.dell.spt.perf.TargetBucketBuilderBench [rounds] [roundMillis]
 * </pre>
 *
 * <p>Each case reports the median and interquartile range of nanoseconds per operation over the
 * measured rounds, after a warm-up. Target bucket sets are configured by reflection so the same
 * source runs against a build without them, which then reports only its one-bucket cases.
 */
public final class TargetBucketBuilderBench {

	private static final int BATCH = 4096;
	private static long sink;
	/** Keeps every single-mode operation reachable, so the JIT cannot scalar-replace its construction. */
	private static final Object[] SINGLE_SINK = new Object[BATCH];

	private TargetBucketBuilderBench() {}

	public static void main(final String... args) throws Exception {
		final int rounds = args.length > 0 ? Integer.parseInt(args[0]) : 15;
		final long roundMillis = args.length > 1 ? Long.parseLong(args[1]) : 1000;
		final String only = args.length > 2 ? args[2] : "";
		final Method members = membersMethod();
		final List<DataItem> items = new ArrayList<>(BATCH);
		for (int i = 0; i < BATCH; i++) {
			final long id = 0x3fffffffL * i + 12345;
			items.add(new DataItemImpl(Long.toString(id, 36), id, 1024));
		}
		System.out.println("# target bucket sets supported: " + (members != null));
		for (final String kind : List.of("ops", "data")) {
			for (final String mode : List.of("batch", "single")) {
				for (final int count : new int[]{1, 16
				}) {
					for (final int shards : new int[]{0, 64
					}) {
						if (count == 1 && shards == 64) {
							continue;
						}
						if (count > 1 && members == null) {
							continue;
						}
						final var name = kind + "/" + mode + "/N=" + count + "/P=" + shards;
						if (!name.contains(only)) {
							continue;
						}
						run(name, builder(kind, count, shards, members), items, mode, rounds, roundMillis);
					}
				}
			}
		}
		System.out.println("# checksum " + sink);
	}

	private static Method membersMethod() {
		try {
			return OperationsBuilderImpl.class.getMethod("targetBucketMembers", List.class, int.class);
		} catch (final NoSuchMethodException e) {
			return null;
		}
	}

	@SuppressWarnings({"rawtypes", "unchecked"
	})
	private static OperationsBuilderImpl<DataItem, Operation<DataItem>> builder(
					final String kind, final int count, final int shards, final Method members) throws Exception {
		final OperationsBuilderImpl builder = "data".equals(kind) ? new DataOperationsBuilderImpl(0) : new OperationsBuilderImpl(0);
		builder.opType(OpType.CREATE);
		builder.outputPathInput(new ConstantValueInputImpl<>("/bench"));
		builder.credentialInput(null);
		if (count > 1) {
			final List<String> paths = new ArrayList<>(count);
			for (int i = 0; i < count; i++) {
				paths.add("/bench-" + i);
			}
			members.invoke(builder, paths, shards);
		}
		return builder;
	}

	private static void run(
					final String name,
					final OperationsBuilderImpl<DataItem, Operation<DataItem>> builder,
					final List<DataItem> items,
					final String mode,
					final int rounds,
					final long roundMillis) throws Exception {
		final List<Operation<DataItem>> buff = new ArrayList<>(BATCH);
		// Warm-up: as long as five measured rounds.
		measure(builder, items, mode, buff, 5 * roundMillis);
		final double[] nanosPerOp = new double[rounds];
		for (int r = 0; r < rounds; r++) {
			nanosPerOp[r] = measure(builder, items, mode, buff, roundMillis);
		}
		Arrays.sort(nanosPerOp);
		System.out.printf(Locale.ROOT, "%-24s median %7.2f ns/op  q1 %7.2f  q3 %7.2f%n",
						name, nanosPerOp[rounds / 2], nanosPerOp[rounds / 4], nanosPerOp[(3 * rounds) / 4]);
	}

	private static double measure(
					final OperationsBuilderImpl<DataItem, Operation<DataItem>> builder,
					final List<DataItem> items,
					final String mode,
					final List<Operation<DataItem>> buff,
					final long millis) throws Exception {
		final long deadline = System.nanoTime() + millis * 1_000_000L;
		long ops = 0;
		long hash = 0;
		final long start = System.nanoTime();
		long now;
		do {
			if ("batch".equals(mode)) {
				buff.clear();
				builder.buildOps(items, buff);
				hash += System.identityHashCode(buff.get(BATCH - 1).dstPath());
			} else {
				for (int i = 0; i < BATCH; i++) {
					final var op = builder.buildOp(items.get(i));
					SINGLE_SINK[i] = op;
					hash += System.identityHashCode(op.dstPath());
				}
			}
			ops += BATCH;
			now = System.nanoTime();
		} while (now < deadline);
		sink += hash;
		return (now - start) / (double) ops;
	}
}
