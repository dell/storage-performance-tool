package com.dell.spt.perf;

import com.dell.spt.base.item.ItemNamePaths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Hand-written microbenchmark for the Class B record of the shared item name rule. It compares the
 * request URI and recorded name logic before and after the rule, interleaved within each round so
 * that machine drift hits both arms. It runs as a plain program on the test classpath:
 *
 * <pre>
 * java -cp &lt;main and test classes&gt; com.dell.spt.perf.ItemNamePathsBench [rounds] [roundMillis]
 * </pre>
 *
 * <p>The "before" methods keep the logic this rule replaced. The "after" methods mirror {@code
 * HttpStorageDriverBase.dataUriPath} and {@code Operation.buildItemPath}, which live on other
 * classpaths or need an item instance.
 */
public final class ItemNamePathsBench {

	private static final int BATCH = 4096;
	private static final String SLASH = "/";
	private static final String PATH = "/bucket-0001";
	private static long sink;

	@FunctionalInterface
	private interface Arm {
		String apply(String name, String path);
	}

	private static final class Case {
		final String name;
		final String[] names;
		final Arm before;
		final Arm after;

		Case(final String name, final String[] names, final Arm before, final Arm after) {
			this.name = name;
			this.names = names;
			this.before = before;
			this.after = after;
		}
	}

	private ItemNamePathsBench() {}

	public static void main(final String... args) {
		final int rounds = args.length > 0 ? Integer.parseInt(args[0]) : 31;
		final long roundMillis = args.length > 1 ? Long.parseLong(args[1]) : 500;
		final String[] generated = new String[BATCH];
		final String[] recorded = new String[BATCH];
		final String[] repeating = new String[BATCH];
		for (int i = 0; i < BATCH; i++) {
			final String id = Long.toString(0x3fffffffL * i + 12345, 36);
			generated[i] = "s000000" + (i % 8) + SLASH + id;
			recorded[i] = PATH + SLASH + generated[i];
			// A generated key that repeats the bucket: before the fix, the request lost the prefix.
			repeating[i] = PATH.substring(1) + SLASH + generated[i];
		}
		final List<Case> cases = List.of(
						new Case("uri/generated", generated,
										ItemNamePathsBench::uriBefore, ItemNamePathsBench::uriAfter),
						new Case("uri/recorded", recorded,
										ItemNamePathsBench::uriBefore, ItemNamePathsBench::uriAfter),
						new Case("uri/repeating", repeating,
										ItemNamePathsBench::uriBefore, ItemNamePathsBench::uriAfter),
						new Case("record/generated", generated,
										ItemNamePathsBench::recordBefore, ItemNamePathsBench::recordAfter),
						new Case("record/recorded", recorded,
										ItemNamePathsBench::recordBefore, ItemNamePathsBench::recordAfter));
		System.out.println("# " + System.getProperty("java.vm.name") + " "
						+ System.getProperty("java.version") + ", " + Runtime.getRuntime().availableProcessors()
						+ " CPUs, rounds=" + rounds + ", roundMillis=" + roundMillis);
		for (final Case benchCase : cases) {
			for (int warm = 0; warm < 10; warm++) {
				measure(benchCase.names, benchCase.before, roundMillis);
				measure(benchCase.names, benchCase.after, roundMillis);
			}
			final List<Double> before = new ArrayList<>(rounds);
			final List<Double> after = new ArrayList<>(rounds);
			final List<Double> delta = new ArrayList<>(rounds);
			for (int round = 0; round < rounds; round++) {
				final boolean beforeFirst = round % 2 == 0;
				final double first = measure(benchCase.names, beforeFirst ? benchCase.before : benchCase.after, roundMillis);
				final double second = measure(benchCase.names, beforeFirst ? benchCase.after : benchCase.before, roundMillis);
				final double beforeNs = beforeFirst ? first : second;
				final double afterNs = beforeFirst ? second : first;
				before.add(beforeNs);
				after.add(afterNs);
				delta.add(afterNs - beforeNs);
			}
			System.out.printf(Locale.ROOT,
							"%-18s before %7.2f ns  after %7.2f ns  delta median %6.2f ns  IQR [%6.2f, %6.2f]%n",
							benchCase.name, quantile(before, 0.5), quantile(after, 0.5),
							quantile(delta, 0.5), quantile(delta, 0.25), quantile(delta, 0.75));
		}
		System.out.println("# checksum " + sink);
	}

	private static double measure(final String[] names, final Arm arm, final long roundMillis) {
		final long deadline = System.nanoTime() + roundMillis * 1_000_000L;
		final long start = System.nanoTime();
		long calls = 0;
		long now;
		do {
			for (final String name : names) {
				sink += arm.apply(name, PATH).length();
			}
			calls += names.length;
			now = System.nanoTime();
		} while (now < deadline);
		return (double) (now - start) / calls;
	}

	private static double quantile(final List<Double> values, final double q) {
		final double[] sorted = values.stream().mapToDouble(Double::doubleValue).toArray();
		Arrays.sort(sorted);
		return sorted[(int) Math.round(q * (sorted.length - 1))];
	}

	private static String uriBefore(final String nameRaw, final String path) {
		final String itemPath = path.startsWith(SLASH) ? path : SLASH + path;
		final String itemName = nameRaw.startsWith(SLASH) ? nameRaw : SLASH + nameRaw;
		if (itemName.startsWith(itemPath)) {
			return itemName;
		}
		return itemPath.endsWith(SLASH) ? itemPath + itemName.substring(1) : itemPath + itemName;
	}

	private static String uriAfter(final String nameRaw, final String path) {
		if (ItemNamePaths.holdsPath(nameRaw, path)) {
			return nameRaw.startsWith(SLASH) ? nameRaw : SLASH + nameRaw;
		}
		return ItemNamePaths.join(path.startsWith(SLASH) ? path : SLASH + path, nameRaw);
	}

	private static String recordBefore(final String name, final String path) {
		if (!name.startsWith(path)) {
			return path.endsWith(SLASH) ? path + name : path + SLASH + name;
		}
		return name;
	}

	private static String recordAfter(final String name, final String path) {
		return ItemNamePaths.startsWithPath(name, path) ? name : ItemNamePaths.join(path, name);
	}
}
