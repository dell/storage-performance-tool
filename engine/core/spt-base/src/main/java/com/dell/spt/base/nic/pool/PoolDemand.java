package com.dell.spt.base.nic.pool;

import com.dell.spt.base.nic.pool.ItemSizeSummary.Bounds;
import com.dell.spt.base.util.BinarySizeFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Pool Demand (item 14a, N4): the RDMA buffers a step can hold at once, per NIC and size class, from
 * what {@code S3RdmaStorageDriver.selectPath} would send over RDMA, the transfer sizes and how many
 * can be in flight together. One estimate, which the engine's check, Pool Pre-warm and through the
 * image command the CLI's sizing and {@code spt verify} all use.
 */
public final class PoolDemand {

	/**
	 * Direct memory one engine keeps beyond its pools for Netty and per-operation buffers (N5). The
	 * SC26 sizing used this, and its four-engine runs went without a direct-memory failure.
	 */
	public static final long PROCESS_HEADROOM_BYTES = 8L << 30;

	/** {@code RdmaBufferPool}'s constant: a pool may hold 1/n of the JVM's direct-memory limit. */
	public static final int DIRECT_MEMORY_SHARE_DIVISOR = 2;

	/** {@code S3RdmaStorageDriver}'s class cap without a concurrency limit. */
	public static final int UNLIMITED_CAP = 256;

	/** Zero, a known count, or unknown. */
	public enum State {
		ZERO, KNOWN, UNKNOWN
	}

	/** The operations that can use RDMA; every other operation has zero demand. */
	public enum OpKind {
		CREATE("PUT"), READ("GET"), OTHER("other");

		private final String label;

		OpKind(final String label) {
			this.label = label;
		}

		public String label() {
			return label;
		}
	}

	/** Where an operation's sizes come from. */
	public sealed

	interface Sizes
	permits Range, Listed, NotKnown
	{}

	/** A fixed size or a range, from the configuration or from the step that writes the file. */
	public record Range(long min, long max, String source) implements Sizes {}

	/** The sizes in an item list. */
	public record Listed(ItemSizeSummary summary) implements Sizes {}

	/** An item input other than a file, such as a bucket listing. */
	public record NotKnown(String reason) implements Sizes {}

	/**
	 * One operation type of a driver.
	 *
	 * @param partBytes {@code item.data.ranges.threshold}: an item above it is split, a CREATE into
	 *     parts that go over RDMA at this size, a READ into range requests that go over HTTP; 0 for none
	 * @param ranged a READ with fixed, random or single ranges, which goes over HTTP whatever its size
	 */
	public record Operation(OpKind kind, long partBytes, boolean ranged, Sizes sizes) {}

	/**
	 * One driver's workload.
	 *
	 * @param concurrency {@code P}; 0 or less is unlimited
	 * @param thresholdBytes {@code storage.rdma.thresholdBytes}
	 * @param multipartObjects {@code storage.driver.limit.multipart.objects}; 0 is unlimited
	 * @param multipartParts {@code storage.driver.limit.multipart.parts}; 0 is unlimited
	 */
	public record Workload(int concurrency, long thresholdBytes, int multipartObjects, int multipartParts, List<Operation> operations) {

		public Workload {
			operations = List.copyOf(operations);
		}
	}

	/**
	 * What one operation type adds.
	 *
	 * @param smallest the smallest eligible transfer, or 0 without one
	 * @param largest the largest eligible transfer, or 0 without one
	 * @param countLimit the most in flight at once on any NIC after the multipart limits
	 * @param source where its sizes came from: {@code size}, the file, or the step that writes it
	 * @param reason why its demand is unknown, or empty
	 */
	public record Contribution(OpKind kind, State state, long smallest, long largest, int countLimit, String source, String reason) {

		public int smallestClass() {
			return state == State.KNOWN ? PoolSizeClass.of(smallest) : 0;
		}

		public int largestClass() {
			return state == State.KNOWN ? PoolSizeClass.of(largest) : 0;
		}
	}

	/**
	 * One NIC's demand. Its need is {@code count × sizeClass}, of the known contributions.
	 *
	 * @param minimum the buffers per size class its share requires, without the multipart limits
	 * @param cap the most buffers it holds per size class
	 * @param count how many buffers of {@code sizeClass} it can hold at once
	 */
	public record NicDemand(String nic, State state, int minimum, int cap, int count, int sizeClass, long needBytes) {

	public int headroom() {
		return cap - minimum;
	}

	}

	/** One driver's demand. */
	public record DriverDemand(List<Contribution> contributions, List<NicDemand> nics, boolean sharedMode) {

		public DriverDemand {
			contributions = List.copyOf(contributions);
			nics = List.copyOf(nics);
		}

	public long maxNeedBytes() {
		return nics.stream().mapToLong(NicDemand::needBytes).max().orElse(0);
	}

	/** What this driver's pools can hold together: {@code N × max_k(need_k)} (N5). */
	public long poolsNeedBytes() {
		return nics.size() * maxNeedBytes();
	}

	/** The smallest limit whose half, split {@code N} ways, holds every pool. */
	public long limitForBudgetBytes() {
		return DIRECT_MEMORY_SHARE_DIVISOR * poolsNeedBytes();
	}

	public boolean hasKnown() {
		return contributions.stream().anyMatch(c -> c.state() == State.KNOWN);
	}

	public boolean hasUnknown() {
		return contributions.stream().anyMatch(c -> c.state() == State.UNKNOWN);
	}

	public State state() {
		return hasKnown() ? State.KNOWN : hasUnknown() ? State.UNKNOWN : State.ZERO;
	}

	/** Whether its known transfers fall in more than one class, so it is checked with the largest and not pre-warmed. */
	public boolean spansClasses() {
		final var known = contributions.stream().filter(c -> c.state() == State.KNOWN).toList();
		if (known.isEmpty()) {
			return false;
		}
		final int largest = known.get(0).largestClass();
		return known.stream().anyMatch(c -> c.largestClass() != largest || c.smallestClass() != largest);
	}

	/** Each unknown contribution's reason, naming its operation. */
	public List<String> unknownReasons() {
		final var reasons = new ArrayList<String>();
		contributions.stream()
						.filter(c -> c.state() == State.UNKNOWN)
						.forEach(c -> reasons.add(c.kind().label() + ": " + c.reason()));
		return reasons;
	}

	}

	/** The drivers that run at once in one step; sequential steps are separate. */
	public record StepDemand(List<DriverDemand> drivers) {

		public StepDemand {
			drivers = List.copyOf(drivers);
		}

	/** {@code Σ_d N_d × max_k(need_k)} of the known contributions. */
	public long poolsNeedBytes() {
		return drivers.stream().mapToLong(DriverDemand::poolsNeedBytes).sum();
	}

	/**
	 * The smallest {@code MaxDirectMemorySize} that passes both checks for the known contributions,
	 * as a lower bound when part is unknown (T3); 0 when no pool needs a buffer.
	 */
	public long requiredLimitBytes(final long headroomBytes) {
		final long pools = poolsNeedBytes();
		if (pools == 0) {
			return 0;
		}
		final long budget = drivers.stream().mapToLong(DriverDemand::limitForBudgetBytes).max().orElse(0);
		return Math.max(budget, pools + headroomBytes);
	}

	public List<String> unknownReasons() {
		final var reasons = new ArrayList<String>();
		drivers.forEach(driver -> reasons.addAll(driver.unknownReasons()));
		return List.copyOf(reasons);
	}

	public boolean hasKnown() {
		return drivers.stream().anyMatch(DriverDemand::hasKnown);
	}

	public boolean hasUnknown() {
		return drivers.stream().anyMatch(DriverDemand::hasUnknown);
	}

	}

	/** A NIC Pool whose need exceeds its budget. */
	public record PoolShort(int driver, String nic, long needBytes, long budgetBytes) {}

	/**
	 * The two results, apart. An unknown part can only add to what this finds short.
	 *
	 * @param poolsShort every pool beyond its driver's budget split {@code N} ways
	 * @param processNeedBytes the concurrent drivers' pools plus the headroom; 0 when no pool needs a buffer
	 * @param processShort whether that exceeds the limit
	 */
	public record Check(long limitBytes, List<PoolShort> poolsShort, long processNeedBytes, boolean processShort, long headroomBytes) {

		public Check {
			poolsShort = List.copyOf(poolsShort);
		}

	public boolean passed() {
		return poolsShort.isEmpty() && !processShort;
	}

	}

	private PoolDemand() {}

	/** One NIC's budget: its driver's share of the limit, split equally (item 14). */
	public static long nicBudgetBytes(final long limitBytes, final int nics) {
		return limitBytes / DIRECT_MEMORY_SHARE_DIVISOR / nics;
	}

	/** The buffers per size class NIC {@code index} requires: its share, or {@code P} in the shared mode; 0 when unlimited. */
	public static int minimum(final int concurrency, final int nics, final int index) {
		if (concurrency <= 0) {
			return 0;
		}
		if (concurrency < nics) {
			return concurrency;
		}
		return concurrency / nics + (index < concurrency % nics ? 1 : 0);
	}

	/** The most buffers NIC {@code index} holds per size class (E7). */
	public static int cap(final int concurrency, final int nics, final int index) {
		return concurrency <= 0 ? UNLIMITED_CAP : minimum(concurrency, nics, index) + 1;
	}

	/** Estimates one driver's demand on {@code nics}, in layout order. */
	public static DriverDemand estimate(final Workload workload, final List<String> nics) {
		final var contributions = workload.operations().stream().map(operation -> contribution(workload, operation)).toList();
		final var known = contributions.stream().filter(c -> c.state() == State.KNOWN).toList();
		final int sizeClass = known.stream().mapToInt(Contribution::largestClass).max().orElse(0);
		final boolean unknown = contributions.stream().anyMatch(c -> c.state() == State.UNKNOWN);
		final int count = nics.size();
		final var demands = new ArrayList<NicDemand>(count);
		for (var i = 0; i < count; i++) {
			final int minimum = minimum(workload.concurrency(), count, i);
			final int cap = cap(workload.concurrency(), count, i);
			if (known.isEmpty()) {
				demands.add(new NicDemand(nics.get(i), unknown ? State.UNKNOWN : State.ZERO, minimum, cap, 0, 0, 0));
				continue;
			}
			final int held = known.stream().mapToInt(c -> Math.min(minimum, c.countLimit())).max().orElse(0);
			demands.add(new NicDemand(nics.get(i), State.KNOWN, minimum, cap, held, sizeClass, Math.multiplyExact((long) held, (long) sizeClass)));
		}
		final boolean shared = workload.concurrency() > 0 && workload.concurrency() < count;
		return new DriverDemand(contributions, demands, shared);
	}

	private static Contribution contribution(final Workload workload, final Operation operation) {
		if (operation.kind() == OpKind.OTHER || (operation.kind() == OpKind.READ && operation.ranged())) {
			return zero(operation, "");
		}
		return switch (operation.sizes()) {
			case NotKnown notKnown -> unknown(operation, "", notKnown.reason());
			case Range range -> ofBounds(workload, operation, List.of(new Bounds(range.min(), range.max())), range.source());
			case Listed listed -> {
				final var summary = listed.summary();
				yield summary.complete()
								? ofBounds(workload, operation, List.copyOf(summary.bounds().values()), summary.source())
								: unknown(operation, summary.source(), summary.why());
			}
		};
	}

	private static Contribution ofBounds(
					final Workload workload, final Operation operation, final List<Bounds> sizes, final String source) {
		long smallest = Long.MAX_VALUE;
		long largest = 0;
		for (final Bounds bounds : sizes) {
			final long[] eligible = eligible(operation, bounds, workload.thresholdBytes());
			if (eligible != null) {
				smallest = Math.min(smallest, eligible[0]);
				largest = Math.max(largest, eligible[1]);
			}
		}
		if (largest == 0 && smallest == Long.MAX_VALUE) {
			return zero(operation, source);
		}
		if (workload.concurrency() <= 0) {
			return new Contribution(operation.kind(), State.UNKNOWN, 0, 0, 0, source, "unlimited concurrency");
		}
		return new Contribution(operation.kind(), State.KNOWN, smallest, largest, countLimit(workload, operation, sizes), source, "");
	}

	/**
	 * Applies the operation's transfer shape, then the threshold and the pool's limit, to a run of
	 * sizes. Returns its smallest and largest eligible transfer, or {@code null}. A part's tail is
	 * counted at the part size, which is the demand a part size names.
	 */
	private static long[] eligible(final Operation operation, final Bounds sizes, final long thresholdBytes) {
		long low = sizes.smallest();
		long high = sizes.largest();
		final long part = operation.partBytes();
		if (part > 0) {
			if (operation.kind() == OpKind.READ) {
				if (low > part) {
					return null;
				}
				high = Math.min(high, part);
			} else {
				low = Math.min(low, part);
				high = Math.min(high, part);
			}
		}
		if (high < thresholdBytes) {
			return null;
		}
		low = Math.max(low, thresholdBytes);
		if (low > PoolSizeClass.MAX_BYTES) {
			return null;
		}
		return new long[] {low, Math.min(high, PoolSizeClass.MAX_BYTES)};
	}

	/**
	 * The multipart limits on parts in flight, when every object is split: both limits multiply, and
	 * the object limit alone allows each object's parts, while the part limit alone still fills the
	 * concurrency limit (as {@code CoopStorageDriverBase} schedules them).
	 */
	private static int countLimit(final Workload workload, final Operation operation, final List<Bounds> sizes) {
		final long part = operation.partBytes();
		final long smallestSize = sizes.stream().mapToLong(Bounds::smallest).min().orElse(0);
		if (operation.kind() != OpKind.CREATE || part <= 0 || smallestSize <= part) {
			return Integer.MAX_VALUE;
		}
		final int objects = workload.multipartObjects();
		if (objects <= 0) {
			return Integer.MAX_VALUE;
		}
		final long largestSize = sizes.stream().mapToLong(Bounds::largest).max().orElse(0);
		final long perObject = workload.multipartParts() > 0 ? workload.multipartParts() : (largestSize + part - 1) / part;
		return (int) Math.min(Integer.MAX_VALUE, objects * perObject);
	}

	private static Contribution zero(final Operation operation, final String source) {
		return new Contribution(operation.kind(), State.ZERO, 0, 0, 0, source, "");
	}

	private static Contribution unknown(final Operation operation, final String source, final String reason) {
		return new Contribution(operation.kind(), State.UNKNOWN, 0, 0, 0, source, reason);
	}

	/**
	 * Why a driver's pools are not pre-warmed (item 14c), or empty when they are: each NIC's count in
	 * the one class of its known demand. Zero demand has nothing to warm; the others start cold.
	 */
	public static String prewarmSkip(final DriverDemand demand) {
		if (demand.state() == State.ZERO) {
			return "no demand";
		}
		if (demand.hasUnknown()) {
			return "its demand is unknown";
		}
		if (demand.spansClasses()) {
			return "its sizes span size classes";
		}
		if (demand.sharedMode()) {
			return "the shared mode";
		}
		return "";
	}

	/** Runs both checks against {@code limitBytes}. Only known contributions count (T3). */
	public static Check check(final StepDemand step, final long limitBytes, final long headroomBytes) {
		final var poolsShort = new ArrayList<PoolShort>();
		for (var d = 0; d < step.drivers().size(); d++) {
			final var driver = step.drivers().get(d);
			final long budget = driver.nics().isEmpty() ? 0 : nicBudgetBytes(limitBytes, driver.nics().size());
			for (final var nic : driver.nics()) {
				if (nic.needBytes() > budget) {
					poolsShort.add(new PoolShort(d, nic.nic(), nic.needBytes(), budget));
				}
			}
		}
		final long pools = step.poolsNeedBytes();
		final long processNeed = pools == 0 ? 0 : pools + headroomBytes;
		return new Check(limitBytes, poolsShort, processNeed, processNeed > limitBytes, headroomBytes);
	}

	/** {@code bytes} as the logs and messages show it, such as {@code 24GiB}. */
	public static String format(final long bytes) {
		return BinarySizeFormat.formatFixedSize(bytes);
	}
}
