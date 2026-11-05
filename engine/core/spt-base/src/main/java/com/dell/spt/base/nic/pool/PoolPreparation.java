package com.dell.spt.base.nic.pool;

import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.ItemType;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.nic.pool.PoolDemand.DriverDemand;
import com.dell.spt.base.nic.pool.PoolDemand.State;
import com.dell.spt.base.nic.pool.PoolDemand.StepDemand;
import com.dell.spt.base.storage.driver.AdmissionPreparable;
import com.dell.spt.base.storage.driver.PoolDemandDriver;
import com.github.akurilov.confuse.Config;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * One step's pool work in the barrier's preparation (items 14a and 14c), once every driver has
 * started and before admission opens: it reads item sizes if the step holds no summary, computes the
 * demand with the plan this engine resolved, runs the two checks, applies the shortfall policy, and
 * pre-warms each NIC's minimum on every lane at once.
 */
public final class PoolPreparation {

	/**
	 * The CLI's estimate for this engine, per step: {@code <step id>=<bytes>[,...]}. It is set per
	 * container, as the JVM option it goes with, and only ever logged against.
	 */
	public static final String ESTIMATE_ENV = "SPT_RDMA_POOL_ESTIMATE";

	/** One context with NIC Pools. */
	public record Participant(int contextIndex, PoolDemandDriver driver, Config config, boolean mixed) {

		public Participant(final int contextIndex, final PoolDemandDriver driver, final Config config) {
			this(contextIndex, driver, config, false);
		}
	}

	/** Where this runs: the clock, and the environment holding the CLI's estimate. */
	public record Environment(LongSupplier nanoTime, Function<String, String> env) {

		public static Environment system() {
			return new Environment(System::nanoTime, System::getenv);
		}
	}

	/** One NIC, for the run metadata. */
	public record NicReport(
					String nic, State state, int minimum, int cap, int headroom, long budgetBytes, int count, int sizeClass, long needBytes,
					int prewarmed, long prewarmMillis, String prewarmNote) {}

	/** One driver, for the run metadata. */
	public record DriverReport(int contextIndex, String shortfall, DriverDemand demand, List<NicReport> nics) {

		public DriverReport {
			nics = List.copyOf(nics);
		}
	}

	/**
	 * The step's results, for the run metadata.
	 *
	 * @param check the two check results over the known contributions
	 * @param requiredLimitBytes the limit the step needs
	 * @param partial whether part of the demand is unknown
	 * @param estimateBytes the CLI's estimate for this step, or -1 without one
	 * @param prewarmMillis the longest NIC's pre-warm time
	 */
	public record Report(
					List<DriverReport> drivers, PoolDemand.Check check, long requiredLimitBytes, boolean partial, List<String> unknown,
					long estimateBytes, long prewarmMillis) {

		public Report {
			drivers = List.copyOf(drivers);
			unknown = List.copyOf(unknown);
		}
	}

	private record Active(Participant participant, PoolDemandDriver.Layout layout, DriverDemand demand) {}

	private PoolPreparation() {}

	/**
	 * Runs the step's pool work.
	 *
	 * @return the results, or {@code null} when no driver has NIC Pools
	 * @throws RunTerminalException when a short pool fails the step under {@code fail}
	 * @throws InterruptedException when a stop or a timeout ended the work
	 */
	public static Report run(
					final String stepId, final List<Participant> participants, final AdmissionPreparable.Preparation preparation,
					final Environment environment) throws InterruptedException {
		final var layouts = new ArrayList<Participant>(participants.size());
		for (final var participant : participants) {
			if (participant.driver().poolLayout() != null) {
				layouts.add(participant);
			}
		}
		if (layouts.isEmpty()) {
			return null;
		}
		preparation.phase("reading item sizes");
		final var active = new ArrayList<Active>(layouts.size());
		for (final var participant : layouts) {
			active.add(demandOf(stepId, participant, preparation, environment));
			if (Thread.currentThread().isInterrupted()) {
				throw new InterruptedException("Interrupted while reading item sizes");
			}
		}
		preparation.phase("checking pool demand");
		final var step = new StepDemand(active.stream().map(Active::demand).toList());
		final long limit = active.get(0).layout().limitBytes();
		final var check = PoolDemand.check(step, limit, PoolDemand.PROCESS_HEADROOM_BYTES);
		final long required = step.requiredLimitBytes(PoolDemand.PROCESS_HEADROOM_BYTES);
		final long estimate = logDiscrepancy(stepId, required, environment);
		applyPolicy(stepId, active, step, check, required);
		preparation.phase("pre-warming pools");
		final var prewarmed = prewarm(stepId, active);
		final var report = report(active, step, check, required, estimate, prewarmed);
		log(stepId, report);
		return report;
	}

	private static Active demandOf(
					final String stepId, final Participant participant, final AdmissionPreparable.Preparation preparation,
					final Environment environment) {
		final var layout = participant.driver().poolLayout();
		final var config = participant.config();
		final long now = environment.nanoTime().getAsLong();
		final long remaining = Math.max(0, preparation.deadlineNanos() - now);
		final long budgetEnd = now + remaining / ItemSizeRecording.BUDGET_DIVISOR;
		final Function<String, ItemSizeSummary> reader = file -> scan(stepId, file, config, budgetEnd, environment);
		final var operations = participant.mixed()
						? PoolDemandConfig.mixedOperations(config, reader)
						: PoolDemandConfig.operations(config, reader);
		final var workload = new PoolDemand.Workload(
						layout.concurrency(), layout.thresholdBytes(), layout.multipartObjects(), layout.multipartParts(), operations);
		return new Active(participant, layout, PoolDemand.estimate(workload, layout.nics()));
	}

	private static ItemSizeSummary scan(
					final String stepId, final String file, final Config config, final long budgetEnd, final Environment environment) {
		final var itemType = ItemType.valueOf(config.stringVal("item-type").toUpperCase(Locale.ROOT));
		final var summary = ItemSizeScan.scan(file, ItemType.getItemFactory(itemType), budgetEnd, environment.nanoTime());
		if (summary.complete()) {
			Loggers.MSG.info("{}: read the sizes in item file \"{}\" in {} ms: {} items, {} to {}", stepId, file, summary.millis(),
							summary.items(), PoolDemand.format(Math.max(0, summary.smallest())), PoolDemand.format(Math.max(0, summary.largest())));
		} else {
			Loggers.MSG.warn("{}: could not read the sizes in item file \"{}\" in {} ms: {}", stepId, file, summary.millis(), summary.why());
		}
		return summary;
	}

	/** Compares with the CLI's estimate, if any. It is only ever a log line; the engine's figure decides. */
	private static long logDiscrepancy(final String stepId, final long required, final Environment environment) {
		final long estimate = estimateFor(stepId, environment.env().apply(ESTIMATE_ENV));
		if (estimate >= 0 && estimate != required) {
			Loggers.MSG.warn("{}: pool demand discrepancy: the CLI estimated this step needs {} of direct memory, this engine's plan needs "
							+ "{}; the engine's figure decides", stepId, PoolDemand.format(estimate), PoolDemand.format(required));
		}
		return estimate;
	}

	/** The estimate for {@code stepId} in the {@link #ESTIMATE_ENV} form, or -1. */
	static long estimateFor(final String stepId, final String value) {
		if (value == null || value.isBlank()) {
			return -1;
		}
		final Map<String, Long> estimates = new HashMap<>();
		for (final String entry : value.split(",", -1)) {
			final int separator = entry.lastIndexOf('=');
			if (separator > 0) {
				try {
					estimates.put(entry.substring(0, separator).trim(), Long.parseLong(entry.substring(separator + 1).trim()));
				} catch (final NumberFormatException e) {
					Loggers.MSG.debug("{}: ignoring a malformed estimate in {}", stepId, ESTIMATE_ENV);
				}
			}
		}
		return estimates.getOrDefault(stepId, -1L);
	}

	private static void applyPolicy(
					final String stepId, final List<Active> active, final StepDemand step, final PoolDemand.Check check, final long required) {
		final String host = active.get(0).layout().host();
		final var unknown = step.unknownReasons();
		Loggers.MSG.info("{}: pool check: {} pool(s) short of their budgets; concurrent drivers need {} with {} headroom against "
						+ "MaxDirectMemorySize {}: {}; the step needs {}", stepId, check.poolsShort().size(),
						PoolDemand.format(step.poolsNeedBytes()), PoolDemand.format(check.headroomBytes()), PoolDemand.format(check.limitBytes()),
						check.processShort() ? "short" : "fits", PoolDemand.format(required));
		if (!step.hasKnown() && step.hasUnknown()) {
			Loggers.MSG.warn("{}: cannot check the RDMA pool budget on {}: {}. The pools fill on first use, and a short pool falls back to "
							+ "per-operation buffers", stepId, host, String.join("; ", unknown));
			return;
		}
		if (step.hasUnknown()) {
			Loggers.MSG.warn("{}: the RDMA pool check on {} is partial: it covers the known demand only, and this is unknown: {}", stepId, host,
							String.join("; ", unknown));
		}
		if (check.passed()) {
			return;
		}
		final var message = shortMessage(stepId, host, active, check, required);
		final boolean fail = check.processShort()
						? active.stream().anyMatch(driver -> driver.layout().failOnShortfall())
						: check.poolsShort().stream().anyMatch(pool -> active.get(pool.driver()).layout().failOnShortfall());
		if (fail) {
			throw new RunTerminalException(IntegrityTerminalException.Category.CONFIGURATION, message, null);
		}
		Loggers.MSG.warn("{}; the run goes on under {}={}, with per-operation buffers once a pool is full", message,
						PoolDemandConfig.SHORTFALL_SETTING, PoolDemandConfig.SHORTFALL_WARN);
	}

	private static String shortMessage(
					final String stepId, final String host, final List<Active> active, final PoolDemand.Check check, final long required) {
		final String what;
		if (!check.poolsShort().isEmpty()) {
			final var pool = check.poolsShort().get(0);
			final var nics = active.get(pool.driver()).demand().nics().size();
			final var more = check.poolsShort().size() > 1 ? " (and " + (check.poolsShort().size() - 1) + " more)" : "";
			what = "NIC " + pool.nic() + "'s pool needs " + PoolDemand.format(pool.needBytes()) + " but has "
							+ PoolDemand.format(pool.budgetBytes()) + more + ": half of MaxDirectMemorySize "
							+ PoolDemand.format(check.limitBytes()) + ", split among " + nics + " NIC(s)";
		} else {
			what = "the step's concurrent RDMA drivers need " + PoolDemand.format(check.processNeedBytes() - check.headroomBytes()) + " plus "
							+ PoolDemand.format(check.headroomBytes()) + " headroom, but MaxDirectMemorySize is " + PoolDemand.format(check.limitBytes());
		}
		return "Step " + stepId + " on host " + host + ": " + what + ". Set -XX:MaxDirectMemorySize=" + required + " in SPT_JAVA_OPTS, "
						+ "or " + PoolDemandConfig.SHORTFALL_SETTING + "=" + PoolDemandConfig.SHORTFALL_WARN + " to run with per-operation buffers";
	}

	/** Starts every lane of every driver, then waits for them all. */
	private static Map<String, PoolDemandDriver.PrewarmResult> prewarm(
					final String stepId, final List<Active> active) throws InterruptedException {
		final var running = new ArrayList<CompletableFuture<PoolDemandDriver.PrewarmResult>>();
		final var owners = new ArrayList<Active>();
		for (final var driver : active) {
			final var orders = orders(driver);
			if (!orders.isEmpty()) {
				for (final var future : driver.participant().driver().startPrewarm(orders)) {
					running.add(future);
					owners.add(driver);
				}
			}
		}
		final Map<String, PoolDemandDriver.PrewarmResult> results = new HashMap<>();
		for (var i = 0; i < running.size(); i++) {
			final PoolDemandDriver.PrewarmResult result;
			try {
				result = running.get(i).get();
			} catch (final ExecutionException e) {
				throw new IllegalStateException(e.getCause());
			}
			if (result.stopped()) {
				throw new InterruptedException("Pool pre-warm stopped");
			}
			final var driver = owners.get(i);
			results.put(key(driver, result.nic()), result);
			if (result.failure() != null) {
				final var message = "Step " + stepId + " on host " + driver.layout().host() + ": NIC " + result.nic() + "'s pool pre-warm stopped at "
								+ result.buffers() + " buffers: " + result.failure() + ". Set a larger -XX:MaxDirectMemorySize in SPT_JAVA_OPTS, or "
								+ PoolDemandConfig.SHORTFALL_SETTING + "=" + PoolDemandConfig.SHORTFALL_WARN + " to keep what was registered";
				if (driver.layout().failOnShortfall()) {
					throw new RunTerminalException(IntegrityTerminalException.Category.CONFIGURATION, message, null);
				}
				Loggers.MSG.warn("{}; the run goes on under {}={}", message, PoolDemandConfig.SHORTFALL_SETTING, PoolDemandConfig.SHORTFALL_WARN);
			}
		}
		return results;
	}

	private static String key(final Active driver, final String nic) {
		return driver.participant().contextIndex() + "/" + nic;
	}

	/**
	 * Each NIC's minimum in the class of its known demand, without a skip reason. A pool short of its
	 * budget under {@code warn} warms what fits: its budget would refuse the rest anyway.
	 */
	private static List<PoolDemandDriver.PrewarmOrder> orders(final Active driver) {
		final var orders = new ArrayList<PoolDemandDriver.PrewarmOrder>();
		if (!PoolDemand.prewarmSkip(driver.demand()).isEmpty()) {
			return orders;
		}
		final long budget = PoolDemand.nicBudgetBytes(driver.layout().limitBytes(), driver.layout().nics().size());
		for (final var nic : driver.demand().nics()) {
			final int fits = (int) Math.min(nic.count(), budget / nic.sizeClass());
			if (fits > 0) {
				orders.add(new PoolDemandDriver.PrewarmOrder(nic.nic(), fits, nic.sizeClass()));
			}
		}
		return orders;
	}

	private static Report report(
					final List<Active> active, final StepDemand step, final PoolDemand.Check check, final long required, final long estimate,
					final Map<String, PoolDemandDriver.PrewarmResult> results) {
		final var drivers = new ArrayList<DriverReport>(active.size());
		long longest = 0;
		for (final var driver : active) {
			final var skip = PoolDemand.prewarmSkip(driver.demand());
			final var nics = new ArrayList<NicReport>();
			final long budget = PoolDemand.nicBudgetBytes(driver.layout().limitBytes(), driver.layout().nics().size());
			for (final var nic : driver.demand().nics()) {
				final var result = results.get(key(driver, nic.nic()));
				final int buffers = result == null ? 0 : result.buffers();
				final long millis = result == null ? 0 : result.millis();
				longest = Math.max(longest, millis);
				final String note;
				if (result != null && result.failure() != null) {
					note = result.failure();
				} else if (skip.isEmpty() && buffers < nic.count()) {
					note = "its budget holds only " + buffers + " of " + nic.count();
				} else {
					note = skip;
				}
				nics.add(new NicReport(nic.nic(), nic.state(), nic.minimum(), nic.cap(), nic.headroom(), budget, nic.count(), nic.sizeClass(),
								nic.needBytes(), buffers, millis, note));
			}
			drivers.add(new DriverReport(driver.participant().contextIndex(), driver.layout().failOnShortfall()
							? PoolDemandConfig.SHORTFALL_FAIL
							: PoolDemandConfig.SHORTFALL_WARN, driver.demand(), nics));
		}
		return new Report(drivers, check, required, step.hasUnknown(), step.unknownReasons(), estimate, longest);
	}

	/** One line per NIC: its pool demand, minimum, cap, headroom and budget, and its pre-warm or why it was skipped (item 17). */
	private static void log(final String stepId, final Report report) {
		for (final var driver : report.drivers()) {
			for (final var nic : driver.nics()) {
				final var demand = nic.state() == State.KNOWN
								? nic.count() + " x " + PoolDemand.format(nic.sizeClass())
								: nic.state().name().toLowerCase(Locale.ROOT);
				final var warm = nic.prewarmNote().isEmpty()
								? "pre-warmed " + nic.prewarmed() + " buffers in " + nic.prewarmMillis() + " ms"
								: nic.state() == State.ZERO
												? "nothing to pre-warm: " + nic.prewarmNote()
												: "pre-warmed " + nic.prewarmed() + " buffers: " + nic.prewarmNote() + "; the load starts with cold pools";
				Loggers.MSG.info("{}: NIC {}: pool demand {}; minimum {}, cap {}, headroom {}, budget {}; {}", stepId, nic.nic(), demand,
								nic.minimum(), nic.cap(), nic.headroom(), PoolDemand.format(nic.budgetBytes()), warm);
			}
		}
	}
}
