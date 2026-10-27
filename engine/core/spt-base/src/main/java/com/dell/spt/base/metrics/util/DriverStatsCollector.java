package com.dell.spt.base.metrics.util;

import static com.dell.spt.base.metrics.MetricsConstants.*;

import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.DriverStatsSnapshot;
import com.dell.spt.base.metrics.snapshot.DriverStatsTotal;
import io.prometheus.client.Collector;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Exports the storage driver statistics of the Entry Node's distributed steps while they run.
 *
 * <p>Each step's fleet total gives one sample per name: a counter as a Prometheus counter, a gauge
 * as a gauge, with the labels of the step's other series and the driver type. The sum is exported
 * only when it is complete, so a counter never falls because a node was missing from one sum;
 * {@code driver_reporting} and {@code driver_partial} are exported for every total. The series
 * leave with the step. Its final values are on the fleet metrics endpoint and in the run results.
 *
 * <p>A counter is never exported below the highest value exported before for its series, which
 * Prometheus would read as a reset. The built-in contexts publish their refreshes in order, so a
 * complete total does not fall; the guard keeps that true for a context that does not order them.
 *
 * <p>A driver names its statistics freely. A name Prometheus does not accept is exported in a
 * valid form, and a statistic whose name would take the series of another is left out. Each is
 * logged once.
 */
public final class DriverStatsCollector extends Collector implements Collector.Describable {

	private static final String COUNTER_SUFFIX = "_total";
	private static final String CREATED_SUFFIX = "_created";
	private static final String HELP_COUNTER = "Storage driver counter: the fleet total of a running step";
	private static final String HELP_GAUGE = "Storage driver gauge: the sum of the levels each node read at its own moment, for a running step";
	private static final String HELP_REPORTING = "Nodes whose storage driver statistics are in the fleet total";
	private static final String HELP_PARTIAL = "1 when the fleet total of storage driver statistics is incomplete";
	private static final List<String> LABEL_NAMES = labelNames();

	private final Supplier<? extends Collection<? extends DistributedMetricsContext>> contexts;
	private final Set<String> logged = ConcurrentHashMap.newKeySet();
	/** The highest value exported for each counter series of the steps that run. */
	private final Map<Series, Double> highest = new ConcurrentHashMap<>();

	/** @param contexts the distributed metrics contexts of the steps that run */
	public DriverStatsCollector(final Supplier<? extends Collection<? extends DistributedMetricsContext>> contexts) {
		this.contexts = contexts;
	}

	/** Declares no names: they depend on the drivers that run. */
	@Override
	public List<MetricFamilySamples> describe() {
		return List.of();
	}

	@Override
	public List<MetricFamilySamples> collect() {
		final List<Step> steps = new ArrayList<>();
		final Set<List<String>> running = new HashSet<>();
		for (final DistributedMetricsContext<?> context : contexts.get()) {
			final List<String> labelValues = List.of(PrometheusMetricsExporterImpl.labelValues(context));
			running.add(labelValues);
			context.refreshLastSnapshot();
			final DistributedAllMetricsSnapshot snapshot = context.lastSnapshot();
			final DriverStatsTotal total = snapshot == null ? null : snapshot.driverStatsTotal();
			if (total != null) {
				steps.add(new Step(labelValues, total));
			}
		}
		highest.keySet().removeIf(series -> !running.contains(series.step()));
		// The order decides which of two statistics that take the same series is left out.
		steps.sort(Comparator.comparing(step -> String.join("\u0000", step.labelValues())));
		final Families families = new Families();
		for (final Step step : steps) {
			collect(step, families);
		}
		return families.samples();
	}

	private void collect(final Step step, final Families families) {
		final DriverStatsTotal total = step.total();
		final DriverStatsSnapshot sum = total.sum();
		final String driver = sum == null ? String.join(",", total.drivers()) : sum.driver();
		final List<String> labelValues = new ArrayList<>(step.labelValues());
		labelValues.add(driver);
		families.add(name(METRIC_NAME_DRIVER_REPORTING), Type.GAUGE, HELP_REPORTING, labelValues, total.reporting());
		families.add(name(METRIC_NAME_DRIVER_PARTIAL), Type.GAUGE, HELP_PARTIAL, labelValues, total.partial() ? 1 : 0);
		if (sum == null || total.partial()) {
			return;
		}
		sum.counters().forEach((statistic, value) -> {
			final String family = statisticName(driver, statistic, Type.COUNTER);
			final Series series = new Series(step.labelValues(), driver, family);
			final Double before = highest.get(series);
			final double exported = before == null ? value : Math.max(value, before);
			if (families.add(family, Type.COUNTER, HELP_COUNTER, labelValues, exported)) {
				highest.merge(series, exported, Math::max);
			} else {
				leftOut(driver, statistic, family);
			}
		});
		sum.gauges().forEach((statistic, value) -> {
			final String family = statisticName(driver, statistic, Type.GAUGE);
			if (!families.add(family, Type.GAUGE, HELP_GAUGE, labelValues, value)) {
				leftOut(driver, statistic, family);
			}
		});
	}

	/** Returns the family name of a statistic, made valid for Prometheus. */
	private String statisticName(final String driver, final String statistic, final Type type) {
		final String named = name(METRIC_NAME_DRIVER_PREFIX + statistic);
		final String family = sanitizeMetricName(named);
		if (!family.equals(named) && logged.add("renamed " + driver + " " + statistic)) {
			Loggers.ERR.warn("Storage driver {} statistic \"{}\" is exported to Prometheus as \"{}\"", driver, statistic, family);
		}
		// A counter's samples take the suffix; the family name does not end with it.
		return type == Type.COUNTER && family.endsWith(COUNTER_SUFFIX)
						? family.substring(0, family.length() - COUNTER_SUFFIX.length())
						: family;
	}

	private void leftOut(final String driver, final String statistic, final String family) {
		if (logged.add("left out " + driver + " " + statistic)) {
			Loggers.ERR.warn(
							"Storage driver {} statistic \"{}\" is not exported to Prometheus: another series takes the name \"{}\"",
							driver,
							statistic,
							family);
		}
	}

	private static String name(final String metricName) {
		return String.format(METRIC_FORMAT, metricName);
	}

	private static List<String> labelNames() {
		final List<String> names = new ArrayList<>(METRIC_LABELS);
		names.add(METRIC_LABEL_DRIVER);
		return List.copyOf(names);
	}

	/** The fleet total of one step's metrics context, with the values of its labels. */
	private record Step(List<String> labelValues, DriverStatsTotal total) {}

	/** A counter series: the labels of its step, its driver and its family. */
	private record Series(List<String> step, String driver, String family) {}

	/** The families of one collection and the sample names they take. */
	private static final class Families {

		private final Map<String, Family> byName = new TreeMap<>();
		private final Map<String, String> owners = new HashMap<>();

		/**
		 * Adds a sample to a family, and the family when it is new.
		 *
		 * @return false when the sample would take a series of another family, or one the family
		 *     already has
		 */
		boolean add(
						final String name, final Type type, final String help, final List<String> labelValues, final double value) {
			Family family = byName.get(name);
			if (family == null) {
				final List<String> sampleNames = sampleNames(name, type);
				if (sampleNames.stream().anyMatch(owners::containsKey)) {
					return false;
				}
				family = new Family(type, help);
				byName.put(name, family);
				sampleNames.forEach(sampleName -> owners.put(sampleName, name));
			} else if (family.type != type) {
				return false;
			}
			if (!family.labelValues.add(labelValues)) {
				return false;
			}
			final String sampleName = type == Type.COUNTER ? name + COUNTER_SUFFIX : name;
			family.samples.add(new MetricFamilySamples.Sample(sampleName, LABEL_NAMES, labelValues, value));
			return true;
		}

		List<MetricFamilySamples> samples() {
			final List<MetricFamilySamples> samples = new ArrayList<>(byName.size());
			byName.forEach((name, family) -> samples.add(new MetricFamilySamples(name, family.type, family.help, family.samples)));
			return samples;
		}

		/** The names a family takes in the text formats Prometheus reads. */
		private static List<String> sampleNames(final String name, final Type type) {
			return type == Type.COUNTER ? List.of(name, name + COUNTER_SUFFIX, name + CREATED_SUFFIX) : List.of(name);
		}
	}

	private static final class Family {

		private final Type type;
		private final String help;
		private final List<MetricFamilySamples.Sample> samples = new ArrayList<>();
		private final Set<List<String>> labelValues = new HashSet<>();

		private Family(final Type type, final String help) {
			this.type = type;
			this.help = help;
		}
	}
}
