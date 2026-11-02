package com.dell.spt.base.nic.pool;

import com.dell.spt.base.nic.pool.PoolDemand.Contribution;
import com.dell.spt.base.nic.pool.PoolDemand.DriverDemand;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Locale;

/** Writes a step's pool work as JSON: the {@code pool} object of the NIC-set report and of the image command's output. */
public final class PoolReportJson {

	private PoolReportJson() {}

	/** The engine's checks and pre-warm, for the run metadata (item 17). */
	public static void write(final ObjectNode node, final PoolPreparation.Report report) {
		node.put("max_direct_memory_bytes", report.check().limitBytes());
		node.put("required_limit_bytes", report.requiredLimitBytes());
		node.put("headroom_bytes", report.check().headroomBytes());
		node.put("partial", report.partial());
		strings(node.putArray("unknown"), report.unknown());
		if (report.estimateBytes() >= 0) {
			node.put("cli_estimate_bytes", report.estimateBytes());
		}
		node.put("prewarm_ms", report.prewarmMillis());
		check(node.putObject("checks"), report.check());
		final var drivers = node.putArray("drivers");
		for (final var driver : report.drivers()) {
			final var one = drivers.addObject();
			one.put("context_index", driver.contextIndex());
			one.put("shortfall", driver.shortfall());
			contributions(one.putArray("operations"), driver.demand());
			final var nics = one.putArray("nics");
			for (final var nic : driver.nics()) {
				final var row = nics.addObject();
				row.put("nic", nic.nic());
				row.put("demand", state(nic.state()));
				row.put("minimum", nic.minimum());
				row.put("cap", nic.cap());
				row.put("headroom", nic.headroom());
				row.put("budget_bytes", nic.budgetBytes());
				row.put("count", nic.count());
				row.put("size_class_bytes", nic.sizeClass());
				row.put("need_bytes", nic.needBytes());
				row.put("prewarmed_buffers", nic.prewarmed());
				row.put("prewarm_ms", nic.prewarmMillis());
				if (!nic.prewarmNote().isEmpty()) {
					row.put("prewarm_note", nic.prewarmNote());
				}
			}
		}
	}

	/** The two results, apart. */
	public static void check(final ObjectNode node, final PoolDemand.Check check) {
		final var pools = node.putObject("per_pool");
		pools.put("passed", check.poolsShort().isEmpty());
		final var shorts = pools.putArray("short");
		for (final var pool : check.poolsShort()) {
			final var row = shorts.addObject();
			row.put("driver", pool.driver());
			row.put("nic", pool.nic());
			row.put("need_bytes", pool.needBytes());
			row.put("budget_bytes", pool.budgetBytes());
		}
		final var process = node.putObject("process");
		process.put("passed", !check.processShort());
		process.put("need_bytes", check.processNeedBytes());
		process.put("limit_bytes", check.limitBytes());
	}

	static void contributions(final ArrayNode array, final DriverDemand demand) {
		for (final Contribution contribution : demand.contributions()) {
			final var row = array.addObject();
			row.put("op", contribution.kind().label());
			row.put("demand", state(contribution.state()));
			row.put("source", contribution.source());
			if (contribution.state() == PoolDemand.State.KNOWN) {
				row.put("smallest_bytes", contribution.smallest());
				row.put("largest_bytes", contribution.largest());
				row.put("size_class_bytes", contribution.largestClass());
			}
			if (!contribution.reason().isEmpty()) {
				row.put("reason", contribution.reason());
			}
		}
	}

	static void strings(final ArrayNode array, final Iterable<String> values) {
		values.forEach(array::add);
	}

	static String state(final PoolDemand.State state) {
		return state.name().toLowerCase(Locale.ROOT);
	}
}
