package com.dell.spt.base.nic.pool;

import com.dell.spt.base.nic.pool.PoolDemand.DriverDemand;
import com.dell.spt.base.nic.pool.PoolDemand.OpKind;
import com.dell.spt.base.nic.pool.PoolDemand.Operation;
import com.dell.spt.base.nic.pool.PoolDemand.StepDemand;
import com.dell.spt.base.nic.pool.PoolDemand.Workload;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;

/**
 * The estimate as the image command serves it, so the CLI's sizing and {@code spt verify} share one
 * implementation with the engine's check. The request is base64 JSON, which passes unchanged through
 * a remote shell: the steps the engine would run, each with its concurrent drivers and their
 * operations. The CLI reads item files itself, on its own host, and sends what it found.
 *
 * <p>The response gives the limit each check needs apart, so the CLI compares without doing the
 * arithmetic again, and it composes the steps: the largest limit across them sets the value.
 */
public final class PoolDemandRequest {

	/** One step: the drivers that run at once. */
	public record Step(String id, List<Workload> drivers) {

		public Step {
			drivers = List.copyOf(drivers);
		}
	}

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private PoolDemandRequest() {}

	/**
	 * Reads a request.
	 *
	 * @throws IllegalArgumentException when it is not one, which the command reports as a usage error
	 */
	public static List<Step> parse(final String base64) {
		final JsonNode root;
		try {
			root = MAPPER.readTree(new String(Base64.getDecoder().decode(base64), StandardCharsets.UTF_8));
		} catch (final JsonProcessingException | IllegalArgumentException e) {
			throw new IllegalArgumentException("--pool-request is not base64 JSON: " + e.getMessage(), e);
		}
		final var steps = new ArrayList<Step>();
		for (final JsonNode step : root.path("steps")) {
			final var drivers = new ArrayList<Workload>();
			for (final JsonNode driver : step.path("drivers")) {
				drivers.add(workload(driver));
			}
			steps.add(new Step(step.path("id").asText(""), drivers));
		}
		return steps;
	}

	private static Workload workload(final JsonNode driver) {
		final var operations = new ArrayList<Operation>();
		for (final JsonNode operation : driver.path("operations")) {
			operations.add(operation(operation));
		}
		return new Workload(driver.path("concurrency").asInt(0), driver.path("thresholdBytes").asLong(0), driver.path("multipartObjects").asInt(0),
						driver.path("multipartParts").asInt(0), operations);
	}

	private static Operation operation(final JsonNode operation) {
		final OpKind kind;
		try {
			kind = OpKind.valueOf(operation.path("kind").asText("").toUpperCase(Locale.ROOT));
		} catch (final IllegalArgumentException e) {
			throw new IllegalArgumentException("--pool-request has an unknown operation kind: " + operation.path("kind"), e);
		}
		final JsonNode sizes = operation.path("sizes");
		final PoolDemand.Sizes parsed;
		if (sizes.has("range")) {
			parsed = new PoolDemand.Range(sizes.get("range").get(0).asLong(), sizes.get("range").get(1).asLong(), sizes.path("source").asText(""));
		} else if (sizes.has("listed")) {
			parsed = new PoolDemand.Listed(ItemSizeSummary.fromJson(sizes.get("listed")));
		} else {
			parsed = new PoolDemand.NotKnown(sizes.path("unknown").asText("the workload does not say"));
		}
		return new Operation(kind, operation.path("partBytes").asLong(0), operation.path("ranged").asBoolean(false), parsed);
	}

	/** Writes each step's demand on {@code nics}, this host's planned NICs in layout order. */
	public static void write(final ObjectNode node, final List<Step> steps, final List<String> nics) {
		node.put("headroomBytes", PoolDemand.PROCESS_HEADROOM_BYTES);
		final var array = node.putArray("steps");
		for (final var step : steps) {
			final var drivers = step.drivers().stream().map(workload -> PoolDemand.estimate(workload, nics)).toList();
			write(array.addObject(), step.id(), drivers);
		}
	}

	private static void write(final ObjectNode node, final String id, final List<DriverDemand> drivers) {
		final var demand = new StepDemand(drivers);
		node.put("id", id);
		node.put("limitBytes", demand.requiredLimitBytes(PoolDemand.PROCESS_HEADROOM_BYTES));
		node.put("limitForPoolsBytes", drivers.stream().mapToLong(DriverDemand::limitForBudgetBytes).max().orElse(0));
		final long pools = demand.poolsNeedBytes();
		node.put("limitForProcessBytes", pools == 0 ? 0 : pools + PoolDemand.PROCESS_HEADROOM_BYTES);
		node.put("poolsNeedBytes", pools);
		node.put("known", demand.hasKnown());
		node.put("partial", demand.hasKnown() && demand.hasUnknown());
		PoolReportJson.strings(node.putArray("unknown"), demand.unknownReasons());
		long resident = 0;
		final var array = node.putArray("drivers");
		for (final var driver : drivers) {
			final var row = array.addObject();
			row.put("demand", PoolReportJson.state(driver.state()));
			PoolReportJson.contributions(row.putArray("operations"), driver);
			final var skip = PoolDemand.prewarmSkip(driver);
			row.put("prewarm", skip.isEmpty());
			if (!skip.isEmpty()) {
				row.put("prewarmSkip", skip);
			}
			final var nics = row.putArray("nics");
			for (final var nic : driver.nics()) {
				final var nicRow = nics.addObject();
				nicRow.put("nic", nic.nic());
				nicRow.put("demand", PoolReportJson.state(nic.state()));
				nicRow.put("minimum", nic.minimum());
				nicRow.put("cap", nic.cap());
				nicRow.put("count", nic.count());
				nicRow.put("sizeClassBytes", nic.sizeClass());
				nicRow.put("needBytes", nic.needBytes());
				if (skip.isEmpty()) {
					resident += nic.needBytes();
				}
			}
		}
		node.put("prewarmResidentBytes", resident);
	}
}
