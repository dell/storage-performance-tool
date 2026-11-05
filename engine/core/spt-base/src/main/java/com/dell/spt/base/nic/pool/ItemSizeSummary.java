package com.dell.spt.base.nic.pool;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * The sizes in one item list, or one slice of it (D54): only the smallest and largest size in each
 * power-of-two range, never the items. Which of them an operation would send over RDMA depends on
 * the operation's transfer shape, which {@link PoolDemand} applies, so one summary serves every
 * shape and a slice's summary stays small. Within one range the shape can move only the bounds, not
 * the size class they fall in.
 *
 * <p>Completion (D54-R1): it is complete only after a verified normal end of input and, for a slice,
 * a successful write. An incomplete summary's bounds are never used, and it never fails a step by
 * itself: its contribution is unknown.
 *
 * @param source what was read, for messages
 * @param complete whether every item was seen
 * @param reason why not, or {@code null} when complete
 * @param detail the failure's own words, or empty
 * @param items the items seen
 * @param bounds the bounds, by the exponent of the power of two that holds them
 * @param millis how long the read took
 */
public record ItemSizeSummary(String source,boolean complete,Reason reason,String detail,long items,NavigableMap<Integer,Bounds>bounds,long millis){

/**
 * Where a slice's summary travels: an internal schema entry with no shipped default, so the stored
 * effective configuration has it only for a slice that was given one.
 */
public static final String CONFIG_PATH="item-input-sizeSummary";

/** Why a summary is incomplete. */
public enum Reason {
	MALFORMED_ITEM("a malformed item"), READ_FAILED("a read failure"), BUDGET_EXPIRED("its read budget expired"), NOT_A_FILE("the items do not come from a file");

	private final String text;

	Reason(final String text) {
		this.text = text;
	}

	public String text() {
		return text;
	}

	}

	/** The smallest and largest size seen in one power-of-two range. */
	public record Bounds(long smallest, long largest) {

	private Bounds with(final long size) {
		return new Bounds(Math.min(smallest, size), Math.max(largest, size));
	}}

	private static final ObjectMapper MAPPER = new ObjectMapper();

	public ItemSizeSummary{bounds=Collections.unmodifiableNavigableMap(new TreeMap<>(bounds));detail=detail==null?"":detail;
	}

	/** The range's exponent: the smallest {@code e} with {@code size <= 2^e}. */
	static int exponentOf(final long size) {
		return size <= 1 ? 0 : Long.SIZE - Long.numberOfLeadingZeros(size - 1);
	}

	/** Names what was read and why it is incomplete, for a log line and a partial check's message. */
	public String why() {
		if (complete) {
			return "";
		}
		final var why = source + ": " + reason.text();
		return detail.isEmpty() ? why : why + " (" + detail + ")";
	}

	/** The largest size seen, or -1 without any. */
	public long largest() {
		return bounds.isEmpty() ? -1 : bounds.lastEntry().getValue().largest();
	}

	/** The smallest size seen, or -1 without any. */
	public long smallest() {
		return bounds.isEmpty() ? -1 : bounds.firstEntry().getValue().smallest();
	}

	/** The JSON form a slice's configuration and the image command's request carry. */
	public String encode() {
		final var root = MAPPER.createObjectNode();
		root.put("source", source);
		root.put("complete", complete);
		if (reason != null) {
			root.put("reason", reason.name());
		}
		if (!detail.isEmpty()) {
			root.put("detail", detail);
		}
		root.put("items", items);
		root.put("millis", millis);
		final var array = root.putArray("bounds");
		bounds.values().forEach(value -> array.addArray().add(value.smallest()).add(value.largest()));
		try {
			return MAPPER.writeValueAsString(root);
		} catch (final JsonProcessingException e) {
			throw new IllegalStateException("Failed to serialize an item size summary", e);
		}
	}

	/**
	 * Reads {@link #encode()}'s form.
	 *
	 * @throws IllegalArgumentException when it is not that form
	 */
	public static ItemSizeSummary decode(final String json) {
		try {
			return fromJson(MAPPER.readTree(json));
		} catch (final JsonProcessingException | RuntimeException e) {
			throw new IllegalArgumentException("Not an item size summary: " + e.getMessage(), e);
		}
	}

	static ItemSizeSummary fromJson(final JsonNode root) {
		final NavigableMap<Integer, Bounds> bounds = new TreeMap<>();
		for (final JsonNode pair : root.path("bounds")) {
			final long smallest = pair.get(0).asLong();
			final long largest = pair.get(1).asLong();
			if (smallest > largest || exponentOf(smallest) != exponentOf(largest)) {
				throw new IllegalArgumentException("bad size bounds " + pair);
			}
			bounds.put(exponentOf(largest), new Bounds(smallest, largest));
		}
		final boolean complete = root.path("complete").asBoolean(false);
		final Reason reason = root.hasNonNull("reason") ? Reason.valueOf(root.get("reason").asText()) : null;
		if (!complete && reason == null) {
			throw new IllegalArgumentException("an incomplete summary needs its reason");
		}
		return new ItemSizeSummary(root.path("source").asText(""), complete, complete ? null : reason, root.path("detail").asText(""),
						root.path("items").asLong(0), bounds, root.path("millis").asLong(0));
	}

	/** Collects one list's or slice's sizes. Confined to one thread at a time. */
	public static final class Recorder {

		private final String source;
		private final long startNanos;
		private final NavigableMap<Integer, Bounds> bounds = new TreeMap<>();
		private long items;
		private boolean finished;
		private Reason reason;
		private String detail = "";
		private long endNanos;

		public Recorder(final String source) {
			this(source, System.nanoTime());
		}

		Recorder(final String source, final long startNanos) {
			this.source = source;
			this.startNanos = startNanos;
		}

		/** Records one item's size; nothing is recorded after a failure, as its bounds are never used. */
		public void record(final long size) {
			if (reason != null) {
				return;
			}
			items++;
			final int exponent = exponentOf(size);
			final Bounds known = bounds.get(exponent);
			bounds.put(exponent, known == null ? new Bounds(size, size) : known.with(size));
		}

		/** The input ended normally and everything was written. The first failure stays. */
		public void finish() {
			if (!finished) {
				finished = true;
				endNanos = System.nanoTime();
			}
		}

		/** Marks it incomplete. The first failure stays, and a finished summary is not changed by a later one. */
		public void fail(final Reason why, final String what) {
			if (reason == null && !finished) {
				reason = why;
				detail = what == null ? "" : what;
				endNanos = System.nanoTime();
			}
		}

		public boolean failed() {
			return reason != null;
		}

		public ItemSizeSummary summary() {
			final boolean complete = finished && reason == null;
			final Reason why = complete ? null : reason != null ? reason : Reason.READ_FAILED;
			final long end = endNanos != 0 ? endNanos : System.nanoTime();
			return new ItemSizeSummary(source, complete, why, detail, items, complete ? bounds : new TreeMap<>(), (end - startNanos) / 1_000_000);
		}
	}
}
