package com.dell.spt.base.config;

import com.dell.spt.base.item.op.data.range.RangeReadPolicy;
import com.dell.spt.base.util.BinarySizeFormat;
import com.github.akurilov.confuse.Config;
import java.util.Objects;

/** Construction-time access and validation for the opt-in single-range READ mode. */
public final class RangeReadConfig {
	public static final String DRIVER_TYPE = "s3";

	private RangeReadConfig() {}

	/** Preserve legacy extension configurations which predate the nullable read subtree. */
	public static RangeReadPolicy fromLoad(final Config loadConfig) {
		final Config op = loadConfig.configVal("op");
		if (!op.schema().containsKey("read") || op.mapVal(null).get("read") == null) {
			return null;
		}
		try {
			return RangeReadPolicy.parse(byteString(op, "size"),
							byteString(op, "offset"), byteString(op, "align"));
		} catch (final IllegalArgumentException e) {
			throw new IllegalConfigurationException("Invalid load.op.read.range: " + e.getMessage(), e);
		}
	}

	private static String byteString(final Config op, final String leaf) {
		final Object raw = op.val("read-range-" + leaf);
		if (raw == null || raw instanceof String) {
			return (String) raw;
		}
		// Direct setters may retain numeric scalars under a string schema.
		// Keep checked integer parsing local to range mode.
		if (raw instanceof Number) {
			return raw.toString();
		}
		throw new IllegalArgumentException(leaf + " must be an integer byte size or binary-size string");
	}

	/** Validate before constructing driver/network resources; disabled mode retains existing behavior. */
	public static RangeReadPolicy validate(final Config config) {
		final RangeReadPolicy policy = fromLoad(config.configVal("load"));
		if (policy == null) {
			return null;
		}
		if (!"read".equalsIgnoreCase(config.stringVal("load-op-type"))
						|| !"data".equalsIgnoreCase(config.stringVal("item-type"))) {
			throw invalid("requires DATA READ");
		}
		if (!DRIVER_TYPE.equals(config.stringVal("storage-driver-type"))) {
			throw invalid("requires the Netty s3 driver; AWS, native RDMA and other drivers are unsupported");
		}
		if (config.boolVal("item-data-verify")
						|| !"none".equals(config.stringVal("storage-integrity-mode"))) {
			throw invalid("does not support content or metadata-integrity verification");
		}
		if (config.boolVal("load-op-recycle-content-update")) {
			throw invalid("does not support recycled content updates");
		}
		final Config ranges = config.configVal("item-data-ranges");
		final var fixed = ranges.listVal("fixed");
		final Object threshold = ranges.val("threshold");
		final long thresholdBytes = threshold instanceof String text
						? BinarySizeFormat.parseFixedSize(text)
						: com.github.akurilov.commons.reflection.TypeUtil.typeConvert(threshold, long.class);
		if ((fixed != null && !fixed.isEmpty()) || ranges.intVal("random") > 0 || thresholdBytes > 0) {
			throw invalid("conflicts with active item.data.ranges fixed, random or threshold settings");
		}
		return policy;
	}

	public static void requireMatchingRuntime(final RangeReadPolicy configured, final RangeReadPolicy installed) {
		if (configured != null && !Objects.equals(configured, installed)) {
			throw invalid("requires a driver with the matching single-range runtime");
		}
	}

	private static IllegalConfigurationException invalid(final String detail) {
		return new IllegalConfigurationException("load.op.read.range " + detail);
	}
}
