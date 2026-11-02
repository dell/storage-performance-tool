package com.dell.spt.base.nic.pool;

import com.dell.spt.base.config.NicSetConfig;
import com.dell.spt.base.config.RangeReadConfig;
import com.dell.spt.base.item.ItemType;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.nic.pool.PoolDemand.OpKind;
import com.dell.spt.base.nic.pool.PoolDemand.Operation;
import com.dell.spt.base.nic.pool.PoolDemand.Sizes;
import com.dell.spt.base.util.BinarySizeFormat;
import com.github.akurilov.commons.reflection.TypeUtil;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

/** Reads what Pool Demand needs from a step's effective configuration. */
public final class PoolDemandConfig {

	/** The driver type whose NIC set has NIC Pools. */
	public static final String RDMA_DRIVER_TYPE = "s3-rdma";

	public static final String SHORTFALL_SETTING = "storage.rdma.poolShortfall";
	public static final String SHORTFALL_FAIL = "fail";
	public static final String SHORTFALL_WARN = "warn";

	private PoolDemandConfig() {}

	/** Whether this root configuration's driver has NIC Pools: {@code s3-rdma} with a NIC set, RDMA and the pool on. */
	public static boolean applies(final Config root) {
		final var storage = root.configVal("storage");
		return RDMA_DRIVER_TYPE.equals(root.stringVal("storage-driver-type"))
						&& NicSetConfig.isSet(storage)
						&& flag(root, "storage-rdma-enabled")
						&& flag(root, "storage-rdma-bufferPool");
	}

	/** Whether a slice of this configuration gets a size summary: only its CREATE and READ operations can use RDMA. */
	public static boolean recordsItemSizes(final Config root) {
		return applies(root)
						&& isData(root)
						&& (weight(root, "get") > 0
										|| (noWeights(root) && (isOpType(root, OpType.CREATE) || isOpType(root, OpType.READ))));
	}

	/**
	 * The operations of the driver this configuration describes.
	 *
	 * @param scan reads an item file this engine reads itself, for a configuration that holds no summary
	 */
	public static List<Operation> operations(final Config root, final Function<String, ItemSizeSummary> scan) {
		if (!isData(root)) {
			return List.of(new Operation(OpKind.OTHER, 0, false, new PoolDemand.NotKnown("")));
		}
		final long part = partBytes(root);
		final boolean ranged = ranged(root);
		final var operations = new ArrayList<Operation>(2);
		if (noWeights(root)) {
			if (isOpType(root, OpType.CREATE)) {
				operations.add(new Operation(OpKind.CREATE, part, false, itemSizes(root, scan)));
			} else if (isOpType(root, OpType.READ)) {
				operations.add(new Operation(OpKind.READ, part, ranged, itemSizes(root, scan)));
			} else {
				operations.add(new Operation(OpKind.OTHER, 0, false, new PoolDemand.NotKnown("")));
			}
			return operations;
		}
		// A mixed step: its GETs read the item input, and its PUTs write objects of item.data.size.
		if (weight(root, "get") > 0) {
			operations.add(new Operation(OpKind.READ, part, ranged, itemSizes(root, scan)));
		}
		if (weight(root, "put") > 0) {
			operations.add(new Operation(OpKind.CREATE, part, false, configuredSize(root)));
		}
		if (operations.isEmpty()) {
			operations.add(new Operation(OpKind.OTHER, 0, false, new PoolDemand.NotKnown("")));
		}
		return operations;
	}

	/** The item file the engine would read for its own, or {@code null} when the configuration holds a summary or has no file. */
	public static String fileToScan(final Config root) {
		return summaryOf(root) != null ? null : text(root, "item-input-file");
	}

	/** Whether a short pool fails the step, the default, rather than warns. */
	public static boolean failsOnShortfall(final String value) {
		return !SHORTFALL_WARN.equals(value);
	}

	private static Sizes itemSizes(final Config root, final Function<String, ItemSizeSummary> scan) {
		final String encoded = summaryOf(root);
		if (encoded != null) {
			try {
				return new PoolDemand.Listed(ItemSizeSummary.decode(encoded));
			} catch (final IllegalArgumentException e) {
				return new PoolDemand.NotKnown("its item sizes could not be read: " + e.getMessage());
			}
		}
		final String file = text(root, "item-input-file");
		if (file != null) {
			return new PoolDemand.Listed(scan.apply(file));
		}
		final String path = text(root, "item-input-path");
		if (path != null) {
			return new PoolDemand.NotKnown("its items come from the bucket listing " + path);
		}
		return configuredSize(root);
	}

	private static String summaryOf(final Config root) {
		return text(root, ItemSizeSummary.CONFIG_PATH);
	}

	private static Sizes configuredSize(final Config root) {
		final Object raw = root.val("item-data-size");
		final SizeInBytes size = raw instanceof String text
						? BinarySizeFormat.parseSize(text)
						: new SizeInBytes(TypeUtil.typeConvert(raw, long.class));
		return new PoolDemand.Range(size.getMin(), size.getMax(), "item.data.size");
	}

	private static long partBytes(final Config root) {
		final Object raw = root.val("item-data-ranges-threshold");
		final long part = raw instanceof String text ? BinarySizeFormat.parseFixedSize(text) : TypeUtil.typeConvert(raw, long.class);
		return Math.max(0, part);
	}

	private static boolean ranged(final Config root) {
		final var ranges = root.configVal("item-data-ranges");
		final List<?> fixed = ranges.listVal("fixed");
		return (fixed != null && !fixed.isEmpty())
						|| ranges.intVal("random") > 0
						|| RangeReadConfig.fromLoad(root.configVal("load")) != null;
	}

	private static boolean isData(final Config root) {
		return ItemType.DATA.equals(ItemType.valueOf(root.stringVal("item-type").toUpperCase(Locale.ROOT)));
	}

	private static boolean isOpType(final Config root, final OpType type) {
		return type.equals(OpType.valueOf(root.stringVal("load-op-type").toUpperCase(Locale.ROOT)));
	}

	private static boolean noWeights(final Config root) {
		return weight(root, "get") <= 0 && weight(root, "put") <= 0 && weight(root, "delete") <= 0 && weight(root, "stat") <= 0;
	}

	/** A mixed step's weight; the schema entry belongs to the mixed extension, so it may not exist. */
	private static int weight(final Config root, final String operation) {
		try {
			return root.intVal("load-op-weights-" + operation);
		} catch (final RuntimeException absent) {
			return 0;
		}
	}

	/** An RDMA setting, on by default; its schema entry belongs to the extension. */
	private static boolean flag(final Config root, final String path) {
		try {
			return root.boolVal(path);
		} catch (final RuntimeException absent) {
			return true;
		}
	}

	private static String text(final Config root, final String path) {
		try {
			final String value = root.stringVal(path);
			return value == null || value.isEmpty() ? null : value;
		} catch (final RuntimeException absent) {
			return null;
		}
	}
}
