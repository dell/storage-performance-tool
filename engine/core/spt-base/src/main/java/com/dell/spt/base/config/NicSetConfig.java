package com.dell.spt.base.config;

import com.dell.spt.base.item.op.OpType;
import com.github.akurilov.commons.net.NetUtil;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.exceptions.InvalidValuePathException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Construction-time access to {@code storage.net.interfaces}, the client NICs a step's connections
 * are bound to. The value is stored as requested; each engine resolves it on its own host at start.
 */
public final class NicSetConfig {

	/** Path of the setting relative to the storage configuration. */
	public static final String INTERFACES_PATH = "net-interfaces";

	/** The entry that asks each engine to choose its own NICs. It is only valid alone. */
	public static final String AUTO = "auto";

	/** Path of the host overrides relative to the storage configuration. */
	public static final String NODE_INTERFACES_PATH = "net-nodeInterfaces";

	/** The key of the Entry Node's host override, whose engine has no address in the node list. */
	public static final String ENTRY_KEY = "entry";

	/** The host override value that turns a node's NICs off. It is only valid alone. */
	public static final String NONE = "none";

	/** The two settings' paths relative to a root configuration, for the Entry Node's slicer. */
	public static final String SLICE_INTERFACES_PATH = "storage-" + INTERFACES_PATH;

	public static final String SLICE_NODE_INTERFACES_PATH = "storage-" + NODE_INTERFACES_PATH;

	static final String SETTING = "storage.net.interfaces";

	static final String NODE_SETTING = "storage.net.nodeInterfaces";

	private static final String RDMA_DEVICE_PATH = "rdma-device";
	private static final String RDMA_LOCAL_IP_PATH = "rdma-localIp";
	private static final String RDMA_DEVICE_AUTO = "auto";

	private NicSetConfig() {}

	/**
	 * Returns the requested entries, trimmed and checked. A missing path, an empty list or a single
	 * blank entry (the engine CLI's {@code --storage-net-interfaces=}) means no NIC set, so the step
	 * keeps the kernel's route.
	 *
	 * @throws IllegalConfigurationException for a blank entry, a repeated entry, an IPv6 literal,
	 *     {@code none}, or {@code auto} beside other entries or not in lowercase
	 */
	public static List<String> interfaces(final Config storageConfig) {
		final List<?> values;
		try {
			values = storageConfig.listVal(INTERFACES_PATH);
		} catch (final InvalidValuePathException | NoSuchElementException e) {
			return List.of();
		}
		return validate(values);
	}

	/** Returns whether the step requests a NIC set. */
	public static boolean isSet(final Config storageConfig) {
		return !interfaces(storageConfig).isEmpty();
	}

	/**
	 * Returns the host overrides by {@link #ENTRY_KEY} or node address, each with its checked
	 * entries; {@link #NONE} gives no entries, which is no NIC set on that node. Node addresses have
	 * their port, as the Entry Node's slicer sees them. A missing path or an empty map means none.
	 *
	 * @param config the step's root configuration
	 * @throws IllegalConfigurationException for a key that names no configured node, or a value that
	 *     is blank, has {@link #NONE} beside other entries, or breaks the rules of {@link #interfaces}
	 */
	public static Map<String, List<String>> nodeInterfaces(final Config config) {
		final Map<String, Object> overrides;
		try {
			overrides = config.configVal("storage").mapVal(NODE_INTERFACES_PATH);
		} catch (final InvalidValuePathException | NoSuchElementException e) {
			return Map.of();
		}
		if (overrides == null || overrides.isEmpty()) {
			return Map.of();
		}
		final var nodeConfig = config.configVal("load-step-node");
		final var port = nodeConfig.intVal("port");
		final var nodeAddrs = nodeConfig.<String> listVal("addrs").stream()
						.map(addr -> NetUtil.addPortIfMissing(addr, port))
						.toList();
		final var checked = new LinkedHashMap<String, List<String>>(overrides.size());
		overrides.forEach((key, value) -> {
			final var node = ENTRY_KEY.equals(key) ? key : NetUtil.addPortIfMissing(key, port);
			if (!ENTRY_KEY.equals(node) && !nodeAddrs.contains(node)) {
				throw new IllegalConfigurationException(
								NODE_SETTING + " names " + key + ", which is neither " + ENTRY_KEY + " nor in load.step.node.addrs");
			}
			checked.put(node, nodeEntries(key, value));
		});
		return checked;
	}

	private static List<String> nodeEntries(final String key, final Object value) {
		final var setting = NODE_SETTING + "[" + key + "]";
		final List<?> values = value instanceof List<?> list ? list : Arrays.asList(String.valueOf(value).split(",", -1));
		if (values.isEmpty() || (values.size() == 1 && (values.get(0) == null || String.valueOf(values.get(0)).isBlank()))) {
			throw new IllegalConfigurationException(setting + " is empty; list NICs, or " + AUTO + " or " + NONE + " alone");
		}
		final var entries = validate(values, setting);
		if (entries.contains(NONE)) {
			if (entries.size() > 1) {
				throw new IllegalConfigurationException(setting + ": " + NONE + " must be the only entry");
			}
			return List.of();
		}
		return entries;
	}

	static List<String> validate(final List<?> values) {
		final var entries = validate(values, SETTING);
		if (entries.contains(NONE)) {
			throw new IllegalConfigurationException(
							SETTING + " entry \"" + NONE + "\": " + NONE + " applies only to " + NODE_SETTING);
		}
		return entries;
	}

	private static List<String> validate(final List<?> values, final String setting) {
		if (values == null || values.isEmpty()) {
			return List.of();
		}
		if (values.size() == 1 && (values.get(0) == null || String.valueOf(values.get(0)).isBlank())) {
			return List.of();
		}
		final var entries = new ArrayList<String>(values.size());
		final var seen = new HashSet<String>();
		for (final Object raw : values) {
			final var entry = raw == null ? "" : String.valueOf(raw).strip();
			if (entry.isEmpty()) {
				throw new IllegalConfigurationException(
								setting + " has a blank entry; list interface names, IPv4 addresses or RDMA devices, or "
												+ AUTO + " alone");
			}
			if (!AUTO.equals(entry) && AUTO.equals(entry.toLowerCase(Locale.ROOT))) {
				throw new IllegalConfigurationException(setting + " entry \"" + entry + "\": write " + AUTO + " in lowercase");
			}
			if (entry.indexOf(':') >= 0) {
				throw new IllegalConfigurationException(
								setting + " entry \"" + entry + "\" looks like an IPv6 address; NIC sets support IPv4 only");
			}
			if (!seen.add(entry)) {
				throw new IllegalConfigurationException(setting + " lists \"" + entry + "\" twice");
			}
			entries.add(entry);
		}
		if (entries.size() > 1 && seen.contains(AUTO)) {
			throw new IllegalConfigurationException(setting + ": " + AUTO + " must be the only entry");
		}
		return List.copyOf(entries);
	}

	/**
	 * Rejects an explicit RDMA device or local address beside a NIC set: the NIC set chooses both
	 * for each connection.
	 */
	public static void requireNoRdmaAddress(final Config storageConfig) {
		final var device = optionalString(storageConfig, RDMA_DEVICE_PATH);
		if (device != null && !device.isBlank() && !RDMA_DEVICE_AUTO.equals(device.strip())) {
			throw new IllegalConfigurationException(
							"storage.rdma.device (--rdma-device) cannot be combined with " + SETTING
											+ "; list the RDMA device in " + SETTING + " instead");
		}
		final var localIp = optionalString(storageConfig, RDMA_LOCAL_IP_PATH);
		if (localIp != null && !localIp.isBlank()) {
			throw new IllegalConfigurationException(
							"storage.rdma.localIp (--rdma-local-ip) cannot be combined with " + SETTING
											+ "; list the local address in " + SETTING + " instead");
		}
	}

	/**
	 * Rejects a workload a NIC set does not carry: NOOP, and copy (CREATE from a source path).
	 * Without a NIC set every workload passes.
	 *
	 * @param itemInputPath the step's {@code item.input.path}, the copy source for CREATE
	 */
	public static void requireSupportedWorkload(
					final Config storageConfig, final OpType opType, final String itemInputPath) {
		if (!isSet(storageConfig)) {
			return;
		}
		if (opType == OpType.NOOP) {
			throw new IllegalConfigurationException(SETTING + " does not support NOOP operations");
		}
		if (opType == OpType.CREATE && itemInputPath != null && !itemInputPath.isBlank()) {
			throw new IllegalConfigurationException(
							SETTING + " does not support copy (CREATE with item.input.path); remove one of them");
		}
	}

	private static String optionalString(final Config config, final String path) {
		try {
			return config.stringVal(path);
		} catch (final InvalidValuePathException | NoSuchElementException e) {
			return null;
		}
	}
}
