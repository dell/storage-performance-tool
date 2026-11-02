package com.dell.spt.base.config;

import com.dell.spt.base.item.op.OpType;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.exceptions.InvalidValuePathException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
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

	static final String SETTING = "storage.net.interfaces";

	private static final String RDMA_DEVICE_PATH = "rdma-device";
	private static final String RDMA_LOCAL_IP_PATH = "rdma-localIp";
	private static final String RDMA_DEVICE_AUTO = "auto";

	private NicSetConfig() {}

	/**
	 * Returns the requested entries, trimmed and checked. A missing path, an empty list or a single
	 * blank entry (the engine CLI's {@code --storage-net-interfaces=}) means no NIC set, so the step
	 * keeps the kernel's route.
	 *
	 * @throws IllegalConfigurationException for a blank entry, a repeated entry, an IPv6 literal, or
	 *     {@code auto} beside other entries or not in lowercase
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

	static List<String> validate(final List<?> values) {
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
								SETTING + " has a blank entry; list interface names, IPv4 addresses or RDMA devices, or "
												+ AUTO + " alone");
			}
			if (!AUTO.equals(entry) && AUTO.equals(entry.toLowerCase(Locale.ROOT))) {
				throw new IllegalConfigurationException(SETTING + " entry \"" + entry + "\": write " + AUTO + " in lowercase");
			}
			if (entry.indexOf(':') >= 0) {
				throw new IllegalConfigurationException(
								SETTING + " entry \"" + entry + "\" looks like an IPv6 address; NIC sets support IPv4 only");
			}
			if (!seen.add(entry)) {
				throw new IllegalConfigurationException(SETTING + " lists \"" + entry + "\" twice");
			}
			entries.add(entry);
		}
		if (entries.size() > 1 && seen.contains(AUTO)) {
			throw new IllegalConfigurationException(SETTING + ": " + AUTO + " must be the only entry");
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
