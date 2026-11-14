package com.dell.spt.base.config;

import com.dell.spt.base.logging.Loggers;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.exceptions.InvalidValuePathException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Resolves the Mongoose 3.x setting {@code storage.net.http.versioning} for the S3 drivers.
 *
 * <p>Mongoose users turn on S3 versioning with {@code --storage-net-http-versioning=true}, while SPT's S3 drivers
 * read {@code storage.object.versioning}. With an S3 driver type and the synonym set to true, this sets
 * {@code storage.object.versioning} to true on the step configuration before the step records it, so the drivers,
 * the workers that receive slices of it, and the step's {@code config.yaml} all show versioning in effect.
 *
 * <p>Either key set to true turns versioning on. The synonym set to false, or absent, changes nothing: its shipped
 * default is false, so an explicit false can't be told apart from it, and it never turns
 * {@code storage.object.versioning} off. Other driver types are left alone; the Swift driver reads the key as its own
 * setting.
 */
public final class HttpVersioningSynonym {

	/** The Mongoose 3.x key. The HTTP protocol schema declares it, and so does the Swift driver's. */
	public static final String SYNONYM_PATH = "storage-net-http-versioning";
	/** The SPT key the S3 drivers read. */
	public static final String TARGET_PATH = "storage-object-versioning";
	/** The driver types whose drivers read {@code storage.object.versioning}. */
	public static final Set<String> S3_DRIVER_TYPES = Set.of("s3", "s3-rdma", "s3-tables", "s3-aws");

	static final String NOTICE = "The argument \"" + SYNONYM_PATH
					+ "\" is the Mongoose name for S3 versioning, use \"" + TARGET_PATH + "\" instead";

	private static final String DRIVER_TYPE_PATH = "storage-driver-type";
	/** Every step type builds dormant prototypes, so the notice is logged once per process, not once per step. */
	private static final AtomicBoolean NOTICE_LOGGED = new AtomicBoolean();

	private HttpVersioningSynonym() {}

	/**
	 * Resolves the synonym in place on a step configuration and on each of its context configurations. A context
	 * that omits the driver type or the synonym inherits it from the step configuration, as the merged context does.
	 */
	public static void resolve(final Config stepConfig, final List<Config> ctxConfigs) {
		resolve(stepConfig, ctxConfigs, NOTICE_LOGGED);
	}

	static void resolve(final Config stepConfig, final List<Config> ctxConfigs, final AtomicBoolean noticeLogged) {
		var applied = resolveOne(stepConfig, stepConfig);
		if (ctxConfigs != null) {
			for (final var ctxConfig : ctxConfigs) {
				applied |= resolveOne(ctxConfig, stepConfig);
			}
		}
		if (applied && noticeLogged.compareAndSet(false, true)) {
			Loggers.ERR.warn(NOTICE);
		}
	}

	/** Returns whether {@code config} has an S3 driver type with the synonym set to true. */
	private static boolean resolveOne(final Config config, final Config base) {
		if (config == null) {
			return false;
		}
		final var driverType = effectiveValue(config, base, DRIVER_TYPE_PATH);
		if (driverType == null || !S3_DRIVER_TYPES.contains(driverType.toString())) {
			return false;
		}
		final var synonym = effectiveValue(config, base, SYNONYM_PATH);
		if (synonym == null || !Boolean.parseBoolean(synonym.toString())) {
			return false;
		}
		if (!Boolean.parseBoolean(String.valueOf(value(config, TARGET_PATH)))) {
			try {
				config.val(TARGET_PATH, true);
			} catch (final InvalidValuePathException e) {
				// No S3 driver schema is loaded, so no driver reads the key.
				return false;
			}
		}
		return true;
	}

	private static Object effectiveValue(final Config config, final Config base, final String path) {
		final var value = value(config, path);
		return value != null || base == null || base == config ? value : value(base, path);
	}

	/** Returns the value at {@code path}, or null when it is unset or its schema isn't loaded. */
	private static Object value(final Config config, final String path) {
		try {
			return config.val(path);
		} catch (final NoSuchElementException | InvalidValuePathException e) {
			return null;
		}
	}
}
