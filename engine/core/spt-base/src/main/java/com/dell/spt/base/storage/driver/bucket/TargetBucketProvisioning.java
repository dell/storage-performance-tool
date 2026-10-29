package com.dell.spt.base.storage.driver.bucket;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Constants.KEY_STEP_ID;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

import com.dell.spt.base.control.run.RunScope;
import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.integrity.IntegrityTerminalException.Category;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.storage.driver.StorageDriverFactory;
import com.dell.spt.base.storage.driver.bucket.TargetBuckets.Target;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.Level;

/**
 * Target-bucket provisioning, owned by the controller engine.
 *
 * <p>Before a step's configuration is sliced and before any slice or metrics context starts, the
 * controller resolves the step's static target buckets, checks each one and creates a missing one
 * when the step may: its policy {@code storage.targetBucket.provision} is true and it writes new
 * objects into that bucket. Every other bucket is validated only. Provisioning never clears,
 * deletes or reconfigures a bucket.
 */
public final class TargetBucketProvisioning {

	static final String POLICY = "storage-targetBucket-provision";
	private static final String CLS_NAME = TargetBucketProvisioning.class.getSimpleName();

	public enum Outcome {
		EXISTING, CREATED
	}

	private TargetBucketProvisioning() {}

	/**
	 * Provisions the target buckets of a step: its own configuration, or each of its context
	 * configurations merged over it.
	 *
	 * <p>Initialization expressions in the bucket segment of {@code item.output.path} are evaluated
	 * here, once, and written back so every slice shares the value.
	 *
	 * @param stepId the step identity for messages, or null when it is not known yet
	 * @throws RunTerminalException when a target is unusable or the configuration names no static bucket
	 */
	public static void provision(
					final String stepId,
					final Config config,
					final List<Config> ctxConfigs,
					final List<Extension> extensions) {
		provision(stepId, config, ctxConfigs, extensions, TargetBucketStep.ORDINARY);
	}

	/**
	 * Provisions the target buckets of a step, as {@link #provision(String, Config, List, List)} does,
	 * for a step that tells setup what its configuration cannot: whether it is a mixed step and
	 * whether that step writes new objects. Every context's {@code storage.targetBucket.count} is
	 * validated first, for every storage driver.
	 *
	 * @param step what setup must know about the step beyond its configuration
	 */
	public static void provision(
					final String stepId,
					final Config config,
					final List<Config> ctxConfigs,
					final List<Extension> extensions,
					final TargetBucketStep step) {
		// The step's log context routes messages to its messages.log; closing it also undoes any
		// context a driver's control request leaves behind on this scenario thread.
		try (final var logCtx = logContext(stepId)) {
			final var targetsByDriver = new LinkedHashMap<Map<String, Object>, DriverTargets>();
			if (ctxConfigs == null || ctxConfigs.isEmpty()) {
				collect(config, null, config, extensions, step, false, targetsByDriver);
			} else {
				for (int index = 0; index < ctxConfigs.size(); index++) {
					final var ctxConfig = ctxConfigs.get(index);
					collect(config, ctxConfig, merge(config, ctxConfig), extensions, step, step.pipeline() && index > 0,
									targetsByDriver);
				}
			}
			for (final var driverTargets : targetsByDriver.values()) {
				provision(stepId, driverTargets);
			}
		} catch (final RunTerminalException e) {
			throw e.withStepId(stepId);
		}
	}

	private static CloseableThreadContext.Instance logContext(final String stepId) {
		final var context = CloseableThreadContext.put(KEY_CLASS_NAME, CLS_NAME);
		return stepId == null ? context : context.put(KEY_STEP_ID, stepId);
	}

	/**
	 * Adds one context's targets when its driver opts into provisioning. An initialization
	 * expression in the bucket segment of a written output path is evaluated first and written back
	 * where the path is defined, so every slice copies the resolved value.
	 */
	private static void collect(
					final Config config,
					final Config ctxConfig,
					final Config effective,
					final List<Extension> extensions,
					final TargetBucketStep step,
					final boolean fedByPreviousContext,
					final Map<Map<String, Object>, DriverTargets> targetsByDriver) {
		final var factory = controlFactory(extensions, effective.stringVal("storage-driver-type"));
		TargetBucketSet.validate(effective, step, factory != null, fedByPreviousContext);
		if (factory == null) {
			return;
		}
		var context = effective;
		// A bucket list replaces the output path's bucket segment, so there is nothing to evaluate in it.
		if (TargetBucketSet.names(context).isEmpty()
						&& TargetBuckets.writes(context, step, TargetBucketSet.count(context))) {
			final var owner = ctxConfig != null && definesOutputPath(ctxConfig) ? ctxConfig : config;
			if (resolveOutputPath(owner) && ctxConfig != null) {
				context = merge(config, ctxConfig);
			}
		}
		final var targets = TargetBuckets.targets(context, context.boolVal(POLICY), step);
		if (targets.isEmpty()) {
			return;
		}
		// Contexts share a control client only when their whole effective storage configuration is
		// equal: region, TLS, headers and credentials all shape the requests.
		final var storageConfig = context.configVal("storage");
		targetsByDriver
						.computeIfAbsent(Config.deepToMap(storageConfig), key -> new DriverTargets(factory, storageConfig))
						.merge(targets);
	}

	private static Config merge(final Config config, final Config ctxConfig) {
		final var merged = TreeUtil.reduceForest(List.of(Config.deepToMap(config), Config.deepToMap(ctxConfig)));
		return new BasicConfig(config.pathSep(), config.schema(), merged);
	}

	/** Returns true when the output path changed. */
	private static boolean resolveOutputPath(final Config config) {
		final var path = config.stringVal(TargetBuckets.OUTPUT_PATH);
		final var resolved = TargetBuckets.resolveOutputPath(path);
		if (resolved == null || resolved.equals(path)) {
			return false;
		}
		config.val(TargetBuckets.OUTPUT_PATH, resolved);
		return true;
	}

	@SuppressWarnings("unchecked")
	private static boolean definesOutputPath(final Config config) {
		try {
			final Object item = Config.deepToMap(config).get("item");
			if (!(item instanceof Map<?, ?> itemMap)) {
				return false;
			}
			final Object output = itemMap.get("output");
			return output instanceof Map<?, ?> outputMap && outputMap.get("path") != null;
		} catch (final RuntimeException e) {
			return false;
		}
	}

	private static TargetBucketControlFactory controlFactory(
					final List<Extension> extensions, final String driverType) {
		if (extensions == null || driverType == null) {
			return null;
		}
		for (final var extension : extensions) {
			if (extension instanceof StorageDriverFactory<?, ?, ?> factory && driverType.equals(factory.id())) {
				return extension instanceof TargetBucketControlFactory capability ? capability : null;
			}
		}
		return null;
	}

	private static void provision(final String stepId, final DriverTargets driverTargets) {
		final TargetBucketControl control;
		try {
			control = driverTargets.factory.openTargetBucketControl(stepId, driverTargets.storageConfig);
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			throwUnchecked(e);
			return;
		} catch (final RuntimeException e) {
			throw new RunTerminalException(
							Category.CONFIGURATION, stepId, "failed to open the target-bucket control client", e);
		}
		try (control) {
			for (final var target : driverTargets.targets.values()) {
				report(stepId, driverTargets.fingerprint, target, provision(control, target));
			}
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			throwUnchecked(e);
		}
	}

	/**
	 * Checks one target and creates it when it is missing and creatable. Only a definitive
	 * not-found response permits creation; after a failed creation exactly one recheck decides.
	 */
	static Outcome provision(final TargetBucketControl control, final Target target) throws InterruptedException {
		final var bucket = target.bucket();
		final var check = control.check(bucket, target.credential());
		switch (check.kind()) {
		case OK:
			return Outcome.EXISTING;
		case FAILED:
			throw failure("target bucket \"" + bucket + "\" is not accessible (" + check.detail() + ")");
		case NOT_FOUND:
		default:
			break;
		}
		if (!target.written()) {
			throw failure("target bucket \"" + bucket + "\" does not exist; this step uses existing objects only "
							+ "and never creates a bucket");
		}
		if (!target.creatable()) {
			throw failure("target bucket \"" + bucket + "\" does not exist and target-bucket provisioning is disabled "
							+ "(--provision-target-bucket=false, storage.targetBucket.provision: false)");
		}
		final var create = control.create(bucket, target.credential());
		if (create.kind() == TargetBucketResponse.Kind.OK) {
			return Outcome.CREATED;
		}
		final var recheck = control.check(bucket, target.credential());
		if (recheck.kind() == TargetBucketResponse.Kind.OK) {
			// Another engine or client created it first.
			return Outcome.EXISTING;
		}
		throw failure("failed to create target bucket \"" + bucket + "\" (" + create.detail()
						+ "); it is still not accessible (" + recheck.detail() + ")");
	}

	private static RunTerminalException failure(final String message) {
		return new RunTerminalException(Category.EXECUTION, message, null);
	}

	private static void report(final String stepId, final String fingerprint, final Target target, final Outcome outcome) {
		final var bucket = target.bucket();
		final var scope = RunScope.current();
		final var first = scope == null || scope.firstReport(
						fingerprint + "/" + bucket + "/" + (target.credential() == null ? "" : target.credential().getUid()));
		final var level = first ? Level.INFO : Level.DEBUG;
		final var step = stepId == null ? "load step" : stepId;
		// A control request may have replaced the class name of this thread's log context.
		try (final var logCtx = CloseableThreadContext.put(KEY_CLASS_NAME, CLS_NAME)) {
			if (outcome == Outcome.CREATED) {
				Loggers.MSG.log(level, "{}: created target bucket \"{}\"", step, bucket);
			} else {
				Loggers.MSG.log(level, "{}: using existing target bucket \"{}\"", step, bucket);
			}
		}
	}

	/**
	 * Returns a non-secret fingerprint of the storage a bucket lives in, used only to report each
	 * target once per run. Control-client reuse is keyed by exact configuration equality instead.
	 */
	static String fingerprint(final Config storageConfig) {
		final var identity = new StringBuilder(storageConfig.stringVal("driver-type"));
		try {
			identity.append('|').append(storageConfig.listVal("net-node-addrs"));
			identity.append('|').append(storageConfig.val("net-node-port"));
		} catch (final RuntimeException ignored) {
			// drivers without the Netty endpoint settings share one fingerprint per driver type
		}
		identity.append('|').append(storageConfig.stringVal("auth-uid"));
		try {
			final var digest = MessageDigest.getInstance("SHA-256").digest(identity.toString().getBytes(StandardCharsets.UTF_8));
			return HexFormat.of().formatHex(digest, 0, 16);
		} catch (final NoSuchAlgorithmException e) {
			throw new IllegalStateException("SHA-256 is unavailable", e);
		}
	}

	/** Targets sharing one effective storage configuration, deduplicated by bucket and credential. */
	private static final class DriverTargets {

		private final TargetBucketControlFactory factory;
		private final Config storageConfig;
		private final String fingerprint;
		private final Map<TargetBuckets.Key, Target> targets = new LinkedHashMap<>();

		private DriverTargets(final TargetBucketControlFactory factory, final Config storageConfig) {
			this.factory = factory;
			this.storageConfig = storageConfig;
			this.fingerprint = fingerprint(storageConfig);
		}

		private void merge(final List<Target> more) {
			for (final var target : more) {
				targets.merge(target.key(), target, Target::mergedWith);
			}
		}
	}
}
