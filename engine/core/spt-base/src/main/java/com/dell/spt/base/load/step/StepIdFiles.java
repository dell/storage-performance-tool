package com.dell.spt.base.load.step;

import com.dell.spt.base.config.NicSetConfig;
import com.dell.spt.base.config.RangeReadConfig;
import com.dell.spt.base.integrity.IntegrityConfig;
import com.dell.spt.base.integrity.IntegrityCsvArtifacts;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.load.lifecycle.OperationLifecycleArtifact;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.range.RangeReadArtifact;
import com.dell.spt.base.util.BinarySizeFormat;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.commons.reflection.TypeUtil;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * The logger files a step publishes once per step id and refuses to replace at its close: the
 * operation lifecycle and range read evidence of a CREATE or READ step, the integrity logs of a
 * metadata-mode step, and the NIC files of a step with a NIC set. The Entry Node merges the first
 * two kinds from every slice, and collects the NIC files. Both the Entry Node and each slice test
 * these files at the step's start, from this one list, so that a step whose id an earlier step used
 * in the same log directory is refused before any load.
 */
public final class StepIdFiles {

	/** A logger-backed CSV that every slice writes and the Entry Node merges at close. */
	public record LoggerArtifact(String loggerName, String fileName, String header) {}

	/** The loggers of the NIC files a slice with a NIC set publishes. */
	public static final List<String> NIC_LOGGER_NAMES = List.of(Loggers.NIC_PATHS.getName(), Loggers.NIC_SET.getName());
	/** The loggers of the integrity logs, which an appender writes and may hold open after the step. */
	public static final Set<String> APPENDER_LOGGER_NAMES = Set.of(
					Loggers.INTEGRITY_FAILURES.getName(), Loggers.INTEGRITY_PERFORMANCE.getName(), Loggers.MULTIPART_LIFECYCLE.getName());
	/** The loggers of every file of this kind, of any step. */
	public static final Set<String> LOGGER_NAMES = Set.of(
					Loggers.OPERATION_LIFECYCLE.getName(),
					Loggers.RANGE_READ.getName(),
					Loggers.INTEGRITY_FAILURES.getName(),
					Loggers.INTEGRITY_PERFORMANCE.getName(),
					Loggers.MULTIPART_LIFECYCLE.getName(),
					Loggers.NIC_PATHS.getName(),
					Loggers.NIC_SET.getName());

	private StepIdFiles() {}

	/** The terminal lifecycle evidence of a CREATE or READ step, with its range reads when it has a range policy. */
	public static List<LoggerArtifact> operationLifecycle(final OpType opType, final boolean rangeRead) {
		if (opType != OpType.CREATE && opType != OpType.READ) {
			return List.of();
		}
		final var artifacts = new ArrayList<LoggerArtifact>(2);
		artifacts.add(new LoggerArtifact(Loggers.OPERATION_LIFECYCLE.getName(),
						OperationLifecycleArtifact.FILE_NAME, OperationLifecycleArtifact.HEADER));
		if (rangeRead) {
			artifacts.add(new LoggerArtifact(Loggers.RANGE_READ.getName(), RangeReadArtifact.FILE_NAME, RangeReadArtifact.HEADER));
		}
		return artifacts;
	}

	/** The integrity logger artifacts of a metadata-mode step of {@code opType}. */
	public static List<LoggerArtifact> integrity(
					final IntegrityConfig integrityConfig, final OpType opType, final Config itemConfig) {
		if (!integrityConfig.enabled()) {
			return List.of();
		}
		final var failures = new LoggerArtifact(Loggers.INTEGRITY_FAILURES.getName(),
						IntegrityCsvArtifacts.FAILURES_FILE_NAME, IntegrityCsvArtifacts.FAILURES_HEADER);
		final var performance = new LoggerArtifact(Loggers.INTEGRITY_PERFORMANCE.getName(),
						IntegrityCsvArtifacts.PERFORMANCE_FILE_NAME, IntegrityCsvArtifacts.PERFORMANCE_HEADER);
		if (OpType.READ.equals(opType)) {
			return List.of(failures, performance);
		}
		if (!OpType.CREATE.equals(opType)) {
			return List.of();
		}
		final var threshold = itemConfig.configVal("data").configVal("ranges").val("threshold");
		final long thresholdBytes = threshold instanceof String
						? BinarySizeFormat.parseFixedSize((String) threshold)
						: TypeUtil.typeConvert(threshold, long.class);
		return thresholdBytes > 0
						? List.of(performance, new LoggerArtifact(Loggers.MULTIPART_LIFECYCLE.getName(),
										IntegrityCsvArtifacts.MULTIPART_LIFECYCLE_FILE_NAME,
										IntegrityCsvArtifacts.MULTIPART_LIFECYCLE_HEADER))
						: List.of(performance);
	}

	/** Whether the step, or any of its contexts, reads ranges. */
	public static boolean rangeRead(final Config config, final List<Config> ctxConfigs) {
		if (ctxConfigs == null || ctxConfigs.isEmpty()) {
			return RangeReadConfig.fromLoad(config.configVal("load")) != null;
		}
		for (final var context : ctxConfigs) {
			final var merged = TreeUtil.reduceForest(List.of(Config.deepToMap(config), Config.deepToMap(context)));
			final var effective = new BasicConfig(config.pathSep(), config.schema(), merged);
			if (RangeReadConfig.fromLoad(effective.configVal("load")) != null) {
				return true;
			}
		}
		return false;
	}

	/** Whether this slice requested a NIC set, in its base or any context configuration. */
	public static boolean nicSet(final Config config, final List<Config> ctxConfigs) {
		return NicSetConfig.isSet(config.configVal("storage"))
						|| (ctxConfigs != null && ctxConfigs.stream().anyMatch(ctx -> NicSetConfig.isSet(ctx.configVal("storage"))));
	}

	public static OpType opType(final Config config) {
		return OpType.valueOf(config.configVal("load").configVal("op").stringVal("type").toUpperCase(Locale.ROOT));
	}

	/**
	 * The artifacts the Entry Node merges from every slice: each kind from the first configuration,
	 * the base one then each context's, that has any, as the Entry Node builds its aggregators.
	 */
	public static List<LoggerArtifact> merged(final Config config, final List<Config> ctxConfigs) {
		final var configs = new ArrayList<Config>();
		configs.add(config);
		if (ctxConfigs != null) {
			configs.addAll(ctxConfigs);
		}
		final boolean rangeRead = rangeRead(config, ctxConfigs);
		List<LoggerArtifact> lifecycle = List.of();
		List<LoggerArtifact> integrity = List.of();
		for (final var stepConfig : configs) {
			final var opType = opType(stepConfig);
			if (lifecycle.isEmpty()) {
				lifecycle = operationLifecycle(opType, rangeRead);
			}
			if (integrity.isEmpty()) {
				integrity = integrity(
								IntegrityConfig.fromStorage(stepConfig.configVal("storage")), opType, stepConfig.configVal("item"));
			}
		}
		final var all = new ArrayList<LoggerArtifact>(lifecycle.size() + integrity.size());
		all.addAll(lifecycle);
		all.addAll(integrity);
		return List.copyOf(all);
	}

	/**
	 * The loggers of every file a slice of this configuration publishes and refuses to replace: the
	 * {@link #merged} artifacts, and the NIC files when the slice {@code nicSet} requested a NIC set.
	 */
	public static List<String> sliceLoggerNames(final List<LoggerArtifact> merged, final boolean nicSet) {
		final var names = new ArrayList<String>(merged.size() + NIC_LOGGER_NAMES.size());
		merged.forEach(artifact -> names.add(artifact.loggerName()));
		if (nicSet) {
			names.addAll(NIC_LOGGER_NAMES);
		}
		return List.copyOf(names);
	}

	/**
	 * Whether {@code path} exists on this node. Unlike {@link Files#exists}, a file that cannot be
	 * tested, for a denied directory for example, is neither present nor absent.
	 *
	 * @throws IOException when the file system cannot tell
	 */
	public static boolean exists(final Path path) throws IOException {
		try {
			Files.readAttributes(path, BasicFileAttributes.class);
			return true;
		} catch (final NoSuchFileException missing) {
			return false;
		}
	}

	/**
	 * Whether {@code fileName} exists where {@code fileManager} runs, with the calls a remote file
	 * manager already serves: a file with content has a size, and an empty one reads as its end.
	 *
	 * @throws IOException when the file manager cannot tell
	 */
	public static boolean exists(final FileManager fileManager, final String fileName) throws IOException {
		if (fileManager.fileSize(fileName) > 0) {
			return true;
		}
		try {
			fileManager.readFromFile(fileName, 0);
			return true;
		} catch (final EOFException empty) {
			return true;
		} catch (final NoSuchFileException missing) {
			return false;
		}
	}
}
