package com.dell.spt.base.load.step.client;

import static com.github.akurilov.commons.io.el.ExpressionInput.ASYNC_MARKER;
import static com.github.akurilov.commons.io.el.ExpressionInput.INIT_MARKER;
import static com.github.akurilov.commons.io.el.ExpressionInput.SYNC_MARKER;

import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.ItemNamePaths;
import com.dell.spt.base.item.ItemType;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.storage.driver.bucket.TargetBucketSet;
import com.github.akurilov.confuse.Config;
import java.io.File;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;

/**
 * Refuses, or warns about, a copy whose constant output path overlaps the storage path it lists.
 *
 * <p>A copy names its destination by the listed item name whenever that name already holds the
 * output path, so an output path that overlaps the input path can make a destination equal to its
 * source. The step client checks the step's own configuration once, before it lists the input path
 * into the items files it hands each engine, so this is the one place that can still tell a listed
 * path from an items file. Only a copy that lists a storage path into one constant output path can be
 * checked: an output path expression, a Target Bucket Set of several buckets, or an items file name
 * their destinations item by item.
 *
 * <p>On {@code fs} a listed name is the file's absolute path, and a destination equal to its source
 * opens the source for writing and truncates it, so an output path that is the listed directory, or
 * one of its parent directories, is refused. The listed directory is the input path as the listing
 * names it: doubled and trailing slashes dropped, and a relative input path made absolute against the
 * working directory. The output path compares as written, as the driver compares it with each name: a
 * relative output path never holds an absolute name, and {@code .} and {@code ..} segments are compared
 * as written on both sides. Symlink and hard-link aliases are not checked.
 *
 * <p>Other drivers compare the paths without their leading and trailing slashes. Equal paths copy
 * every object onto itself, so they are refused. An output path under the input path copies correctly
 * into an empty prefix, but the listing also returns any keys already under the output path, such as
 * an earlier run's copies, and those are copied onto themselves: that is a warning.
 */
final class CopyIntoItsSource {

	/** The {@code storage.driver.type} of the file system driver, whose item names are file paths. */
	static final String FS_DRIVER_TYPE = "fs";

	private static final char SEPARATOR = '/';

	private CopyIntoItsSource() {}

	/**
	 * Checks a step configuration whose item input feeds the step's operations.
	 *
	 * @throws RunTerminalException when every copy, or each copy on {@code fs}, would land on its source
	 */
	static void check(final String stepId, final Config config) {
		final var itemConfig = config.configVal("item");
		final var storageConfig = config.configVal("storage");
		final var opType = OpType.valueOf(
						config.configVal("load").configVal("op").stringVal("type").toUpperCase(java.util.Locale.ROOT));
		final var itemType = ItemType.valueOf(itemConfig.stringVal("type").toUpperCase(java.util.Locale.ROOT));
		if (!OpType.CREATE.equals(opType) || !ItemType.DATA.equals(itemType)) {
			return;
		}
		final var inputConfig = itemConfig.configVal("input");
		final var inputFile = inputConfig.stringVal("file");
		final var inputPath = inputConfig.stringVal("path");
		if ((inputFile != null && !inputFile.isEmpty()) || inputPath == null || inputPath.isEmpty()) {
			return;
		}
		final var outputPath = constantOutputPath(itemConfig.stringVal("output-path"), storageConfig);
		if (outputPath == null) {
			return;
		}
		if (FS_DRIVER_TYPE.equals(storageConfig.stringVal("driver-type"))) {
			final var listedDir = listedDirectory(inputPath);
			if (listedDir != null
							&& ItemNamePaths.startsWithPath(withoutTrailingSlashes(listedDir) + "/x", outputPath)) {
				throw refusal(stepId, "The copy output path \"" + outputPath + "\" is the item input path \"" + inputPath
								+ "\" or one of its parent directories: each copy would write over its own source file and "
								+ "empty it. Choose an output path outside the input path");
			}
			return;
		}
		final var input = withoutLeadingAndTrailingSlashes(inputPath);
		final var output = withoutLeadingAndTrailingSlashes(outputPath);
		if (input.isEmpty() || output.isEmpty()) {
			return;
		}
		if (input.equals(output)) {
			throw refusal(stepId, "The copy output path \"" + outputPath + "\" is the item input path \"" + inputPath
							+ "\": every object would be copied onto itself. Choose a different output path");
		}
		if (ItemNamePaths.startsWithPath(output, input)) {
			Loggers.MSG.warn(
							"{}: the copy output path \"{}\" lies under the item input path \"{}\": the listing also "
											+ "returns keys already under the output path, such as an earlier run's copies, and those "
											+ "are copied onto themselves",
							stepId, outputPath, inputPath);
		}
	}

	/**
	 * Returns the one output path every copy of the step names, as the load generator resolves it, or
	 * null when destinations are decided item by item.
	 */
	private static String constantOutputPath(final String configured, final Config storageConfig) {
		if (configured == null || configured.isEmpty()) {
			return null;
		}
		final var names = TargetBucketSet.namesOfStorage(storageConfig);
		final int memberCount = names.isEmpty() ? TargetBucketSet.countOfStorage(storageConfig) : names.size();
		if (memberCount > 1) {
			return null;
		}
		// a one-name list is a single target bucket at that name, as LoadGeneratorBuilderImpl resolves it
		final var path = names.size() == 1 ? TargetBucketSet.memberPaths(configured, 1, names).get(0) : configured;
		if (path.contains(ASYNC_MARKER) || path.contains(SYNC_MARKER) || path.contains(INIT_MARKER)) {
			return null;
		}
		return path;
	}

	/**
	 * Returns the directory part of the names an {@code fs} listing of {@code inputPath} gives, as the
	 * listing builds them: the path parsed, which drops doubled and trailing slashes, then made absolute
	 * against the working directory. Dot segments stay as written. Null when the path cannot be parsed,
	 * which fails the listing anyway.
	 */
	private static String listedDirectory(final String inputPath) {
		try {
			return new File(Paths.get(inputPath).toString()).getAbsolutePath();
		} catch (final InvalidPathException e) {
			return null;
		}
	}

	private static RunTerminalException refusal(final String stepId, final String message) {
		return new RunTerminalException(IntegrityTerminalException.Category.CONFIGURATION, stepId, message, null);
	}

	private static String withoutTrailingSlashes(final String path) {
		int end = path.length();
		while (end > 0 && path.charAt(end - 1) == SEPARATOR) {
			end--;
		}
		return path.substring(0, end);
	}

	private static String withoutLeadingAndTrailingSlashes(final String path) {
		final var trimmed = withoutTrailingSlashes(path);
		int start = 0;
		while (start < trimmed.length() && trimmed.charAt(start) == SEPARATOR) {
			start++;
		}
		return trimmed.substring(start);
	}
}
