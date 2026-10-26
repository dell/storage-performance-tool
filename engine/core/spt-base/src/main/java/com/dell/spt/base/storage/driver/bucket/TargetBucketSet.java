package com.dell.spt.base.storage.driver.bucket;

import static com.github.akurilov.commons.io.el.ExpressionInput.ASYNC_MARKER;
import static com.github.akurilov.commons.io.el.ExpressionInput.INIT_MARKER;
import static com.github.akurilov.commons.io.el.ExpressionInput.SYNC_MARKER;

import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException.Category;
import com.dell.spt.base.item.ItemType;
import com.dell.spt.base.item.op.OpType;
import com.github.akurilov.confuse.Config;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;

/**
 * The target bucket set of a step: the {@code N} buckets ({@code storage.targetBucket.count}) its
 * new objects are spread over.
 *
 * <p>With {@code N = 1} the set is the output path's own bucket and nothing here changes a path.
 * With {@code N > 1} the output path's bucket is only the base of the member names
 * {@code <base>-0} to {@code <base>-<N-1>}; it is not itself a member. The count governs only new
 * objects: existing objects keep the bucket their input records, and {@code item.input.path} is
 * used as written.
 */
public final class TargetBucketSet {

	/** The engine setting, as a dashed configuration path. */
	public static final String COUNT = "storage-targetBucket-count";
	public static final int MAX_COUNT = 1000;

	private static final String COUNT_SETTING = "storage.targetBucket.count";
	private static final Pattern IPV4_FORM = Pattern.compile("\\d+\\.\\d+\\.\\d+\\.\\d+");
	private static final List<String> RESERVED_PREFIXES = List.of("xn--", "sthree-", "amzn-s3-demo-");
	private static final List<String> RESERVED_SUFFIXES = List.of("-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3");

	private TargetBucketSet() {}

	/**
	 * Returns the configured bucket count; a configuration without the setting has one bucket.
	 *
	 * @throws RunTerminalException when the count is outside 1 to {@value #MAX_COUNT}
	 */
	public static int count(final Config config) {
		return count(config, COUNT);
	}

	private static int count(final Config config, final String path) {
		// A configuration without the setting, as built by an embedding test or tool, targets one bucket.
		final Object configured;
		try {
			configured = config.val(path);
		} catch (final NoSuchElementException e) {
			return 1;
		}
		if (configured == null) {
			return 1;
		}
		final int count = config.intVal(path);
		if (count < 1 || count > MAX_COUNT) {
			throw configuration(COUNT_SETTING + " must be between 1 and " + MAX_COUNT + ", not " + count);
		}
		return count;
	}

	/**
	 * Returns the index of the member a new object goes to: {@code floorMod(floorDiv(id, d), count)},
	 * where {@code d} is the item naming shard count, or 1 without shards. Dividing by the shard
	 * count first keeps the bucket independent of the object's shard label, so every member gets
	 * every label.
	 *
	 * @param id the new item's id, its offset
	 * @param shardDivisor {@code max(item.naming.shards, 1)}
	 */
	public static int memberIndex(final long id, final int shardDivisor, final int count) {
		return Math.floorMod(shardDivisor == 1 ? id : Math.floorDiv(id, shardDivisor), count);
	}

	/** Returns the bucket count of a step's storage configuration, {@code storage}. */
	public static int countOfStorage(final Config storageConfig) {
		return count(storageConfig, "targetBucket-count");
	}

	/**
	 * Returns the {@code count} member paths of a base path: with one member the path itself,
	 * otherwise the normalized path with its bucket segment replaced by each member name, in index
	 * order. A static prefix after the bucket is kept: {@code /bench/a/b} gives {@code /bench-3/a/b}.
	 *
	 * @throws RunTerminalException when the path names no bucket or a member name is not a valid
	 *     bucket name
	 */
	public static List<String> memberPaths(final String basePath, final int count) {
		if (count == 1) {
			return Collections.singletonList(basePath);
		}
		final var path = basePath == null ? "" : basePath;
		int start = 0;
		while (start < path.length() && path.charAt(start) == '/') {
			start++;
		}
		int end = path.length();
		while (end > start && path.charAt(end - 1) == '/') {
			end--;
		}
		final var trimmed = path.substring(start, end);
		final int slash = trimmed.indexOf('/');
		final var base = slash < 0 ? trimmed : trimmed.substring(0, slash);
		final var rest = slash < 0 ? "" : trimmed.substring(slash);
		if (base.isEmpty()) {
			throw configuration(COUNT_SETTING + " " + count + " requires item.output.path to name a base bucket");
		}
		final var paths = new ArrayList<String>(count);
		for (int index = 0; index < count; index++) {
			final var name = base + "-" + index;
			final var problem = nameProblem(name);
			if (problem != null) {
				throw configuration(COUNT_SETTING + " " + count + ": the generated bucket name \"" + name
								+ "\" is not a valid bucket name: " + problem);
			}
			paths.add("/" + name + rest);
		}
		return List.copyOf(paths);
	}

	/**
	 * Returns why a generated member name breaks the AWS general-purpose bucket naming rules, or
	 * null when it follows them. Only generated names are checked; a bucket that an input names is
	 * used as written.
	 */
	static String nameProblem(final String name) {
		if (name.length() < 3 || name.length() > 63) {
			return "it must be 3 to 63 characters long";
		}
		for (int i = 0; i < name.length(); i++) {
			final char c = name.charAt(i);
			if (!lowercaseLetterOrDigit(c) && c != '-' && c != '.') {
				return "it may contain only lowercase letters, digits, hyphens and periods";
			}
		}
		if (!lowercaseLetterOrDigit(name.charAt(0)) || !lowercaseLetterOrDigit(name.charAt(name.length() - 1))) {
			return "it must start and end with a lowercase letter or digit";
		}
		if (name.contains("..")) {
			return "it must not contain two adjacent periods";
		}
		if (IPV4_FORM.matcher(name).matches()) {
			return "it must not be formatted as an IP address";
		}
		for (final var prefix : RESERVED_PREFIXES) {
			if (name.startsWith(prefix)) {
				return "it must not start with the reserved prefix \"" + prefix + "\"";
			}
		}
		for (final var suffix : RESERVED_SUFFIXES) {
			if (name.endsWith(suffix)) {
				return "it must not end with the reserved suffix \"" + suffix + "\"";
			}
		}
		return null;
	}

	private static boolean lowercaseLetterOrDigit(final char c) {
		return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9');
	}

	/**
	 * Validates a step context's use of the bucket count before any control request: the count is
	 * always bounded, and with more than one bucket the combinations the set cannot serve fail.
	 *
	 * @param hasControl whether the selected storage driver provisions target buckets
	 * @param fedByPreviousContext whether the context takes its items from the previous context of a
	 *     pipeline step
	 * @throws RunTerminalException when the configuration is invalid
	 */
	static void validate(
					final Config ctxConfig,
					final TargetBucketStep step,
					final boolean hasControl,
					final boolean fedByPreviousContext) {
		final int count = count(ctxConfig);
		if (count == 1) {
			return;
		}
		final var setting = COUNT_SETTING + " " + count;
		if (!hasControl) {
			throw configuration(setting + " requires the s3, s3-aws or s3-rdma storage driver, not \""
							+ ctxConfig.stringVal("storage-driver-type") + "\"");
		}
		if (notEmpty(ctxConfig.stringVal("storage-auth-file"))) {
			throw configuration(setting + " cannot be combined with storage.auth.file: every generated bucket uses "
							+ "storage.auth.uid and storage.auth.secret");
		}
		final var opType = OpType.valueOf(ctxConfig.stringVal("load-op-type").toUpperCase(Locale.ROOT));
		if (OpType.LIST.equals(opType)) {
			throw configuration(setting + " cannot be combined with LIST, which lists a single bucket");
		}
		final var itemType = ItemType.valueOf(ctxConfig.stringVal("item-type").toUpperCase(Locale.ROOT));
		if (!ItemType.DATA.equals(itemType)) {
			throw configuration(setting + " requires item.type data, not " + itemType.name().toLowerCase(Locale.ROOT));
		}
		final var inputPath = ctxConfig.stringVal(TargetBuckets.INPUT_PATH);
		if (step.mixed()) {
			if (notEmpty(inputPath)) {
				throw configuration(setting + " cannot be combined with item.input.path in a mixed step: its PUTs "
								+ "would copy from that path instead of writing new objects");
			}
		} else if (OpType.CREATE.equals(opType)) {
			if (notEmpty(inputPath)) {
				throw configuration(setting + " cannot be combined with item.input.path in a CREATE step, which "
								+ "would copy objects instead of writing new ones");
			}
			if (notEmpty(ctxConfig.stringVal(TargetBuckets.INPUT_FILE))) {
				throw configuration(setting + " cannot be combined with item.input.file in a CREATE step: the "
								+ "bucket count applies only to newly generated objects");
			}
			if (fedByPreviousContext) {
				throw configuration(setting + " cannot be combined with a pipeline CREATE sub-step after the first, "
								+ "which writes the items the previous sub-step passes on: the bucket count applies "
								+ "only to newly generated objects");
			}
		}
		if (TargetBuckets.writes(ctxConfig, step, count)) {
			final var outputPath = ctxConfig.stringVal(TargetBuckets.OUTPUT_PATH);
			if (!notEmpty(outputPath)) {
				throw configuration(setting + " requires item.output.path to name a base bucket");
			}
			final var bucket = TargetBuckets.bucketSegment(outputPath);
			final var rest = outputPath.substring(Math.min(outputPath.length(),
							(outputPath.startsWith("/") ? 1 : 0) + bucket.length()));
			if (rest.contains(SYNC_MARKER) || rest.contains(ASYNC_MARKER) || rest.contains(INIT_MARKER)) {
				throw configuration(setting + " requires a static item.output.path after the bucket: expressions "
								+ "such as ${...}, #{...} or %{...} there are supported only with one bucket");
			}
		}
	}

	private static boolean notEmpty(final String value) {
		return value != null && !value.isEmpty();
	}

	private static RunTerminalException configuration(final String message) {
		return new RunTerminalException(Category.CONFIGURATION, message, null);
	}
}
