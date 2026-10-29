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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;

/**
 * The target bucket set of a step: the {@code N} buckets its new objects are spread over, either
 * generated from {@code storage.targetBucket.count} or named by {@code storage.targetBucket.names}.
 *
 * <p>With {@code N = 1} and no list the set is the output path's own bucket and nothing here
 * changes a path. With {@code N > 1} and no list the output path's bucket is only the base of the
 * member names {@code <base>-0} to {@code <base>-<N-1>}; it is not itself a member. A non-empty
 * list is the whole set: member {@code i} is {@code names[i]}, used as written, and the output
 * path's bucket segment is only a placeholder that each member path replaces. The set governs only
 * new objects: existing objects keep the bucket their input records, and {@code item.input.path}
 * is used as written.
 */
public final class TargetBucketSet {

	/** The engine setting, as a dashed configuration path. */
	public static final String COUNT = "storage-targetBucket-count";
	/** The engine setting that names the buckets, as a dashed configuration path. */
	public static final String NAMES = "storage-targetBucket-names";
	public static final int MAX_COUNT = 1000;

	private static final String COUNT_SETTING = "storage.targetBucket.count";
	private static final String NAMES_SETTING = "storage.targetBucket.names";
	private static final List<String> EXPRESSION_MARKERS = List.of(SYNC_MARKER, ASYNC_MARKER, INIT_MARKER);
	private static final Pattern IPV4_FORM = Pattern.compile("\\d+\\.\\d+\\.\\d+\\.\\d+");
	private static final List<String> RESERVED_PREFIXES = List.of("xn--", "sthree-", "amzn-s3-demo-");
	private static final List<String> RESERVED_SUFFIXES = List.of("-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3");

	private TargetBucketSet() {}

	/**
	 * Returns the number of buckets in the set: the length of the configured list, or the
	 * configured count when there is no list. A configuration without either has one bucket.
	 *
	 * @throws RunTerminalException when the count is outside 1 to {@value #MAX_COUNT}, the list is
	 *     invalid, or a non-empty list is combined with a count above 1
	 */
	public static int count(final Config config) {
		return count(config, COUNT, NAMES);
	}

	/**
	 * Returns the bucket names of a configuration's list, in order, or an empty list when the set
	 * is generated.
	 *
	 * @throws RunTerminalException when a name is invalid, a name repeats, or the list is longer
	 *     than {@value #MAX_COUNT}
	 */
	public static List<String> names(final Config config) {
		return names(config, NAMES);
	}

	private static int count(final Config config, final String countPath, final String namesPath) {
		final int generated = generatedCount(config, countPath);
		final var names = names(config, namesPath);
		if (names.isEmpty()) {
			return generated;
		}
		if (generated > 1) {
			throw configuration(NAMES_SETTING + " lists " + names.size() + " buckets and cannot be combined with "
							+ COUNT_SETTING + " " + generated + ": the list is the whole set, so set only one of them");
		}
		return names.size();
	}

	private static List<String> names(final Config config, final String path) {
		final List<?> configured;
		try {
			configured = config.listVal(path);
		} catch (final NoSuchElementException e) {
			// A configuration without the setting, as built by an embedding test or tool, has no list.
			return List.of();
		}
		if (configured == null || configured.isEmpty()) {
			return List.of();
		}
		if (configured.size() > MAX_COUNT) {
			throw configuration(NAMES_SETTING + " lists " + configured.size() + " buckets, at most " + MAX_COUNT);
		}
		final var names = new ArrayList<String>(configured.size());
		final var seen = new HashSet<String>();
		for (int index = 0; index < configured.size(); index++) {
			final var name = configured.get(index) == null ? "" : String.valueOf(configured.get(index));
			final var problem = listedNameProblem(name);
			if (problem != null) {
				throw configuration(NAMES_SETTING + "[" + index + "] \"" + name + "\" " + problem);
			}
			if (!seen.add(name)) {
				throw configuration(NAMES_SETTING + "[" + index + "] \"" + name + "\" repeats an earlier name");
			}
			names.add(name);
		}
		return List.copyOf(names);
	}

	/**
	 * Returns why a listed name cannot be used as written, or null. The list is the user's
	 * statement of what the target accepts, so only the structure is checked, never the AWS naming
	 * rules that apply to generated names.
	 */
	static String listedNameProblem(final String name) {
		if (name.isEmpty()) {
			return "must not be empty";
		}
		for (int i = 0; i < name.length(); i++) {
			final char c = name.charAt(i);
			if (c == '/' || c == ',') {
				return "must not contain '" + c + "'";
			}
			if (Character.isWhitespace(c)) {
				return "must not contain whitespace";
			}
		}
		for (final var marker : EXPRESSION_MARKERS) {
			if (name.contains(marker)) {
				return "must not contain the expression marker " + marker;
			}
		}
		return null;
	}

	private static int generatedCount(final Config config, final String path) {
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

	/** Returns the number of buckets in the set of a step's storage configuration, {@code storage}. */
	public static int countOfStorage(final Config storageConfig) {
		return count(storageConfig, "targetBucket-count", "targetBucket-names");
	}

	/** Returns the listed bucket names of a step's storage configuration, empty when generated. */
	public static List<String> namesOfStorage(final Config storageConfig) {
		return names(storageConfig, "targetBucket-names");
	}

	/**
	 * Returns the member paths of a base path for a set of {@code count} buckets: from the list when
	 * {@code names} is not empty, otherwise generated from the base. This is the one resolution that
	 * setup and the operation builders share.
	 *
	 * @param names the validated list, or empty for a generated set
	 * @throws RunTerminalException when a generated member name is not a valid bucket name
	 */
	public static List<String> memberPaths(final String basePath, final int count, final List<String> names) {
		return names.isEmpty() ? memberPaths(basePath, count) : memberPaths(basePath, names);
	}

	/**
	 * Returns the member paths of a listed set: with one name the path with its bucket segment
	 * replaced and nothing else changed, otherwise the normalized path with its bucket segment
	 * replaced by each name in order. The bucket segment of {@code basePath} is only a placeholder:
	 * it is never evaluated and never contacted unless an input names it. A path without a bucket
	 * segment gives {@code /<name>}.
	 *
	 * @param names the validated list, not empty
	 */
	static List<String> memberPaths(final String basePath, final List<String> names) {
		final var path = basePath == null ? "" : basePath;
		if (names.size() == 1) {
			if (path.isEmpty()) {
				return Collections.singletonList("/" + names.get(0));
			}
			final int start = path.startsWith("/") ? 1 : 0;
			final var placeholder = TargetBuckets.bucketSegment(path);
			return Collections.singletonList(
							path.substring(0, start) + names.get(0) + path.substring(start + placeholder.length()));
		}
		final var stripped = path.replaceFirst("^/+", "");
		var rest = stripped.substring(TargetBuckets.bucketSegment(stripped).length());
		int end = rest.length();
		while (end > 0 && rest.charAt(end - 1) == '/') {
			end--;
		}
		rest = rest.substring(0, end);
		final var paths = new ArrayList<String>(names.size());
		for (final var name : names) {
			paths.add("/" + name + rest);
		}
		return List.copyOf(paths);
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
		final var normalized = normalized(basePath == null ? "" : basePath);
		final var base = normalized.base();
		final var rest = normalized.rest();
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

	/** A path with one leading slash and no trailing slash, split into its bucket segment and the rest. */
	private record NormalizedPath(String base, String rest) {}

	private static NormalizedPath normalized(final String path) {
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
		return new NormalizedPath(slash < 0 ? trimmed : trimmed.substring(0, slash), slash < 0 ? "" : trimmed.substring(slash));
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
		final boolean listed = !names(ctxConfig).isEmpty();
		final var setting = listed ? NAMES_SETTING + " (" + count + " names)" : COUNT_SETTING + " " + count;
		final var members = listed ? "listed" : "generated";
		final var scope = listed ? "the bucket list" : "the bucket count";
		if (!hasControl) {
			throw configuration(setting + " requires the s3, s3-aws or s3-rdma storage driver, not \""
							+ ctxConfig.stringVal("storage-driver-type") + "\"");
		}
		if (notEmpty(ctxConfig.stringVal("storage-auth-file"))) {
			throw configuration(setting + " cannot be combined with storage.auth.file: every " + members + " bucket uses "
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
				throw configuration(setting + " cannot be combined with item.input.file in a CREATE step: "
								+ scope + " applies only to newly generated objects");
			}
			if (fedByPreviousContext) {
				throw configuration(setting + " cannot be combined with a pipeline CREATE sub-step after the first, "
								+ "which writes the items the previous sub-step passes on: " + scope + " applies "
								+ "only to newly generated objects");
			}
		}
		if (TargetBuckets.writes(ctxConfig, step, count)) {
			final var outputPath = ctxConfig.stringVal(TargetBuckets.OUTPUT_PATH);
			if (!notEmpty(outputPath)) {
				if (listed) {
					return;
				}
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
