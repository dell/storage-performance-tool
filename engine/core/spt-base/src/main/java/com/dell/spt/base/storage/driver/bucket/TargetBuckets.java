package com.dell.spt.base.storage.driver.bucket;

import static com.github.akurilov.commons.io.el.ExpressionInput.ASYNC_MARKER;
import static com.github.akurilov.commons.io.el.ExpressionInput.INIT_MARKER;
import static com.github.akurilov.commons.io.el.ExpressionInput.SYNC_MARKER;

import com.dell.spt.base.config.el.CompositeExpressionInputBuilder;
import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException.Category;
import com.dell.spt.base.item.ItemType;
import com.dell.spt.base.item.io.IntegrityManifestItemInput;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.confuse.Config;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/** Resolves the static S3 target buckets of one load-step context. */
final class TargetBuckets {

	static final String OUTPUT_PATH = "item-output-path";
	static final String INPUT_PATH = "item-input-path";
	static final String INPUT_FILE = "item-input-file";

	/**
	 * A bucket the step uses. {@code written} when the step writes new objects into it;
	 * {@code creatable} when, in addition, its policy permits creating it.
	 */
	record Target(String bucket, boolean written, boolean creatable, Credential credential) {

		Target mergedWith(final Target other) {
			return new Target(bucket, written || other.written, creatable || other.creatable, credential);
		}
	}

	private TargetBuckets() {}

	/** True when the context's operations write new objects to its output path. */
	static boolean writesOutputPath(final Config ctxConfig) {
		final var opType = OpType.valueOf(ctxConfig.stringVal("load-op-type").toUpperCase(Locale.ROOT));
		final var itemType = ItemType.valueOf(ctxConfig.stringVal("item-type").toUpperCase(Locale.ROOT));
		return OpType.CREATE.equals(opType) && ItemType.DATA.equals(itemType);
	}

	/**
	 * Returns the output path with initialization expressions in its bucket segment evaluated
	 * once, or the path unchanged when its bucket segment is literal.
	 *
	 * @throws RunTerminalException when the bucket segment holds a per-operation expression
	 */
	static String resolveOutputPath(final String path) {
		if (path == null || path.isEmpty()) {
			return path;
		}
		final var bucket = bucketSegment(path);
		if (bucket.contains(SYNC_MARKER) || bucket.contains(ASYNC_MARKER)) {
			throw dynamicBucket("item.output.path", path);
		}
		if (!bucket.contains(INIT_MARKER)) {
			return path;
		}
		final String resolved;
		try (final var input = CompositeExpressionInputBuilder.newInstance().expression(bucket).build()) {
			resolved = input.get();
		} catch (final Exception e) {
			throw new RunTerminalException(
							Category.CONFIGURATION,
							"failed to evaluate the initialization expression in the bucket segment of item.output.path",
							e);
		}
		if (resolved == null || resolved.isEmpty() || resolved.indexOf('/') >= 0) {
			throw new RunTerminalException(
							Category.CONFIGURATION,
							"the initialization expression in the bucket segment of item.output.path must yield one bucket name",
							null);
		}
		return "/" + resolved + path.substring(leadingSlash(path) + bucket.length());
	}

	/**
	 * Lists the context's distinct target buckets in first-use order: the output path's bucket
	 * when the context writes there, the input path's bucket and the buckets named by its item
	 * input file.
	 *
	 * @throws RunTerminalException when a bucket segment is not a literal name
	 */
	static List<Target> targets(final Config ctxConfig, final boolean provision) throws IOException {
		final var targets = new LinkedHashMap<String, Target>();
		final var credentials = credentialsByPath(ctxConfig);
		final var writes = writesOutputPath(ctxConfig);
		if (writes) {
			final var outputPath = ctxConfig.stringVal(OUTPUT_PATH);
			if (outputPath != null && !outputPath.isEmpty()) {
				add(targets, literalBucket("item.output.path", outputPath), true, provision, credentials, outputPath);
			}
		}
		final var inputPath = ctxConfig.stringVal(INPUT_PATH);
		if (inputPath != null && !inputPath.isEmpty()) {
			add(targets, literalBucket("item.input.path", inputPath), false, false, credentials, inputPath);
		}
		final var inputFile = ctxConfig.stringVal(INPUT_FILE);
		if (inputFile != null && !inputFile.isEmpty()) {
			for (final var bucket : itemFileBuckets(Path.of(inputFile))) {
				add(targets, bucket, false, false, credentials, "/" + bucket);
			}
		}
		return List.copyOf(targets.values());
	}

	private static void add(
					final Map<String, Target> targets,
					final String bucket,
					final boolean written,
					final boolean creatable,
					final Map<String, Credential> credentials,
					final String path) {
		if (bucket.isEmpty()) {
			return;
		}
		final var target = new Target(bucket, written, written && creatable, credential(credentials, path, bucket));
		targets.merge(bucket, target, Target::mergedWith);
	}

	/** Returns the bucket segment of a path that must name a literal bucket. */
	static String literalBucket(final String setting, final String path) {
		final var bucket = bucketSegment(path);
		if (bucket.contains(SYNC_MARKER) || bucket.contains(ASYNC_MARKER) || bucket.contains(INIT_MARKER)) {
			throw dynamicBucket(setting, path);
		}
		return bucket;
	}

	/**
	 * Returns the first segment of a path, ignoring a leading slash. A slash inside an expression's
	 * braces does not end the segment.
	 */
	static String bucketSegment(final String path) {
		final int start = leadingSlash(path);
		int depth = 0;
		for (int i = start; i < path.length(); i++) {
			final char c = path.charAt(i);
			if (c == '{') {
				depth++;
			} else if (c == '}' && depth > 0) {
				depth--;
			} else if (c == '/' && depth == 0) {
				return path.substring(start, i);
			}
		}
		return path.substring(start);
	}

	private static int leadingSlash(final String path) {
		return path.startsWith("/") ? 1 : 0;
	}

	private static RunTerminalException dynamicBucket(final String setting, final String path) {
		return new RunTerminalException(
						Category.CONFIGURATION,
						"the bucket segment of " + setting + " must be static: per-operation expressions (${...}, #{...}) "
										+ "cannot name a bucket; keep them after the bucket, as in /bucket/${...}, or split the "
										+ "workload into steps with one static bucket each",
						null);
	}

	/**
	 * Returns the distinct buckets named by an item input file: the {@code bucket} column of an
	 * integrity manifest, or the first path segment of absolute item names in an items CSV. Other
	 * formats name no static bucket.
	 */
	static List<String> itemFileBuckets(final Path file) throws IOException {
		if (!file.toString().endsWith(".csv") || !Files.isRegularFile(file)) {
			return List.of();
		}
		final var buckets = new LinkedHashMap<String, Boolean>();
		if (IntegrityManifestItemInput.hasCanonicalHeader(file)) {
			try (final Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8);
							final CSVParser parser = CSVFormat.RFC4180.builder()
											.setHeader(IntegrityManifestItemInput.HEADER.toArray(String[]::new))
											.setSkipHeaderRecord(true)
											.get()
											.parse(reader)) {
				for (final CSVRecord record : parser) {
					final var bucket = record.get(0);
					if (!bucket.isEmpty()) {
						buckets.putIfAbsent(bucket, Boolean.TRUE);
					}
				}
			}
		} else {
			try (final BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
				String line;
				while ((line = reader.readLine()) != null) {
					if (!line.startsWith("/")) {
						continue;
					}
					final int comma = line.indexOf(',');
					final var name = comma < 0 ? line : line.substring(0, comma);
					final int slash = name.indexOf('/', 1);
					if (slash > 1) {
						buckets.putIfAbsent(name.substring(1, slash), Boolean.TRUE);
					}
				}
			}
		}
		return List.copyOf(buckets.keySet());
	}

	/** Loads {@code storage.auth.file} rows of {@code path,uid,secret}, keyed by path. */
	private static Map<String, Credential> credentialsByPath(final Config ctxConfig) throws IOException {
		final var authFile = ctxConfig.stringVal("storage-auth-file");
		if (authFile == null || authFile.isEmpty()) {
			return Map.of();
		}
		final var result = new HashMap<String, Credential>();
		try (final BufferedReader reader = Files.newBufferedReader(Path.of(authFile), StandardCharsets.UTF_8)) {
			String line;
			while ((line = reader.readLine()) != null) {
				final var parts = line.split(",", 3);
				if (parts.length == 3) {
					result.put(parts[0], Credential.getInstance(parts[1], parts[2]));
				}
			}
		}
		return result;
	}

	private static Credential credential(
					final Map<String, Credential> credentials, final String path, final String bucket) {
		final var exact = credentials.get(path);
		return exact != null ? exact : credentials.get("/" + bucket);
	}
}
