package com.dell.spt.base.item.io;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

import com.dell.spt.base.integrity.CrashDurableFilePublisher;
import com.dell.spt.base.integrity.IntegrityCsvFormat;
import com.dell.spt.base.integrity.IntegrityManifestCompletion;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.list.ListOperation;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.apache.commons.csv.CSVPrinter;

/** Writes successful operation results in the canonical integrity-manifest format. */
public final class IntegrityOperationManifestOutput<O extends Operation<? extends Item>>
				implements Output<O> {

	private final Path manifestPath;
	private final String configuredBucket;
	private final OpType opType;
	private final String requestedListPrefix;
	private final CSVPrinter printer;
	private long emittedRecordCount;
	private long excludedDeleteMarkerCount;
	private boolean failed;
	private boolean closed;

	public IntegrityOperationManifestOutput(
					final Path manifestPath, final String configuredPath, final OpType opType) throws IOException {
		this(manifestPath, configuredPath, opType, null);
	}

	public IntegrityOperationManifestOutput(
					final Path manifestPath,
					final String configuredPath,
					final OpType opType,
					final String requestedListPrefix) throws IOException {
		this.manifestPath = manifestPath;
		this.configuredBucket = bucketFromPath(configuredPath);
		this.opType = opType;
		this.requestedListPrefix = requestedListPrefix == null
						? null
						: canonicalListPrefix(requestedListPrefix);
		final Path parent = manifestPath.toAbsolutePath().getParent();
		if (parent != null) {
			Files.createDirectories(parent);
		}
		printer = new CSVPrinter(
						Files.newBufferedWriter(
										manifestPath,
										StandardCharsets.UTF_8,
										StandardOpenOption.CREATE_NEW,
										StandardOpenOption.WRITE),
						IntegrityCsvFormat.RFC4180_LF);
		printer.printRecord(IntegrityManifestItemInput.HEADER);
		printer.flush();
	}

	@Override
	public synchronized boolean put(final O result) {
		if (result == null) {
			close();
			return true;
		}
		try {
			if (result instanceof ListOperation<?> listOperation) {
				final String requestedPrefix = requestedListPrefix == null
								? listPrefix(listOperation)
								: requestedListPrefix;
				for (final var listedObject : listOperation.listedObjects()) {
					requireWithinListPrefix(listedObject.key(), requestedPrefix);
				}
				for (final var listedObject : listOperation.listedObjects()) {
					printer.printRecord(configuredBucket, listedObject.key(), listedObject.size(),
									listedObject.versionId() == null ? "" : listedObject.versionId());
					emittedRecordCount++;
				}
				excludedDeleteMarkerCount = Math.addExact(excludedDeleteMarkerCount, listOperation.deleteMarkersListed());
				return true;
			}
			final Item item = result.item();
			if (!(item instanceof DataItem dataItem)) {
				throw new IOException("integrity manifests require data items");
			}
			final String bucket;
			final String key;
			if (item instanceof IntegrityManifestDataItem manifestItem) {
				bucket = manifestItem.bucket();
				key = manifestItem.name();
			} else {
				bucket = configuredBucket;
				key = keyFromResult(item.name(), bucket);
			}
			final String version = OpType.CREATE.equals(opType)
							? result.returnedVersionId()
							: result.requestedVersionId();
			printer.printRecord(bucket, key, dataItem.size(), version == null ? "" : version);
			return true;
		} catch (final IOException e) {
			failed = true;
			throwUnchecked(e);
			return false;
		}
	}

	@Override
	public synchronized int put(final List<O> results, final int from, final int to) {
		int index = from;
		while (index < to && put(results.get(index))) {
			index++;
		}
		return index - from;
	}

	@Override
	public int put(final List<O> results) {
		return put(results, 0, results.size());
	}

	@Override
	public Input<O> getInput() {
		throw new UnsupportedOperationException();
	}

	@Override
	public synchronized void close() {
		if (closed) {
			return;
		}
		closed = true;
		try {
			printer.close(true);
			if (failed) {
				Files.deleteIfExists(IntegrityManifestCompletion.emissionCountPath(manifestPath));
				Files.deleteIfExists(IntegrityManifestCompletion.deleteMarkerCountPath(manifestPath));
				Files.deleteIfExists(manifestPath);
				return;
			}
			CrashDurableFilePublisher.syncExisting(manifestPath);
			if (OpType.LIST.equals(opType)) {
				publishCount(IntegrityManifestCompletion.emissionCountPath(manifestPath),
								emittedRecordCount, "emission");
				publishCount(IntegrityManifestCompletion.deleteMarkerCountPath(manifestPath),
								excludedDeleteMarkerCount, "delete-marker");
			}
		} catch (final IOException e) {
			throwUnchecked(e);
		}
	}

	private void publishCount(final Path countPath, final long count, final String description) throws IOException {
		if (Files.exists(countPath)) {
			throw new IOException("refusing to replace existing " + description + " count " + countPath);
		}
		final Path parent = countPath.toAbsolutePath().getParent();
		final Path staging = Files.createTempFile(parent, "." + countPath.getFileName(), ".staging");
		boolean committed = false;
		try {
			Files.writeString(
							staging,
							Long.toString(count) + "\n",
							StandardCharsets.US_ASCII,
							StandardOpenOption.TRUNCATE_EXISTING);
			IntegrityManifestCompletion.atomicMove(staging, countPath);
			committed = true;
		} finally {
			if (!committed) {
				Files.deleteIfExists(staging);
			}
		}
	}

	static String bucketFromPath(final String path) {
		if (path == null) {
			return "";
		}
		final String normalized = path.replace('\\', '/');
		int start = 0;
		while (start < normalized.length() && normalized.charAt(start) == '/') {
			start++;
		}
		final int slash = normalized.indexOf('/', start);
		return slash < 0 ? normalized.substring(start) : normalized.substring(start, slash);
	}

	static String keyFromResult(final String itemName, final String bucket) throws IOException {
		if (itemName == null || itemName.isEmpty()) {
			throw new IOException("integrity manifest object key must not be empty");
		}
		String key = itemName.startsWith("/") ? itemName.substring(1) : itemName;
		final String bucketPrefix = bucket + "/";
		if (key.startsWith(bucketPrefix)) {
			key = key.substring(bucketPrefix.length());
		}
		if (bucket.isEmpty() || key.isEmpty()) {
			throw new IOException("integrity manifest bucket and key must not be empty");
		}
		return key;
	}

	private static String listPrefix(final ListOperation<?> listOperation) throws IOException {
		if (listOperation.item() == null) {
			throw new IOException("integrity LIST result is missing its requested prefix item");
		}
		final String prefix = listOperation.item().name();
		return canonicalListPrefix(prefix);
	}

	private static String canonicalListPrefix(final String prefix) {
		if (prefix == null || prefix.isEmpty() || "/".equals(prefix)) {
			return "";
		}
		return prefix.startsWith("/") ? prefix.substring(1) : prefix;
	}

	private static void requireWithinListPrefix(final String key, final String requestedPrefix)
					throws IOException {
		if (key == null || key.isEmpty()) {
			throw new IOException("integrity LIST response contains an empty object key");
		}
		if (!key.startsWith(requestedPrefix)) {
			throw new IOException(
							"integrity LIST response key is outside requested prefix \""
											+ requestedPrefix + "\": " + key);
		}
	}
}
