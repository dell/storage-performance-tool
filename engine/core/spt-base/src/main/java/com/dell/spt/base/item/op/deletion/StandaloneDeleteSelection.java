package com.dell.spt.base.item.op.deletion;

import static com.dell.spt.base.metrics.MetricsConstants.DELETE_SELECTION_ORDER_CANONICAL;

import com.dell.spt.base.item.io.IntegrityManifestItemInput;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.metrics.snapshot.DeleteMetricsSnapshot;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Immutable standalone DELETE selection identity derived from the exact canonical manifest. */
public final class StandaloneDeleteSelection {
	private final long selected;
	private final long selectedCurrentKey;
	private final long selectedExactVersion;
	private final List<String> selectedBuckets;

	private StandaloneDeleteSelection(
					final long selected,
					final long selectedCurrentKey,
					final long selectedExactVersion,
					final List<String> selectedBuckets) {
		this.selected = selected;
		this.selectedCurrentKey = selectedCurrentKey;
		this.selectedExactVersion = selectedExactVersion;
		this.selectedBuckets = Collections.unmodifiableList(new ArrayList<>(selectedBuckets));
	}

	/** Parses and freezes the selection identity before any DELETE request is dispatched. */
	public static StandaloneDeleteSelection fromManifest(final String manifestPath) throws IOException {
		long selected = 0;
		long currentKey = 0;
		long exactVersion = 0;
		final Map<String, Long> buckets = new TreeMap<>();
		try (final var input = new IntegrityManifestItemInput(Path.of(manifestPath))) {
			for (var item = input.get(); item != null; item = input.get()) {
				selected = Math.addExact(selected, 1);
				if (item.versionId() == null || item.versionId().isEmpty()) {
					currentKey = Math.addExact(currentKey, 1);
				} else {
					exactVersion = Math.addExact(exactVersion, 1);
				}
				final String bucket = buckets.containsKey(item.bucket())
								|| buckets.size() < DeleteMetricsSnapshot.MAX_BUCKET_METRICS
												? item.bucket()
												: DeleteMetricsSnapshot.OVERFLOW_BUCKET;
				buckets.merge(bucket, 1L, Math::addExact);
			}
		}
		final List<String> encodedBuckets = buckets.entrySet().stream()
						.map(entry -> entry.getKey() + "=" + entry.getValue())
						.toList();
		return new StandaloneDeleteSelection(selected, currentKey, exactVersion, encodedBuckets);
	}

	/**
	 * Freezes direct-engine selection metadata before the generator can dispatch any request.
	 * Existing complete metadata supplied by the scenario or distributed slicer is preserved.
	 */
	public static void ensureFrozen(final Config loadConfig, final Config itemConfig) {
		final StandaloneDeleteConfig configured = StandaloneDeleteConfig.from(loadConfig);
		if (!configured.enabled() || configured.frozenSelectionAvailable()) {
			return;
		}
		if (itemConfig == null) {
			throw new IllegalConfigurationException(
							"Standalone DELETE requires item input configuration to freeze selection metrics");
		}
		final String manifestPath;
		try {
			manifestPath = itemConfig.stringVal("input-file");
		} catch (final RuntimeException failure) {
			throw new IllegalConfigurationException(
							"Standalone DELETE requires a canonical item-input-file to freeze selection metrics",
							failure);
		}
		if (manifestPath == null || manifestPath.isBlank()) {
			throw new IllegalConfigurationException(
							"Standalone DELETE requires a canonical item-input-file to freeze selection metrics");
		}
		try {
			fromManifest(manifestPath).applyTo(loadConfig);
		} catch (final IOException | RuntimeException failure) {
			throw new IllegalConfigurationException(
							"Failed to freeze standalone DELETE selection metrics from " + manifestPath,
							failure);
		}
	}

	private void applyTo(final Config loadConfig) {
		loadConfig.val("op-delete-selectionOrder", DELETE_SELECTION_ORDER_CANONICAL);
		loadConfig.val("op-delete-selected", selected);
		loadConfig.val("op-delete-selectedCurrentKey", selectedCurrentKey);
		loadConfig.val("op-delete-selectedExactVersion", selectedExactVersion);
		loadConfig.val("op-delete-selectedBuckets", selectedBuckets);
	}

	/** Returns the exact number of manifest rows selected for deletion. */
	public long selected() {
		return selected;
	}

	/** Returns the number of selected rows targeting the current object version. */
	public long selectedCurrentKey() {
		return selectedCurrentKey;
	}

	/** Returns the number of selected rows targeting an explicit object version. */
	public long selectedExactVersion() {
		return selectedExactVersion;
	}

	/** Returns the immutable, bounded bucket-count identity encoded as {@code bucket=count}. */
	public List<String> selectedBuckets() {
		return selectedBuckets;
	}
}
