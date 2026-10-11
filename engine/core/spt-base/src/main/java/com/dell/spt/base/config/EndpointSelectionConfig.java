package com.dell.spt.base.config;

import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.exceptions.InvalidValuePathException;
import java.util.NoSuchElementException;

/** Construction-time access to {@code storage.net.endpoint.selection}. */
public final class EndpointSelectionConfig {

	/** Path of the selection mode relative to the storage configuration. */
	public static final String SELECTION_PATH = "net-endpoint-selection";

	/** Shipped mode: the driver's normal connection pool chooses destinations. */
	public static final String DEFAULT_SELECTION = "default";

	private EndpointSelectionConfig() {}

	/**
	 * Returns the configured mode. A missing path or value means {@link #DEFAULT_SELECTION}, so
	 * configurations that predate the setting keep their behavior.
	 */
	public static String selection(final Config storageConfig) {
		try {
			final var value = storageConfig.stringVal(SELECTION_PATH);
			return value == null || value.isBlank() ? DEFAULT_SELECTION : value.strip();
		} catch (final InvalidValuePathException | NoSuchElementException e) {
			return DEFAULT_SELECTION;
		}
	}

	public static boolean isDefault(final Config storageConfig) {
		return DEFAULT_SELECTION.equals(selection(storageConfig));
	}
}
