package com.dell.spt.base.config;

import com.github.akurilov.confuse.Config;
import java.util.List;

/** Resolves optional run identity consistently for load steps and API metadata. */
public final class RunIdentityConfig {
	private RunIdentityConfig() {}

	public static String clusterId(final Config config) {
		for (final String path : List.of("run-cluster-id", "run-cluster")) {
			try {
				final String value = config.stringVal(path);
				if (value != null && !value.isBlank()) {
					return value;
				}
			} catch (final RuntimeException ignored) {
				// Older configs may omit a leaf or use a scalar intermediate path.
			}
		}
		return null;
	}
}
