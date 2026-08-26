package com.dell.spt.base.buildinfo;

/** Immutable provenance for the engine distribution executing this process. */
public record EngineBuildInfo(
				int schemaVersion,
				String product,
				String version,
				String revision,
				String buildTime,
				boolean development,
				Boolean sourceDirty) {
}
