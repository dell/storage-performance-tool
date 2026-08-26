package com.dell.spt.gradle;

import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.Collections;
import java.util.Map;

/** Normalized build inputs shared by generated metadata and JAR manifests. */
public record EngineBuildMetadata(
				String version, String revision, String buildTime, boolean development, Boolean sourceDirty) {

	public static final String UNKNOWN = "unknown";

	public static EngineBuildMetadata resolve(
				final String version,
				final Map<String, String> properties,
				final Map<String, String> environment,
				final GitProbe git,
				final Clock clock) {
		final var release = parseBoolean(first(properties.get("sptBuildRelease"), environment.get("SPT_BUILD_RELEASE")), false);
		final var revision = first(
					properties.get("sptBuildRevision"), environment.get("SPT_BUILD_REVISION"), git.revision(), UNKNOWN);
		final var buildTimeInput = first(properties.get("sptBuildTime"), environment.get("SPT_BUILD_TIME"));
		final var sourceDateEpoch = environment.get("SOURCE_DATE_EPOCH");
		final String buildTime;
		if (buildTimeInput != null) {
			buildTime = normalizeTimestamp(buildTimeInput);
		} else if (sourceDateEpoch != null && !sourceDateEpoch.isBlank()) {
			buildTime = timestampFromEpoch(sourceDateEpoch);
		} else {
			buildTime = release ? UNKNOWN : clock.instant().toString();
		}
		final var dirtyInput = first(properties.get("sptBuildSourceDirty"), environment.get("SPT_BUILD_SOURCE_DIRTY"));
		final Boolean sourceDirty = dirtyInput == null
					? git.dirty()
					: UNKNOWN.equalsIgnoreCase(dirtyInput) ? null : parseBoolean(dirtyInput, false);

		final var metadata = new EngineBuildMetadata(version, revision, buildTime, !release, sourceDirty);
		metadata.validate();
		return metadata;
	}

	public Map<String, String> resourceValues() {
		final var values = new LinkedHashMap<String, String>();
		values.put("schema_version", "1");
		values.put("product", "spt-engine");
		values.put("version", version);
		values.put("revision", revision);
		values.put("build_time", buildTime);
		values.put("development", Boolean.toString(development));
		values.put("source_dirty", sourceDirty == null ? UNKNOWN : sourceDirty.toString());
		return Collections.unmodifiableMap(values);
	}

	public Map<String, String> manifestAttributes() {
		return Map.of(
					"Implementation-Title", "spt-engine",
					"Implementation-Version", version,
					"Spt-Source-Revision", revision,
					"Spt-Build-Time", buildTime,
					"Spt-Development", Boolean.toString(development),
					"Spt-Source-Dirty", sourceDirty == null ? UNKNOWN : sourceDirty.toString());
	}

	private void validate() {
		if (version == null || version.isBlank()) {
			throw new IllegalArgumentException("Engine build version is unavailable");
		}
		if (!UNKNOWN.equals(revision) && !revision.matches("(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})")) {
			throw new IllegalArgumentException("Engine build revision is not a full source object ID");
		}
		if (!development) {
			if (UNKNOWN.equals(revision)) {
				throw new IllegalArgumentException("Release engine build revision is unavailable");
			}
			if (UNKNOWN.equals(buildTime)) {
				throw new IllegalArgumentException("Release engine build time is unavailable");
			}
			if (sourceDirty == null) {
				throw new IllegalArgumentException("Release engine dirty-state evidence is unavailable");
			}
			if (sourceDirty) {
				throw new IllegalArgumentException("Release engine source is dirty");
			}
		}
	}

	private static String normalizeTimestamp(final String value) {
		if (UNKNOWN.equalsIgnoreCase(value)) {
			return UNKNOWN;
		}
		try {
			return OffsetDateTime.parse(value).toInstant().toString();
		} catch (final DateTimeParseException e) {
			try {
				return Instant.parse(value).toString();
			} catch (final DateTimeParseException nested) {
				throw new IllegalArgumentException("Invalid engine build time: " + value, nested);
			}
		}
	}

	private static String timestampFromEpoch(final String value) {
		try {
			return Instant.ofEpochSecond(Long.parseLong(value)).toString();
		} catch (final NumberFormatException e) {
			throw new IllegalArgumentException("Invalid SOURCE_DATE_EPOCH: " + value, e);
		}
	}

	private static Boolean parseBoolean(final String value, final boolean defaultValue) {
		if (value == null) {
			return defaultValue;
		}
		if ("true".equalsIgnoreCase(value)) {
			return true;
		}
		if ("false".equalsIgnoreCase(value)) {
			return false;
		}
		throw new IllegalArgumentException("Invalid Boolean build input: " + value);
	}

	private static String first(final String... values) {
		for (final var value : values) {
			if (value != null && !value.isBlank()) {
				return value;
			}
		}
		return null;
	}

	public interface GitProbe {

		String revision();

		Boolean dirty();
	}
}
