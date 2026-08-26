package com.dell.spt.base.buildinfo;

import com.dell.spt.base.logging.Loggers;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** Loads and retains the single Engine Build Identity snapshot for this process. */
public final class EngineBuildInfoProvider {

	public static final String RESOURCE_PATH = "META-INF/spt-build-info.properties";
	public static final int SCHEMA_VERSION = 1;
	public static final String PRODUCT = "spt-engine";
	public static final String UNKNOWN = "unknown";

	private static final Set<String> RESOURCE_FIELDS = Set.of(
					"schema_version", "product", "version", "revision", "build_time", "development", "source_dirty");

	private final EngineBuildInfo snapshot;
	private final Consumer<String> warningSink;
	private final AtomicBoolean overrideWarningEmitted = new AtomicBoolean();

	public EngineBuildInfoProvider(final EngineBuildInfoSource source, final Consumer<String> warningSink) {
		this.warningSink = warningSink;
		final EngineBuildInfo loaded;
		try (InputStream input = source.openBuildInfoResource()) {
			if (input == null) {
				warningSink.accept("Engine build information resource is unavailable; using development fallback identity");
				loaded = fallback(source.implementationVersion());
			} else {
				loaded = parse(input);
			}
		} catch (final IOException | IllegalArgumentException e) {
			warningSink.accept("Engine build information resource is malformed; using development fallback identity");
			this.snapshot = fallback(source.implementationVersion());
			return;
		}
		this.snapshot = loaded;
	}

	public static EngineBuildInfoProvider global() {
		return GlobalHolder.INSTANCE;
	}

	public EngineBuildInfo snapshot() {
		return snapshot;
	}

	public void projectVersion(final Config config, final boolean userOverrideAttempted) {
		if (userOverrideAttempted && overrideWarningEmitted.compareAndSet(false, true)) {
			warningSink.accept("Configured run.version override is ignored; using immutable Engine Build Identity");
		}
		config.val("run-version", snapshot.version());
	}

	private static EngineBuildInfo parse(final InputStream input) throws IOException {
		final var properties = new Properties();
		properties.load(input);
		if (!properties.stringPropertyNames().equals(RESOURCE_FIELDS)) {
			throw new IllegalArgumentException("Unexpected engine build information fields");
		}

		final int schemaVersion;
		try {
			schemaVersion = Integer.parseInt(required(properties, "schema_version"));
		} catch (final NumberFormatException e) {
			throw new IllegalArgumentException("Invalid schema version", e);
		}
		if (schemaVersion != SCHEMA_VERSION) {
			throw new IllegalArgumentException("Unsupported engine build information schema");
		}
		final var product = required(properties, "product");
		if (!PRODUCT.equals(product)) {
			throw new IllegalArgumentException("Invalid engine product");
		}
		final var version = required(properties, "version");
		final var revision = required(properties, "revision");
		if (!UNKNOWN.equals(revision) && !revision.matches("(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})")) {
			throw new IllegalArgumentException("Invalid engine build revision");
		}
		final var buildTime = required(properties, "build_time");
		if (!UNKNOWN.equals(buildTime)) {
			try {
				Instant.parse(buildTime);
			} catch (final DateTimeParseException e) {
				throw new IllegalArgumentException("Invalid engine build time", e);
			}
		}
		final var development = strictBoolean(properties, "development");
		final var dirtyText = required(properties, "source_dirty");
		final Boolean sourceDirty = UNKNOWN.equals(dirtyText) ? null : strictBoolean(properties, "source_dirty");
		if (!development
						&& (UNKNOWN.equals(revision) || UNKNOWN.equals(buildTime) || sourceDirty == null || sourceDirty)) {
			throw new IllegalArgumentException("Release engine build information is incomplete");
		}
		return new EngineBuildInfo(schemaVersion, product, version, revision, buildTime, development, sourceDirty);
	}

	private static String required(final Properties properties, final String key) {
		final var value = properties.getProperty(key);
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException("Missing engine build information field: " + key);
		}
		return value;
	}

	private static boolean strictBoolean(final Properties properties, final String key) {
		final var value = required(properties, key);
		if ("true".equals(value)) {
			return true;
		}
		if ("false".equals(value)) {
			return false;
		}
		throw new IllegalArgumentException("Invalid Boolean engine build information field: " + key);
	}

	private static EngineBuildInfo fallback(final String implementationVersion) {
		final var version = implementationVersion == null || implementationVersion.isBlank()
						? UNKNOWN
						: implementationVersion;
		return new EngineBuildInfo(SCHEMA_VERSION, PRODUCT, version, UNKNOWN, UNKNOWN, true, null);
	}

	private static final class GlobalHolder {

		private static final EngineBuildInfoProvider INSTANCE = new EngineBuildInfoProvider(
						new ClasspathSource(), Loggers.ERR::warn);
	}

	private static final class ClasspathSource implements EngineBuildInfoSource {

		@Override
		public InputStream openBuildInfoResource() {
			return EngineBuildInfoProvider.class.getClassLoader().getResourceAsStream(RESOURCE_PATH);
		}

		@Override
		public String implementationVersion() {
			return EngineBuildInfoProvider.class.getPackage().getImplementationVersion();
		}
	}
}
