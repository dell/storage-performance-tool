package com.dell.spt.base.buildinfo;

import static com.dell.spt.base.Constants.KEY_HOME_DIR;

import com.dell.spt.base.logging.Loggers;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.logging.log4j.ThreadContext;

/** Atomically publishes the local Engine Build Record without making workload success depend on it. */
public final class EngineBuildInfoPublisher {

	public static final String FILE_NAME = "engine.build.json";
	private static final String TEMPORARY_FILE_PREFIX = "." + FILE_NAME + ".";

	private final String json;
	private final Consumer<String> warningSink;
	private final AtomicBoolean warningEmitted = new AtomicBoolean();

	public EngineBuildInfoPublisher(final EngineBuildInfo buildInfo, final Consumer<String> warningSink) {
		json = EngineBuildInfoJson.serialize(Objects.requireNonNull(buildInfo));
		this.warningSink = Objects.requireNonNull(warningSink);
	}

	public void publish(final Path logHome, final String stepId) {
		Path temporaryFile = null;
		try {
			final Path stepDirectory = Objects.requireNonNull(logHome).resolve("log").resolve(stepId);
			Files.createDirectories(stepDirectory);
			temporaryFile = Files.createTempFile(stepDirectory, TEMPORARY_FILE_PREFIX, ".tmp");
			Files.writeString(temporaryFile, json, StandardCharsets.UTF_8);
			Files.move(
							temporaryFile,
							stepDirectory.resolve(FILE_NAME),
							StandardCopyOption.ATOMIC_MOVE,
							StandardCopyOption.REPLACE_EXISTING);
			temporaryFile = null;
		} catch (final IOException | RuntimeException e) {
			if (warningEmitted.compareAndSet(false, true)) {
				warningSink.accept("Failed to write " + FILE_NAME + "; continuing without a local Engine Build Record");
			}
		} finally {
			if (temporaryFile != null) {
				try {
					Files.deleteIfExists(temporaryFile);
				} catch (final IOException | RuntimeException ignored) {
					// The primary warning above is intentionally one-shot for this nonfatal artifact.
				}
			}
		}
	}

	public void publishForStep(final String stepId) {
		final String logHome = ThreadContext.get(KEY_HOME_DIR);
		publish(logHome == null ? null : Path.of(logHome), stepId);
	}

	public static EngineBuildInfoPublisher global() {
		return GlobalHolder.INSTANCE;
	}

	private static final class GlobalHolder {

		private static final EngineBuildInfoPublisher INSTANCE = new EngineBuildInfoPublisher(
						EngineBuildInfoProvider.global().snapshot(), Loggers.ERR::warn);
	}
}
