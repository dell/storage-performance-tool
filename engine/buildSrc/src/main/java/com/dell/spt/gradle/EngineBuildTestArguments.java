package com.dell.spt.gradle;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import org.gradle.api.file.RegularFile;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.process.CommandLineArgumentProvider;

/** Resolves expected values after generation, with the resource tracked as a test input. */
public final class EngineBuildTestArguments implements CommandLineArgumentProvider {
	private final Provider<RegularFile> resource;

	public EngineBuildTestArguments(final Provider<RegularFile> resource) {
		this.resource = resource;
	}

	@InputFile
	@PathSensitive(PathSensitivity.NONE)
	public Provider<RegularFile> getResource() {
		return resource;
	}

	@Override
	public Iterable<String> asArguments() {
		try {
			final var metadata = EngineBuildMetadata.read(resource.get().getAsFile());
			return List.of(
					"-Dspt.test.expected-build-revision=" + metadata.revision(),
					"-Dspt.test.expected-build-time=" + metadata.buildTime(),
					"-Dspt.test.expected-build-development=" + metadata.development(),
					"-Dspt.test.expected-build-source-dirty=" + (metadata.sourceDirty() == null ? "unknown" : metadata.sourceDirty()));
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
