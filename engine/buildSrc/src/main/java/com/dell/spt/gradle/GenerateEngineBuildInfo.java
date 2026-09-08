package com.dell.spt.gradle;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;

/** Captures one development timestamp per changed source/input snapshot. */
@CacheableTask
public abstract class GenerateEngineBuildInfo extends DefaultTask {
	@Input
	public abstract MapProperty<String, String> getMetadataValues();

	@Input
	public abstract Property<Boolean> getUseCurrentTime();

	@InputFiles
	@PathSensitive(PathSensitivity.RELATIVE)
	public abstract ConfigurableFileCollection getSources();

	@OutputFile
	public abstract RegularFileProperty getOutputFile();

	@TaskAction
	public void generate() throws IOException {
		final var values = new LinkedHashMap<>(getMetadataValues().get());
		if (getUseCurrentTime().get()) {
			values.put("build_time", Instant.now().toString());
		}
		final var output = getOutputFile().get().getAsFile().toPath();
		Files.createDirectories(output.getParent());
		Files.writeString(output, values.entrySet().stream()
				.map(entry -> entry.getKey() + "=" + entry.getValue())
				.collect(Collectors.joining("\n", "", "\n")), StandardCharsets.UTF_8);
	}
}
