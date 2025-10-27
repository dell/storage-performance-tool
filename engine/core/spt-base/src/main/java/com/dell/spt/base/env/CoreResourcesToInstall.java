package com.dell.spt.base.env;

import static com.dell.spt.base.Constants.APP_NAME;
import static com.dell.spt.base.Constants.USER_HOME;
import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;
import static com.dell.spt.base.config.CliArgUtil.ARG_PATH_SEP;

import com.dell.spt.base.config.BundledDefaultsProvider;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

public final class CoreResourcesToInstall extends InstallableJarResources {

	public static final String RESOURCES_FILE_NAME = "/install_resources.txt";
	private final Path appHomePath;

	public CoreResourcesToInstall() {
		final Config bundledDefaults;
		try {
			final var schema = SchemaProvider.resolveAndReduce(APP_NAME, Thread.currentThread().getContextClassLoader());
			bundledDefaults = new BundledDefaultsProvider().config(ARG_PATH_SEP, schema);
		} catch (final Exception e) {
			throwUncheckedIfInterrupted(e);
			throw new IllegalStateException("Failed to load the bundled default config from the resources", e);
		}
		final var appVersion = bundledDefaults.stringVal("run-version");
		final var msg = " " + APP_NAME + " v " + appVersion + " ";
		final var pad = StringUtils.repeat("#", (120 - msg.length()) / 2);
		System.out.println(pad + msg + pad);
		appHomePath = Paths.get(USER_HOME, "." + APP_NAME, appVersion);
	}

	public final Path appHomePath() {
		return appHomePath;
	}

	@Override
	protected final List<String> resourceFilesToInstall() {
		try (
						final var in = Objects.requireNonNull(
										getClass().getResourceAsStream(RESOURCES_FILE_NAME),
										"Missing install resources manifest: " + RESOURCES_FILE_NAME);
						final var reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
			return reader.lines().collect(Collectors.toList());
		} catch (final IOException e) {
			throw new IllegalStateException("Failed to load the resources list");
		}
	}
}
