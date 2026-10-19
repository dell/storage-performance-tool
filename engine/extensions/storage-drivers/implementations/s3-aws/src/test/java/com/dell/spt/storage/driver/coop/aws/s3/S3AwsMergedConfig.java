package com.dell.spt.storage.driver.coop.aws.s3;

import com.dell.spt.base.Constants;
import com.dell.spt.base.config.BundledDefaultsProvider;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.env.Extension;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * The configuration the engine builds for an s3-aws run: the schemas of every extension and the
 * shipped defaults of the engine and of the extensions, as {@code Main} merges them.
 */
final class S3AwsMergedConfig {

	private static final String EXTENSION_ID = "s3-aws";
	private static final String DEFAULTS_FILE_NAME = "defaults-storage-s3-aws.yaml";

	private S3AwsMergedConfig() {}

	/**
	 * Builds the merged configuration for a local endpoint.
	 *
	 * @param home an empty directory to install the shipped extension defaults into, as the engine
	 *     installs them into its home directory
	 * @param port the port of the S3 endpoint on the loopback address
	 * @param overrides values set as the CLI sets them, by dashed path such as {@code storage-crt-socketTimeoutMs}
	 */
	static Config create(final Path home, final int port, final Map<String, Object> overrides)
					throws Exception {
		final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		final List<Extension> extensions = Extension.load(classLoader);
		final List<Map<String, Object>> schemas = extensions.stream()
						.map(Extension::schemaProvider)
						.filter(Objects::nonNull)
						.map(provider -> {
							try {
								return provider.schema();
							} catch (final Exception failure) {
								throw new IllegalStateException(failure);
							}
						})
						.collect(Collectors.toCollection(ArrayList::new));
		final Map<String, Object> mainSchema = SchemaProvider.resolve(Constants.APP_NAME, classLoader).stream()
						.findFirst()
						.orElseThrow(IllegalStateException::new);
		schemas.add(mainSchema);
		final Config mainDefaults = new BundledDefaultsProvider().config("-", TreeUtil.reduceForest(schemas));

		installShippedDefaults(home);
		final List<Config> allDefaults = extensions.stream()
						.filter(extension -> EXTENSION_ID.equals(extension.id()))
						.map(extension -> extension.defaults(home))
						.filter(Objects::nonNull)
						.collect(Collectors.toCollection(ArrayList::new));
		if (allDefaults.isEmpty()) {
			throw new IllegalStateException("the " + EXTENSION_ID + " extension has no defaults on the class path");
		}
		allDefaults.add(mainDefaults);
		final Config config = ConfigUtil.merge("-", allDefaults);

		config.val("run-id", 1L);
		config.val("storage-driver-type", "s3-aws");
		config.val("storage-driver-limit-concurrency", 1);
		config.val("storage-driver-limit-queue-input", 8);
		config.val("storage-auth-uid", "access-key");
		config.val("storage-auth-secret", "secret-key");
		config.val("storage-region", "us-east-1");
		config.val("storage-net-node-addrs", List.of("127.0.0.1"));
		config.val("storage-net-node-port", port);
		config.val("storage-net-ssl-enabled", false);
		config.val("output-color", false);
		for (final var override : overrides.entrySet()) {
			config.val(override.getKey(), override.getValue());
		}
		return config;
	}

	/** Copies the defaults file the extension ships into the config directory the engine reads it from. */
	private static void installShippedDefaults(final Path home) throws IOException {
		final Path configDir = Files.createDirectories(home.resolve(Constants.DIR_CONFIG));
		try (final InputStream shipped = S3AwsMergedConfig.class.getResourceAsStream(
						"/config/" + DEFAULTS_FILE_NAME)) {
			if (shipped == null) {
				throw new IllegalStateException("the extension ships no " + DEFAULTS_FILE_NAME);
			}
			Files.copy(shipped, configDir.resolve(DEFAULTS_FILE_NAME), StandardCopyOption.REPLACE_EXISTING);
		}
	}
}
