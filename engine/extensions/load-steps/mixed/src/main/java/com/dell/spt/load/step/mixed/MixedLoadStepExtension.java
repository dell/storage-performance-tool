package com.dell.spt.load.step.mixed;

import com.dell.spt.base.env.Extension;
import com.dell.spt.base.env.ExtensionBase;
import com.dell.spt.base.load.step.LoadStepFactory;
import com.dell.spt.base.metrics.MetricsManager;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.io.yaml.YamlSchemaProviderBase;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.dell.spt.base.Constants.APP_NAME;

/**
 * Extension entry-point for the mixed workload load step ({@code MixedLoad} in JS scenarios).
 *
 * <p>Registered via the Java {@link java.util.ServiceLoader} mechanism in
 * {@code META-INF/services/com.dell.spt.base.env.Extension}.
 */
public final class MixedLoadStepExtension
				extends ExtensionBase
				implements LoadStepFactory<MixedLoadStepLocal, MixedLoadStepClient> {

	public static final String TYPE = "MixedLoad";

	private static final SchemaProvider SCHEMA_PROVIDER = new YamlSchemaProviderBase() {
		@Override
		protected InputStream schemaInputStream() {
			return getClass().getResourceAsStream("/config-schema-load-op-mixed.yaml");
		}

		@Override
		public String id() {
			return APP_NAME;
		}
	};

	private static final String DEFAULTS_FILE_NAME = "defaults-load-op-mixed.yaml";

	private static final List<String> RES_INSTALL_FILES = Collections.unmodifiableList(Arrays.asList("config/" + DEFAULTS_FILE_NAME));

	@Override
	public String id() {
		return TYPE;
	}

	@Override
	@SuppressWarnings("unchecked")
	public MixedLoadStepLocal createLocal(
					final Config baseConfig,
					final List<Extension> extensions,
					final List<Config> contextConfigs,
					final MetricsManager metricsManager) {
		return new MixedLoadStepLocal(baseConfig, extensions, contextConfigs, metricsManager);
	}

	@Override
	public MixedLoadStepClient createClient(
					final Config baseConfig,
					final List<Extension> extensions,
					final MetricsManager metricsManager) {
		return new MixedLoadStepClient(baseConfig, extensions, null, metricsManager);
	}

	@Override
	public SchemaProvider schemaProvider() {
		return SCHEMA_PROVIDER;
	}

	@Override
	protected String defaultsFileName() {
		return DEFAULTS_FILE_NAME;
	}

	@Override
	protected List<String> resourceFilesToInstall() {
		return RES_INSTALL_FILES;
	}
}
