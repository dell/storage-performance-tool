package com.dell.spt.load.step.weighted;

import com.dell.spt.base.env.Extension;
import com.dell.spt.base.env.ExtensionBase;
import com.dell.spt.base.load.step.LoadStepFactory;
import static com.dell.spt.base.Constants.APP_NAME;

import com.dell.spt.base.metrics.MetricsManager;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;

import com.github.akurilov.confuse.io.yaml.YamlSchemaProviderBase;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class WeightedLoadStepExtension<T extends WeightedLoadStepLocal>
				extends ExtensionBase
				implements LoadStepFactory<T, WeightedLoadStepClient> {

	public static final String TYPE = "WeightedLoad";

	private static final SchemaProvider SCHEMA_PROVIDER = new YamlSchemaProviderBase() {

		@Override
		protected final InputStream schemaInputStream() {
			return getClass().getResourceAsStream("/config-schema-load-generator-weight.yaml");
		}

		@Override
		public final String id() {
			return APP_NAME;
		}
	};

	private static final String DEFAULTS_FILE_NAME = "defaults-load-generator-weight.yaml";

	private static final List<String> RES_INSTALL_FILES = Collections.unmodifiableList(
					Arrays.asList("config/" + DEFAULTS_FILE_NAME));

	@Override
	public final String id() {
		return TYPE;
	}

	@Override
	@SuppressWarnings("unchecked")
	public final T createLocal(
					final Config baseConfig, final List<Extension> extensions, final List<Config> contextConfigs,
					final MetricsManager metricsManager) {
		return (T) new WeightedLoadStepLocal(baseConfig, extensions, contextConfigs, metricsManager);
	}

	@Override
	public final WeightedLoadStepClient createClient(
					final Config baseConfig, final List<Extension> extensions, final MetricsManager metricsManager) {
		return new WeightedLoadStepClient(baseConfig, extensions, null, metricsManager);
	}

	@Override
	public final SchemaProvider schemaProvider() {
		return SCHEMA_PROVIDER;
	}

	@Override
	protected final String defaultsFileName() {
		return DEFAULTS_FILE_NAME;
	}

	@Override
	protected final List<String> resourceFilesToInstall() {
		return RES_INSTALL_FILES;
	}
}
