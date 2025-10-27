package com.dell.spt.base.load.step.linear;

import com.dell.spt.base.env.Extension;
import com.dell.spt.base.env.ExtensionBase;
import com.dell.spt.base.load.step.LoadStepFactory;
import com.dell.spt.base.metrics.MetricsManager;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class LinearLoadStepExtension<T extends LinearLoadStepLocal>
				extends ExtensionBase
				implements LoadStepFactory<T, LinearLoadStepClient> {

	public static final String TYPE = "Load";

	private static final List<String> RES_INSTALL_FILES = Collections.unmodifiableList(
					Arrays.asList());

	@Override
	public final String id() {
		return TYPE;
	}

	@Override
	@SuppressWarnings("unchecked")
	public final T createLocal(
					final Config baseConfig, final List<Extension> extensions, final List<Config> contextConfigs,
					final MetricsManager metricsManager) {
		return (T) new LinearLoadStepLocal(baseConfig, extensions, contextConfigs, metricsManager);
	}

	@Override
	public final LinearLoadStepClient createClient(
					final Config baseConfig, final List<Extension> extensions, final MetricsManager metricsManager) {
		return new LinearLoadStepClient(baseConfig, extensions, null, metricsManager);
	}

	@Override
	public final SchemaProvider schemaProvider() {
		return null;
	}

	@Override
	protected final String defaultsFileName() {
		return null;
	}

	@Override
	protected final List<String> resourceFilesToInstall() {
		return RES_INSTALL_FILES;
	}
}
