package com.dell.spt.base.load.step;

import com.dell.spt.base.env.Extension;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.dell.spt.base.load.step.client.LoadStepClient;
import com.dell.spt.base.metrics.MetricsManager;
import com.github.akurilov.confuse.Config;

public interface LoadStepFactory<T extends LoadStep, U extends LoadStepClient<U>> extends Extension {

	T createLocal(
					final Config baseConfig,
					final List<Extension> extensions,
					final List<Config> contextConfigs,
					final MetricsManager metricsManager);

	U createClient(
					final Config baseConfig,
					final List<Extension> extensions,
					final MetricsManager metricsManager);

	@SuppressWarnings("unchecked")
	static LoadStep createLocalLoadStep(
					final Config baseConfig,
					final List<Extension> extensions,
					final List<Config> contextConfigs,
					final MetricsManager metricsManager,
					final String stepType) {

		final List<LoadStepFactory<?, ?>> loadStepFactories = extensions.stream()
						.filter(ext -> ext instanceof LoadStepFactory)
						.map(ext -> (LoadStepFactory<?, ?>) ext)
						.collect(Collectors.toList());

		final LoadStepFactory<?, ?> selectedFactory = loadStepFactories.stream()
						.filter(f -> stepType.equals(f.id()))
						.findFirst()
						.orElseThrow(
										() -> new IllegalStateException(
														"Failed to find the load step implementation for type \""
																		+ stepType
																		+ "\", available types: "
																		+ Arrays.toString(
																						loadStepFactories.stream().map(LoadStepFactory::id).toArray())));

		return (LoadStep) selectedFactory.createLocal(baseConfig, extensions, contextConfigs, metricsManager);
	}
}
