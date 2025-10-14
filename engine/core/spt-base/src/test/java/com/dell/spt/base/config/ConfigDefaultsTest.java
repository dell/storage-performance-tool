package com.dell.spt.base.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.dell.spt.base.Constants;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import java.net.URL;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class ConfigDefaultsTest {

	@Test
	void logLevelDefaultPresent() throws Exception {
		final var schema = SchemaProvider.resolve(Constants.APP_NAME, Thread.currentThread().getContextClassLoader())
						.stream()
						.findFirst()
						.orElseThrow();
		final URL defaultsUrl = ConfigDefaultsTest.class.getClassLoader().getResource("config/defaults.yaml");
		assertNotNull(defaultsUrl, "defaults.yaml should be on the classpath");
		final Config config = ConfigUtil.loadConfig(Path.of(defaultsUrl.toURI()).toFile(), schema);
		assertEquals("info", config.stringVal("log-level"), "default log level should be info");
	}
}
