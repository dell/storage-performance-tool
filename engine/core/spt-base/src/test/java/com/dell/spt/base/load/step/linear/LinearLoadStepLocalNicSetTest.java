package com.dell.spt.base.load.step.linear;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.Constants;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.storage.driver.nic.NicSetDriverFactory;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/** A linear step rejects a workload outside the NIC-set matrix before it constructs its driver. */
@SuppressWarnings({"rawtypes", "unchecked"
})
class LinearLoadStepLocalNicSetTest {

	private Config defaults() throws Exception {
		final var schema = SchemaProvider.resolve(Constants.APP_NAME, getClass().getClassLoader()).stream().findFirst().orElseThrow();
		return ConfigUtil.loadConfig(Path.of(getClass().getResource("/config/defaults.yaml").toURI()).toFile(), schema);
	}

	@Test
	void copyAndNoopFailBeforeTheDriver() throws Exception {
		for (final var op : List.of("noop", "create")) {
			final var config = defaults();
			config.val("load-step-id", "nic-set-" + op + "-" + java.util.UUID.randomUUID());
			config.val("load-op-type", op);
			config.val("storage-driver-type", "s3");
			config.val("storage-net-interfaces", List.of("ens2np0"));
			config.val("item-input-path", "/source");
			final NicSetDriverFactory factory = mock(NicSetDriverFactory.class);
			when(factory.id()).thenReturn("s3");
			final var step = new LinearLoadStepLocal(config, List.of(factory), null, mock(MetricsManager.class));
			try {
				final var failure = assertThrows(IllegalStateException.class, step::init);
				assertInstanceOf(IllegalConfigurationException.class, failure.getCause());
				assertTrue(failure.getCause().getMessage().contains("storage.net.interfaces"), failure.getCause().getMessage());
				verify(factory, never()).create(anyString(), any(), any(), anyBoolean(), anyInt());
			} finally {
				step.close();
			}
		}
	}

}
