package com.dell.spt.base.storage.driver.bucket;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.load.step.client.TestLoadStepClient;
import com.dell.spt.base.metrics.MetricsManager;
import com.github.akurilov.confuse.Config;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** The controller engine provisions before metrics contexts, slicing and any slice start. */
class ControllerTargetBucketProvisioningTest {

	@ParameterizedTest(name = "remote slices: {0}")
	@ValueSource(booleans = {false, true
	})
	void provisioningFailureEndsTheStepBeforeInitAndSlices(final boolean distributed) throws Exception {
		final var extension = new ScriptedTargetBucketExtension().check("bench", TargetBucketResponse.notFound());
		final Config config = TargetBucketProvisioningTest.stepConfig("create", "/bench");
		config.val(TargetBucketProvisioning.POLICY, false);
		config.val("load-step-id", "create-step");
		config.val("load-step-node-addrs", distributed ? List.of("192.0.2.10:1099", "192.0.2.11:1099") : List.of());
		final var metrics = mock(MetricsManager.class);
		final var client = new TestLoadStepClient(config, List.<Extension> of(extension), null, metrics);

		final var failure = assertThrows(RunTerminalException.class, client::run);

		assertFalse(failure instanceof IntegrityTerminalException);
		assertEquals("create-step", failure.stepId());
		assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
		assertEquals(List.of("HEAD bench"), extension.requests);
		assertEquals(1, extension.closed);
		assertFalse(client.wasInitCalled(), "metrics contexts must not exist when provisioning fails");
		verify(metrics, never()).register(any());
		assertSame(failure, client.runFailure());
	}

	@ParameterizedTest(name = "remote slices: {0}")
	@ValueSource(booleans = {false, true
	})
	void provisioningUsesTheUnslicedStepConfiguration(final boolean distributed) throws Exception {
		final var extension = new ScriptedTargetBucketExtension().check("bench", TargetBucketResponse.ofHttpStatus(403));
		final Config config = TargetBucketProvisioningTest.stepConfig("create", "/bench");
		config.val("storage-driver-limit-concurrency", 64);
		config.val("load-step-node-addrs", distributed ? List.of("192.0.2.10:1099") : List.of());
		final var client = new TestLoadStepClient(config, List.<Extension> of(extension), null, mock(MetricsManager.class));

		assertThrows(RunTerminalException.class, client::start);

		assertEquals(1, extension.opened, "only the controller opens a control client, once per step");
		assertEquals(64, extension.openedWith.get(0).intVal("driver-limit-concurrency"));
	}
}
