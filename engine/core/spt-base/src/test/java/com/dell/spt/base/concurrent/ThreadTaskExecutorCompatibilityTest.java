package com.dell.spt.base.concurrent;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.load.step.client.metrics.MetricsSnapshotsSupplierTaskImpl;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

class ThreadTaskExecutorCompatibilityTest {

	@Test
	void legacyVirtualThreadConstructorDescriptorsRemainAvailable() throws Exception {
		assertNotNull(TaskBase.class.getDeclaredConstructor(VirtualThreadExecutor.class));
		assertNotNull(MetricsManagerImpl.class.getConstructor(VirtualThreadExecutor.class));
		assertNotNull(MetricsSnapshotsSupplierTaskImpl.class.getConstructor(
						VirtualThreadExecutor.class, LoadStep.class));
	}

	@Test
	void legacyVirtualThreadExecutorMethodsRemainOverridable() throws Exception {
		assertFalse(Modifier.isFinal(ThreadTaskExecutor.class
						.getMethod("submit", Runnable.class).getModifiers()));
		assertFalse(Modifier.isFinal(ThreadTaskExecutor.class
						.getMethod("close").getModifiers()));
		assertFalse(Modifier.isFinal(ThreadTaskExecutor.class
						.getMethod("isShutdown").getModifiers()));
	}
}
