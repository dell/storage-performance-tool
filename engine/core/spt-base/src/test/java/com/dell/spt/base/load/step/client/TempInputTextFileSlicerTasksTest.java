package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.concurrent.TaskBase;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class TempInputTextFileSlicerTasksTest {

	@Test
	void startTasksStartsAllTasks() throws InterruptedException {
		final var startedLatch = new CountDownLatch(2);
		final TaskBase task1 = new TaskBase(ServiceTaskExecutor.VT_EXECUTOR) {
			@Override
			protected void doWork() throws Exception {
				startedLatch.countDown();
				stop();
			}
		};
		final TaskBase task2 = new TaskBase(ServiceTaskExecutor.VT_EXECUTOR) {
			@Override
			protected void doWork() throws Exception {
				startedLatch.countDown();
				stop();
			}
		};
		TempInputTextFileSlicer.startTasks("step-test", List.of(task1, task2), "source.txt");
		assertTrue(startedLatch.await(2, TimeUnit.SECONDS), "Both tasks should have started");
	}
}
