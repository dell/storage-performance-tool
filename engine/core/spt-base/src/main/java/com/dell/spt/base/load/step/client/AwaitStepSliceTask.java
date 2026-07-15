package com.dell.spt.base.load.step.client;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.concurrent.TaskBase;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import java.rmi.RemoteException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;

public class AwaitStepSliceTask extends TaskBase {

	private static final long POLL_TIMEOUT_MILLIS = 100;

	private final LoadStep stepSlice;
	private final String loadStepId;
	private final CountDownLatch awaitCountDown;

	public AwaitStepSliceTask(final LoadStep stepSlice, final CountDownLatch awaitCountDown) {
		super(ServiceTaskExecutor.TASK_EXECUTOR);
		this.stepSlice = stepSlice;
		try {
			this.loadStepId = stepSlice.loadStepId();
		} catch (final RemoteException e) {
			LogUtil.exception(Level.ERROR, e, "Failed to connect the load step slice");
			throw new IllegalStateException(e);
		}
		this.awaitCountDown = awaitCountDown;
	}

	@Override
	protected final void doWork() throws Exception {
		Loggers.MSG.trace("{}: await for the step slice \"{}\" started", loadStepId, stepSlice);
		try {
			if (stepSlice.await(POLL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
				awaitCountDown.countDown();
				stop();
			}
		} catch (final RemoteException e) {
			LogUtil.exception(
							Level.WARN,
							e,
							"Failed to invoke the remote await method on the step slice \"{}\"",
							stepSlice);
		} catch (final IllegalStateException e) {
			LogUtil.exception(Level.WARN, e, "{}: failure in the await method", loadStepId);
		}
	}
}
