package com.dell.spt.base.load.step.client;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.github.akurilov.fiber4j.ExclusiveFiberBase;
import java.rmi.RemoteException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

public class AwaitStepSliceTask extends ExclusiveFiberBase {

	private final LoadStep stepSlice;
	private final String loadStepId;
	private final CountDownLatch awaitCountDown;

	public AwaitStepSliceTask(final LoadStep stepSlice, final CountDownLatch awaitCountDown) {
		super(ServiceTaskExecutor.INSTANCE);
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
	protected final void invokeTimedExclusively(final long startTimeNanos) {
		Loggers.MSG.trace("{}: await for the step slice \"{}\" started", loadStepId, stepSlice);
		try {
			if (stepSlice.await(SOFT_DURATION_LIMIT_NANOS, TimeUnit.NANOSECONDS)) {
				awaitCountDown.countDown();
				stop();
			}
		} catch (final RemoteException e) {
			LogUtil.exception(
							Level.WARN,
							e,
							"Failed to invoke the remote await method on the step slice \"{}\"",
							stepSlice);
		} catch (final InterruptedException e) {
			throwUnchecked(e);
		} catch (final IllegalStateException e) {
			LogUtil.exception(Level.WARN, e, "{}: failure in the await method", loadStepId);
		}
	}
}
