package com.dell.spt.base.load.step;

import com.dell.spt.base.concurrent.Daemon;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.concurrent.AsyncRunnable;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public interface LoadStep extends Daemon {

	/** Returns the unique identifier for this step. */
	String loadStepId() throws RemoteException;

	long runId() throws RemoteException;

	String getTypeName() throws RemoteException;

	List<? extends AllMetricsSnapshot> metricsSnapshots() throws RemoteException;

	@Override
	AsyncRunnable start() throws RemoteException;

	@Override
	AsyncRunnable await() throws InterruptedException, RemoteException;

	@Override
	boolean await(final long timeout, final TimeUnit timeUnit)
					throws InterruptedException, RemoteException;

	@Override
	AsyncRunnable stop() throws RemoteException;

	@Override
	void close() throws IOException;
}
