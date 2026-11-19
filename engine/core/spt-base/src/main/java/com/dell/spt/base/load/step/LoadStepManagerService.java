package com.dell.spt.base.load.step;

import com.dell.spt.base.load.step.service.LoadStepService;
import com.dell.spt.base.svc.Service;
import com.github.akurilov.confuse.Config;
import java.rmi.RemoteException;
import java.util.List;

public interface LoadStepManagerService extends Service {

	String SVC_NAME = "load/step/manager";

	String newStepService(final String stepType, final Config config, final List<Config> ctxConfigs)
					throws RemoteException;

	/** The step service created last, open or not; {@code null} before the first or after the close. */
	LoadStepService getStepService() throws RemoteException;

	/**
	 * The run ids of the step slices running on this node, the newest slice first. A slice runs from its
	 * start until it is stopped or closed: one created but never started, or stopped and not yet closed,
	 * is not listed.
	 */
	List<Long> busySliceRunIds() throws RemoteException;

	/** Whether this node holds a step service it created and has not released. */
	boolean hasOpenSlices() throws RemoteException;

	/**
	 * Stops the running slices of a run on this node and leaves them open, so that a live entry can still
	 * collect their results and close them.
	 *
	 * @return whether any open step service belongs to the run
	 */
	boolean stopSlicesOfRun(long runId) throws RemoteException;
}
