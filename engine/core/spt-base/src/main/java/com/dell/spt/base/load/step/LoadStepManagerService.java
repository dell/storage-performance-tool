package com.dell.spt.base.load.step;

import com.dell.spt.base.load.step.service.LoadStepService;
import com.dell.spt.base.svc.Service;
import com.github.akurilov.confuse.Config;
import java.rmi.RemoteException;
import java.util.List;

public interface LoadStepManagerService extends Service {

	String SVC_NAME = "load/step/manager";
	/** The message of {@link #prepareStepIdFiles}'s default, which the Entry Node recognizes as an older worker's. */
	String NO_STEP_ID_FILES_PREPARATION = "worker does not prepare a step's files";

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

	/**
	 * Prepares this node's files for step {@code stepId} of run {@code runId}, before the Entry Node
	 * creates or starts any slice of the step: when no earlier step of that run here had the id, the
	 * files of {@code loggerNames} an earlier run left under it are moved aside, with one warning, so
	 * that the slice starts as on a clean folder. The Entry Node calls it only for a step whose slice
	 * here publishes such files, and only for the first step of the id in its run; a later one finds
	 * those files refused.
	 *
	 * @param loggerNames the loggers of the files the slice here publishes and refuses to replace
	 * @return whether the step is the first of its id in its run here; when not, nothing was moved,
	 *     and the Entry Node refuses a file there as one an earlier step of the run left
	 * @throws RemoteException also when a file could not be checked or moved aside, as a {@code
	 *     StepIdCheckException} in its cause chain; an older worker does not know the call
	 */
	default boolean prepareStepIdFiles(final long runId, final String stepId, final List<String> loggerNames)
					throws RemoteException {
		throw new RemoteException(NO_STEP_ID_FILES_PREPARATION);
	}
}
