package com.dell.spt.base.storage.driver;

/**
 * A storage driver with work to do after it starts and before a NIC-set step opens admission. The
 * work runs on the step's preparation thread, within its deadline, and outside the Load Interval.
 */
public interface AdmissionPreparable {

	/**
	 * @throws Exception when the driver cannot serve the step, which fails the step with this
	 *     exception's message on every engine
	 */
	void prepareForAdmission(Preparation preparation) throws Exception;

	/** The preparation a driver runs in. */
	interface Preparation {

		/** The {@link System#nanoTime()} at which this preparation is fenced. */
		long deadlineNanos();

		/** Returns whether this preparation has been fenced, so the work should stop. */
		boolean fenced();

		/** Names what the preparation is doing, for the message a timeout gives. */
		void phase(String phase);

		/** Registers an action that interrupts blocked work when the preparation is fenced. */
		void onFence(Runnable action);
	}
}
