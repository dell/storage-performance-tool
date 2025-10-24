package com.dell.spt.base.control.run;

public interface Run extends Runnable {

	/** Returns the run id. */
	long runId() throws IllegalStateException;

	/** Returns the user comment associated with this run. */
	String comment();
}
