package com.dell.spt.base.load.lifecycle;

/** Mutually exclusive lifecycle states for one logical operation dispatch. */
public enum OperationLifecycleState {
	/** The operation has not entered a queue. */
	NEW,
	/** The generator owns the operation before driver admission. */
	GENERATOR_BUFFERED,
	/** The driver owns the operation but has not started transport work. */
	DRIVER_QUEUED,
	/** The driver has started the transport attempt. */
	DISPATCHED,
	/** A completion callback owns result construction and publication. */
	COMPLETING,
	/** The result snapshot was accepted by output and terminal ownership was committed. */
	TERMINAL,
	/** Admission closed before transport execution began. */
	UNATTEMPTED,
	/** A dispatched operation or indeterminate compatibility submission outlived its bound. */
	UNRESOLVED;

	boolean canTransitionTo(final OperationLifecycleState next) {
		return switch (this) {
		case NEW -> next == GENERATOR_BUFFERED
						|| next == DRIVER_QUEUED
						|| next == DISPATCHED
						|| next == UNATTEMPTED;
		case GENERATOR_BUFFERED -> next == DRIVER_QUEUED
						|| next == DISPATCHED
						|| next == UNATTEMPTED;
		case DRIVER_QUEUED -> next == DISPATCHED || next == UNATTEMPTED || next == UNRESOLVED;
		case DISPATCHED -> next == COMPLETING || next == UNRESOLVED;
		case COMPLETING -> next == TERMINAL || next == UNRESOLVED;
		case TERMINAL, UNATTEMPTED, UNRESOLVED -> false;
		};
	}
}
