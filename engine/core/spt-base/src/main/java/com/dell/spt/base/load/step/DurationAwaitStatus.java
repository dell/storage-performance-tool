package com.dell.spt.base.load.step;

/**
 * Worker-local duration validity evidence. Monotonic timestamps remain private to the worker which
 * owns them; distributed controllers exchange only this semantic result.
 */
public enum DurationAwaitStatus {
	NOT_STARTED, RUNNING, REACHED_DEADLINE, EXHAUSTED_BEFORE_DEADLINE, FAILED
}
