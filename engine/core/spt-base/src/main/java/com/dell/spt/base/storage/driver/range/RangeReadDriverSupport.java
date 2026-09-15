package com.dell.spt.base.storage.driver.range;

import com.dell.spt.base.load.step.local.context.range.RangeReadRuntime;

/** Explicit opt-in for fully range-aware adapters; ordinary drivers need not implement this. */
public interface RangeReadDriverSupport {
	RangeReadRuntime<?> rangeReadRuntime();
}
