package com.dell.spt.base.metrics.context;

import com.dell.spt.base.metrics.bucket.BucketRows;
import com.dell.spt.base.metrics.steady.SteadySettings;
import java.util.List;

/** What an entry node knows about one step's bucket metrics when it writes their files. */
public interface BucketMetricsSource {

	/** Each expected node's answer for this step, fetched once and held until the step closes. */
	List<BucketRows.Contribution> contributions();

	/** What makes a window {@code valid}, or null when this step measures no window. */
	SteadySettings steadySettings();

	/** This context's place in its step, as workers number theirs. */
	int contextIndex();

	/** Lets go of the workers' payloads, once the step's last context has written its rows. */
	default void release() {}
}
