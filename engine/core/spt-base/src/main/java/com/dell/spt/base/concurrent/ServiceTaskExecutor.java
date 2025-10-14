package com.dell.spt.base.concurrent;

import com.github.akurilov.fiber4j.FibersExecutor;

public interface ServiceTaskExecutor {
	FibersExecutor INSTANCE = new FibersExecutor();
}
