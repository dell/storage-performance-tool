package com.dell.spt.base.concurrent;

public interface ServiceTaskExecutor {
	ThreadTaskExecutor TASK_EXECUTOR = new PlatformThreadExecutor();
	VirtualThreadExecutor VT_EXECUTOR = new VirtualThreadExecutor();
}
