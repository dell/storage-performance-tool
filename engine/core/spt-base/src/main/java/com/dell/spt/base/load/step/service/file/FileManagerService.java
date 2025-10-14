package com.dell.spt.base.load.step.service.file;

import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.svc.Service;

/** Remote file access service. All files created are deleted when JVM exits. */
public interface FileManagerService extends FileManager, Service {

	String SVC_NAME = "file/manager";
}
