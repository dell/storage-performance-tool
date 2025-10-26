package com.dell.spt.base.load.step.client;

import com.dell.spt.base.load.step.service.file.FileManagerService;
import com.dell.spt.base.svc.ServiceUtil;

public interface FileManagerClient {
	static FileManagerService resolve(final String nodeAddrWithPort)
					throws java.rmi.NotBoundException, java.rmi.RemoteException, java.net.URISyntaxException,
					java.net.MalformedURLException {
		return ServiceUtil.resolve(nodeAddrWithPort, FileManagerService.SVC_NAME, FileManagerService.class);
	}
}
