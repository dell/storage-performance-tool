package com.dell.spt.base.load.step;

import com.dell.spt.base.load.step.service.LoadStepService;
import com.dell.spt.base.svc.Service;
import com.github.akurilov.confuse.Config;
import java.rmi.RemoteException;
import java.util.List;

public interface LoadStepManagerService extends Service {

	String SVC_NAME = "load/step/manager";

	String newStepService(final String stepType, final Config config, final List<Config> ctxConfigs)
					throws RemoteException;

	LoadStepService getStepService() throws RemoteException;
}
