package com.dell.spt.base.load.step.service;

import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.svc.Service;

public interface LoadStepService extends Service, LoadStep {

	String SVC_NAME_PREFIX = "load/step/";
}
