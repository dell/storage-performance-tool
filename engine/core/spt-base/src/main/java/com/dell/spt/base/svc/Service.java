package com.dell.spt.base.svc;

import com.dell.spt.base.concurrent.AsyncRunnable;
import java.rmi.Remote;
import java.rmi.RemoteException;

/** A remote service which has a name for resolution by URI. */
public interface Service extends AutoCloseable, AsyncRunnable, Remote {

	int registryPort() throws RemoteException;

	String name() throws RemoteException;
}
