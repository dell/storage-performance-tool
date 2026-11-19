package com.dell.spt.base.svc;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.net.URISyntaxException;

/** Gives tests outside this package the connect-timeout resolve with a timeout of their own. */
public final class ServiceUtilTestAccess {

	private ServiceUtilTestAccess() {}

	public static <S extends Service> S resolveWithConnectTimeout(
					final String addr, final String name, final Class<S> type, final int connectTimeoutMillis)
					throws NotBoundException, RemoteException, URISyntaxException {
		return ServiceUtil.resolve(addr, name, type, new ConnectTimeoutClientSocketFactory(connectTimeoutMillis));
	}
}
