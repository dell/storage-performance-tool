package com.dell.spt.base.svc;

import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.concurrent.AsyncRunnableBase;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.URISyntaxException;
import java.rmi.RemoteException;
import org.apache.logging.log4j.Level;

public abstract class ServiceBase extends AsyncRunnableBase implements Service {

	protected final int port;

	protected ServiceBase(final int port) {
		this.port = port;
	}

	@Override
	public final int registryPort() {
		return port;
	}

	@Override
	protected void doStart() {
		try {
			ServiceUtil.create(this, port);
			Loggers.MSG.info("Service \"{}\" started @ port #{}", safeName(), port);
		} catch (final RemoteException | URISyntaxException | MalformedURLException | SocketException e) {
			LogUtil.exception(Level.ERROR, e, "Failed to start the service \"{}\" @ port #{}", safeName(), port);
			throw new IllegalStateException("Failed to start service " + safeName(), e);
		}
	}

	@Override
	protected void doShutdown() {}

	@Override
	protected void doStop() {
		try {
			ServiceUtil.close(this);
			Loggers.MSG.info("Service \"{}\" stopped listening the port #{}", safeName(), port);
		} catch (final RemoteException | MalformedURLException e) {
			LogUtil.exception(Level.WARN, e, "Failed to stop the service \"{}\"", safeName());
		}
	}

	private String safeName() {
		try {
			return name();
		} catch (final RemoteException e) {
			return getClass().getSimpleName();
		}
	}
}
