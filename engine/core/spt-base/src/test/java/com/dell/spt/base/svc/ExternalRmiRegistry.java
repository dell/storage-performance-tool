package com.dell.spt.base.svc;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Holds an RMI registry in a separate process for {@link ServiceUtilTest}: it creates the registry
 * on the port given as its argument, prints {@code READY}, and exits when its standard input
 * closes.
 */
public final class ExternalRmiRegistry {

	private ExternalRmiRegistry() {}

	public static void main(final String[] args) throws Exception {
		final Registry registry = LocateRegistry.createRegistry(Integer.parseInt(args[0]));
		System.out.println("READY");
		System.out.flush();
		while (System.in.read() >= 0) {
			// wait for the test to close standard input
		}
		if (registry.list() == null) {
			throw new IllegalStateException("no registry");
		}
	}
}
