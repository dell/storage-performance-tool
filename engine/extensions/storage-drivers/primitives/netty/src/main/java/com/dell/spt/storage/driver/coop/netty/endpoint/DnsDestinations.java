package com.dell.spt.storage.driver.coop.netty.endpoint;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import java.net.InetAddress;
import java.net.InetSocketAddress;

/** Resolves the logical hostname afresh for every selection and pairs the answer with the endpoint port. */
public final class DnsDestinations implements DestinationSource {

	private final PerRequestDnsResolver resolver;
	private final int port;

	public DnsDestinations(final PerRequestDnsResolver resolver, final int port) {
		this.resolver = resolver;
		this.port = port;
	}

	@Override
	public Future<InetSocketAddress> next() {
		// Listeners run on the resolver's loop when the lookup completes; nothing waits on this promise.
		final Promise<InetSocketAddress> destination = ImmediateEventExecutor.INSTANCE.newPromise();
		resolver.resolve().addListener((Future<InetAddress> lookup) -> {
			if (lookup.isSuccess()) {
				destination.trySuccess(new InetSocketAddress(lookup.getNow(), port));
			} else {
				destination.tryFailure(lookup.cause());
			}
		});
		return destination;
	}

	@Override
	public void close() {
		resolver.close();
	}
}
