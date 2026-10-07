package com.dell.spt.storage.driver.coop.netty.endpoint;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/** Rotates over a fixed destination list, starting at the first entry, once per selection. */
public final class RoundRobinDestinations implements DestinationSource {

	private final List<InetSocketAddress> destinations;
	private final AtomicLong selections = new AtomicLong();

	public RoundRobinDestinations(final List<InetSocketAddress> destinations) {
		if (destinations.isEmpty()) {
			throw new IllegalArgumentException("Round robin requires at least one destination");
		}
		this.destinations = List.copyOf(destinations);
	}

	@Override
	public Future<InetSocketAddress> next() {
		final var index = (int) Long.remainderUnsigned(selections.getAndIncrement(), destinations.size());
		return ImmediateEventExecutor.INSTANCE.newSucceededFuture(destinations.get(index));
	}
}
