package com.dell.spt.storage.driver.coop.netty.endpoint;

import io.netty.util.concurrent.Future;
import java.net.InetSocketAddress;

/** Chooses the connect destination of each request attempt. */
public interface DestinationSource extends AutoCloseable {

	/** Selects the destination of one request attempt; every call is a new selection. */
	Future<InetSocketAddress> next();

	/** Releases resources owned by the source, such as a DNS resolver. */
	@Override
	default void close() {}
}
