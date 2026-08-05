package com.dell.spt.storage.driver.coop.netty;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class NettyStorageDriverShutdownTest {

	@Test
	void waitsForEventLoopTerminationWithinBound() {
		final var executor = mock(EventLoopGroup.class);
		final Future<?> termination = mock(Future.class);
		doReturn(termination).when(executor).shutdownGracefully(0, 0, TimeUnit.NANOSECONDS);
		when(termination.awaitUninterruptibly(
						NettyStorageDriverBase.IO_WORKER_SHUTDOWN_TIMEOUT_SECONDS,
						TimeUnit.SECONDS))
						.thenReturn(true);

		assertTrue(NettyStorageDriverBase.shutdownIoExecutor(executor));

		verify(termination).awaitUninterruptibly(
						NettyStorageDriverBase.IO_WORKER_SHUTDOWN_TIMEOUT_SECONDS,
						TimeUnit.SECONDS);
	}

	@Test
	void reportsEventLoopTerminationTimeout() {
		final var executor = mock(EventLoopGroup.class);
		final Future<?> termination = mock(Future.class);
		doReturn(termination).when(executor).shutdownGracefully(0, 0, TimeUnit.NANOSECONDS);
		when(termination.awaitUninterruptibly(
						NettyStorageDriverBase.IO_WORKER_SHUTDOWN_TIMEOUT_SECONDS,
						TimeUnit.SECONDS))
						.thenReturn(false);

		assertFalse(NettyStorageDriverBase.shutdownIoExecutor(executor));
	}
}
