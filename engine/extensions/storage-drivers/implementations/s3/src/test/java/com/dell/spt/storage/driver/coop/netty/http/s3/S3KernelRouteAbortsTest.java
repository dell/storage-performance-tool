package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.channel.socket.nio.NioSocketChannel;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The registry of pending kernel-route aborts, and closing their channels at the stop (D87). */
class S3KernelRouteAbortsTest {

	@Test
	void aClosedRegistryRefusesEveryLaterAbortAndHandsOverThePendingOnes() {
		final var aborts = new S3StorageDriver.KernelRouteAborts<String>();
		assertTrue(aborts.register("settled"));
		assertTrue(aborts.register("pending"));
		aborts.remove("settled");
		assertEquals(1, aborts.pending());

		assertEquals(List.of("pending"), aborts.close(), "the stop settles what is still pending");
		assertFalse(aborts.register("late"), "an abort submitted after the stop starts nothing");
		assertEquals(1, aborts.pending(), "the refused abort was not registered");
		assertEquals(List.of("pending"), aborts.close(), "closing again changes nothing");
		aborts.remove("pending");
		assertEquals(0, aborts.pending());
		assertFalse(aborts.register("later"), "closed for good");
	}

	/**
	 * A connect whose channel could not be created or initialised returns a channel that never
	 * registered with an event loop, and its {@code close()} throws. The stop closes it all the same,
	 * so the permit it gives back next is never skipped.
	 */
	@Test
	void aChannelThatNeverRegisteredClosesWithoutThrowing() {
		final var unregistered = new NioSocketChannel();
		assertTrue(unregistered.isOpen());
		assertThrows(IllegalStateException.class, unregistered::close, "why the helper exists");

		final var channel = new NioSocketChannel();
		assertDoesNotThrow(() -> S3StorageDriver.closeKernelRouteChannel(channel));
		assertFalse(channel.isOpen(), "closed forcibly");
		unregistered.unsafe().closeForcibly();
	}
}
