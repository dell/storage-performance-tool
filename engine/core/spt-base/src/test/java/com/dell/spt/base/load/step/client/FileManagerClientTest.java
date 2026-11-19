package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.svc.DroppedConnectTarget;
import java.rmi.ConnectException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** The entry's node lookup gives up on a dropped connect after its 10 s connect timeout. */
class FileManagerClientTest {

	@Test
	void aDroppedConnectGivesUpAfterTenSeconds() throws Exception {
		try (DroppedConnectTarget target = new DroppedConnectTarget()) {
			final long startNanos = System.nanoTime();
			assertThrows(ConnectException.class, () -> FileManagerClient.resolve(target.host() + ":" + target.port()));
			final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

			assertTrue(elapsedMillis >= 9_900, () -> "gave up after " + elapsedMillis + " ms");
			assertTrue(elapsedMillis < 12_000, () -> "gave up after " + elapsedMillis + " ms");
		}
	}
}
