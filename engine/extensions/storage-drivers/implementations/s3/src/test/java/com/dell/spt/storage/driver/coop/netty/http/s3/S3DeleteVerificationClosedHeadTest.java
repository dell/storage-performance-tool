package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.op.deletion.DeleteInventoryVerifier;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import com.dell.spt.base.item.op.deletion.DeleteVerificationCause;
import com.dell.spt.base.item.op.deletion.DeleteVerificationPhase;
import com.dell.spt.base.item.op.deletion.DeleteVerificationProbe;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** DELETE verification through the real Netty driver against a loopback target that closes every HEAD. */
final class S3DeleteVerificationClosedHeadTest {
	private static final Pattern OBJECT_HEAD = Pattern.compile("^HEAD /bucket/([^ ?]+)");

	private final Map<String, AtomicInteger> headsPerKey = new ConcurrentHashMap<>();
	private ServerSocket server;
	private Thread acceptor;

	@BeforeEach
	void startClosingTarget() throws IOException {
		server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
		acceptor = Thread.ofPlatform().daemon().name("closing-head-target").start(() -> {
			while (!server.isClosed()) {
				try {
					final Socket connection = server.accept();
					// One thread per connection: the driver may hold an idle connection open.
					Thread.ofVirtual().start(() -> closeAfterRequestHead(connection));
				} catch (final IOException closed) {
					// The test closed the server.
				}
			}
		});
	}

	private void closeAfterRequestHead(final Socket connection) {
		try (connection) {
			final Matcher object = OBJECT_HEAD.matcher(readRequestHead(connection.getInputStream()));
			if (object.find()) {
				headsPerKey.computeIfAbsent(object.group(1), ignored -> new AtomicInteger()).incrementAndGet();
			}
			// Close without a response, as a target that drops the control connection does.
		} catch (final IOException closed) {
			// The client went away first.
		}
	}

	@AfterEach
	void stopClosingTarget() throws Exception {
		server.close();
		acceptor.join(5_000);
	}

	@Test
	@Timeout(60)
	void closedHeadsAreRetriedWithBackOffAndLeftUnresolvedAtTheDeadline(@TempDir final Path temp)
					throws Exception {
		final Path manifest = temp.resolve("verify-input.csv");
		Files.writeString(manifest, "bucket,key,size,version_id\nbucket,one,1,\nbucket,two,1,\n");
		final Duration timeout = Duration.ofSeconds(3);
		try (final var driver = newDriver()) {
			driver.start();
			final DeleteVerificationProbe.Observation closed = driver.observe(
							S3DeleteRequestTestFixture.target("probe", null));
			assertEquals(DeleteVerificationProbe.Presence.UNRESOLVED, closed.presence());
			assertEquals(DeleteVerificationCause.noResponse(), closed.unresolvedCause());

			try (final var report = DeleteInventoryVerifier.verify(
							manifest, 2, DeleteVerificationPhase.POST_DELETE, timeout, driver)) {
				assertEquals(2, report.unresolved());
				assertTrue(report.elapsed().compareTo(timeout.minusMillis(10)) >= 0,
								"verification ended before its final pass");
			}
		}
		for (final String key : List.of("one", "two")) {
			final int heads = headsPerKey.getOrDefault(key, new AtomicInteger()).get();
			// About 10 passes fit 3 s with back-off; a fixed 10 ms pause sent over a hundred. The complete
			// pass is the only one guaranteed: on a loaded host it may take most of the window.
			assertTrue(heads >= 1 && heads <= 15, () -> key + " got " + heads + " HEADs in " + timeout);
		}
	}

	private S3StorageDriver<IntegrityManifestDataItem, DeleteRequestOperation> newDriver() throws Exception {
		final Config config = S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1");
		config.val("storage-driver-limit-concurrency", 1);
		config.val("storage-driver-limit-queue-input", 8);
		config.val("storage-net-node-port", server.getLocalPort());
		config.val("storage-net-timeoutMilliSec", 2_000);
		return new S3StorageDriver<>(
						"delete-verification-closed-head-test",
						DataInput.instance(
										null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false, 0.0, true),
						config.configVal("storage"),
						false,
						config.intVal("load-batch-size"));
	}

	private static String readRequestHead(final InputStream input) throws IOException {
		final StringBuilder head = new StringBuilder();
		int value;
		while ((value = input.read()) >= 0) {
			head.append((char) value);
			if (head.length() >= 4 && head.lastIndexOf("\r\n\r\n") == head.length() - 4) {
				break;
			}
		}
		return head.toString();
	}
}
