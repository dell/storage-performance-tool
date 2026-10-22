package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.env.Extension;
import com.dell.spt.base.storage.driver.StorageDriverFactory;
import com.dell.spt.base.storage.driver.bucket.TargetBucketControlFactory;
import com.dell.spt.base.storage.driver.bucket.TargetBucketProvisioning;
import com.dell.spt.base.storage.driver.bucket.TargetBucketResponse;
import com.dell.spt.storage.driver.coop.netty.http.s3.S3TargetBucketControl;
import com.sun.net.httpserver.HttpServer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/** S3-RDMA provisions over plain HTTP, so the controller needs no RDMA device or native library. */
class S3RdmaTargetBucketControlTest {

	@Test
	void factoryOptsIntoTheCapability() {
		final var factory = Extension.load(Thread.currentThread().getContextClassLoader()).stream()
						.filter(e -> e instanceof StorageDriverFactory<?, ?, ?> f && "s3-rdma".equals(f.id()))
						.findFirst()
						.orElseThrow();
		assertInstanceOf(TargetBucketControlFactory.class, factory);
	}

	@Test
	void controllerCreatesTheBucketOverHttpWithRdmaEnabled() throws Exception {
		final var requests = new CopyOnWriteArrayList<String>();
		final var created = new AtomicBoolean();
		final var server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
		server.createContext("/", exchange -> {
			final var method = exchange.getRequestMethod();
			requests.add(method + " " + exchange.getRequestURI().getRawPath()
							+ (exchange.getRequestHeaders().keySet().stream().anyMatch(h -> h.toLowerCase().startsWith("x-amz-rdma"))
											? " with-rdma-token"
											: ""));
			exchange.getRequestBody().readAllBytes();
			final int status = "PUT".equals(method) ? 200 : created.get() ? 200 : 404;
			if ("PUT".equals(method)) {
				created.set(true);
			}
			exchange.sendResponseHeaders(status, -1);
			exchange.close();
		});
		server.start();
		try {
			final var config = S3RdmaStorageDriverTestSupport.config(
							new RdmaConfig(true, 0, false, "auto", "", "WARN", 1_000L), "",
							List.of("127.0.0.1"), server.getAddress().getPort());
			config.val("storage-driver-type", "s3-rdma");
			config.val("storage-targetBucket-provision", true);
			config.val("storage-auth-file", null);
			config.val("load-op-type", "create");
			config.val("item-type", "data");
			config.val("item-output-path", "/rdma-bucket");
			config.val("item-input-path", null);
			config.val("item-input-file", null);
			final List<Extension> extensions = Extension.load(Thread.currentThread().getContextClassLoader());

			TargetBucketProvisioning.provision("rdma-step", config, null, extensions);

			assertEquals(List.of("HEAD /rdma-bucket", "PUT /rdma-bucket"), requests);
		} finally {
			server.stop(0);
		}
	}

	@Test
	void openedControlClientIsThePlainHttpOne() throws Exception {
		final var config = S3RdmaStorageDriverTestSupport.config(
						new RdmaConfig(true, 0, false, "auto", "", "WARN", 1_000L), "", List.of("127.0.0.1"), 1);
		config.val("storage-driver-type", "s3-rdma");
		try (final var control = new S3RdmaStorageDriverExtension<>()
						.openTargetBucketControl("rdma-step", config.configVal("storage"))) {
			assertInstanceOf(S3TargetBucketControl.class, control);
			assertEquals(TargetBucketResponse.Kind.FAILED, control.check("nobody-listens", null).kind());
		}
	}
}
