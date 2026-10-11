package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Real direct-memory accounting, which an in-process test cannot bound: a child JVM with a 48 MiB
 * direct-memory limit holds 24 MiB of idle pool buffers and then needs a 32 MiB per-operation
 * buffer, which fits without the pool.
 */
class RdmaDirectMemoryCanaryTest {

	private static final String DIRECT_LIMIT = "-XX:MaxDirectMemorySize=48m";
	private static final String BUDGET_MIB = "24";
	private static final String REQUEST_MIB = "32";
	private static final long TIMEOUT_SECONDS = 60;

	@Test
	void idlePoolBuffersBlockAPlainAllocation() throws Exception {
		// Control: proves the scenario actually exhausts direct memory.
		assertEquals("oom", run("plain"));
	}

	@Test
	void unpooledAllocationReclaimsIdlePoolBuffers() throws Exception {
		assertEquals("allocated idle=0", run("reclaim"));
	}

	private static String run(final String mode) throws IOException, InterruptedException {
		final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
		final Process process = new ProcessBuilder(java, DIRECT_LIMIT, "-cp", System.getProperty("java.class.path"),
						RdmaDirectMemoryCanary.class.getName(), mode, BUDGET_MIB, REQUEST_MIB)
						.redirectErrorStream(true)
						.start();
		final String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
		assertTrue(process.waitFor(TIMEOUT_SECONDS, TimeUnit.SECONDS), "canary timed out: " + output);
		assertEquals(0, process.exitValue(), output);
		// Log lines may carry terminal escape codes, so the marker is searched for, not anchored.
		return output.lines()
						.filter(line -> line.contains(RdmaDirectMemoryCanary.RESULT_PREFIX))
						.map(line -> line.substring(line.indexOf(RdmaDirectMemoryCanary.RESULT_PREFIX)
										+ RdmaDirectMemoryCanary.RESULT_PREFIX.length()))
						.reduce((first, last) -> last)
						.orElseThrow(() -> new AssertionError("no result: " + output));
	}
}
