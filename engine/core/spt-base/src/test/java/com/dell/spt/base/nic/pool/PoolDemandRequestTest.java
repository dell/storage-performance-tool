package com.dell.spt.base.nic.pool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The estimate as the image command serves it, against the fixtures the CLI's tests share. */
final class PoolDemandRequestTest {

	private static final Path FIXTURES = Path.of("../../../test-fixtures/pool-demand");
	private static final ObjectMapper MAPPER = new ObjectMapper();

	private static String base64(final String json) {
		return Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
	}

	@Test
	void theSharedRequestGivesTheSharedResponse() throws Exception {
		final var request = Files.readString(FIXTURES.resolve("multipart-seed-request.json"));
		final var node = MAPPER.createObjectNode();
		PoolDemandRequest.write(node, PoolDemandRequest.parse(base64(request)), List.of("nic-a", "nic-b", "nic-c", "nic-d"));
		final var want = MAPPER.readTree(FIXTURES.resolve("multipart-seed-response.json").toFile());
		assertEquals(want, MAPPER.readTree(node.toString()), () -> "got " + node.toPrettyString());
	}

	@Test
	void aRequestThatIsNotBase64JsonIsAUsageError() {
		assertThrows(IllegalArgumentException.class, () -> PoolDemandRequest.parse("not base64!"));
		assertThrows(IllegalArgumentException.class, () -> PoolDemandRequest.parse(base64("{not json")));
		assertThrows(IllegalArgumentException.class,
						() -> PoolDemandRequest.parse(base64("{\"steps\":[{\"drivers\":[{\"operations\":[{\"kind\":\"copy\"}]}]}]}")));
	}

	@Test
	void capacityReadsACgroupV1MemoryController(@TempDir final Path root) throws Exception {
		write(root, "proc/meminfo", "MemTotal:       1048576 kB\n");
		write(root, "proc/self/cgroup", "12:cpu,cpuacct:/docker/id\n4:memory:/docker/id\n");
		final var unlimited = MemoryCapacity.read(root);
		assertEquals(MemoryCapacity.SOURCE_MEM_TOTAL, unlimited.source(), "no file names a limit");

		write(root, "sys/fs/cgroup/memory/memory.limit_in_bytes", "9223372036854771712\n");
		assertEquals(MemoryCapacity.SOURCE_MEM_TOTAL, MemoryCapacity.read(root).source(), "v1's value for no limit");

		write(root, "sys/fs/cgroup/memory/memory.limit_in_bytes", "536870912\n");
		final var mounted = MemoryCapacity.read(root);
		assertEquals(1L << 29, mounted.bytes(), "a container's mount is its own cgroup");
		assertEquals(MemoryCapacity.SOURCE_CGROUP, mounted.source());

		write(root, "sys/fs/cgroup/memory/docker/memory.limit_in_bytes", "268435456\n");
		assertEquals(1L << 28, MemoryCapacity.read(root).bytes(), "an ancestor the host's view shows");

		write(root, "proc/self/cgroup", "0::/\n4:memory:/docker/id\n");
		assertEquals(1L << 28, MemoryCapacity.read(root).bytes(), "beside an unified hierarchy without memory");
	}

	@Test
	void capacityIsTheSmallerOfMemTotalAndTheCgroupChainAndNamesItsSource(@TempDir final Path root) throws Exception {
		write(root, "proc/meminfo", "MemTotal:       1048576 kB\nMemFree: 10 kB\n");
		write(root, "proc/self/cgroup", "0::/lxc/payload/docker.scope\n");
		final var noLimit = MemoryCapacity.read(root);
		assertEquals(1L << 30, noLimit.bytes());
		assertEquals(MemoryCapacity.SOURCE_MEM_TOTAL, noLimit.source());

		write(root, "sys/fs/cgroup/lxc/payload/docker.scope/memory.max", "max\n");
		assertEquals(MemoryCapacity.SOURCE_MEM_TOTAL, MemoryCapacity.read(root).source(), "a chain of max sets nothing");

		write(root, "sys/fs/cgroup/lxc/payload/memory.max", "536870912\n");
		final var ancestor = MemoryCapacity.read(root);
		assertEquals(1L << 29, ancestor.bytes(), "a limit on an ancestor only");
		assertEquals(MemoryCapacity.SOURCE_CGROUP, ancestor.source());
		assertEquals("/lxc/payload", ancestor.cgroup());

		write(root, "sys/fs/cgroup/lxc/payload/docker.scope/memory.max", "268435456\n");
		final var own = MemoryCapacity.read(root);
		assertEquals(1L << 28, own.bytes(), "a limit on the engine's own cgroup");
		assertEquals("/lxc/payload/docker.scope", own.cgroup());

		write(root, "sys/fs/cgroup/lxc/memory.max", "1073741824\n");
		assertEquals(1L << 28, MemoryCapacity.read(root).bytes(), "the smallest on the chain");

		write(root, "sys/fs/cgroup/lxc/payload/docker.scope/memory.max", "2147483648\n");
		write(root, "sys/fs/cgroup/lxc/payload/memory.max", "2147483648\n");
		write(root, "sys/fs/cgroup/lxc/memory.max", "max\n");
		final var above = MemoryCapacity.read(root);
		assertEquals(1L << 30, above.bytes(), "a limit above the host's memory does not bind");
		assertEquals(MemoryCapacity.SOURCE_MEM_TOTAL, above.source());
	}

	@Test
	void capacityReportsWhatItCouldNotRead(@TempDir final Path root) {
		final var capacity = MemoryCapacity.read(root);
		assertEquals(-1, capacity.bytes());
		assertTrue(capacity.note().contains("MemTotal"), capacity.note());
		assertTrue(capacity.note().contains("cgroup"), capacity.note());
	}

	private static void write(final Path root, final String path, final String content) throws Exception {
		final var file = root.resolve(path);
		Files.createDirectories(file.getParent());
		Files.writeString(file, content);
	}
}
