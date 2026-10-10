package com.dell.spt.storage.driver.coop.netty.endpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HostDnsServersTest {

	@TempDir
	Path dir;

	@Test
	void readsIpv4NameServersInFileOrder() throws IOException {
		final var conf = write("""
						# generated
						search corp.example.test
						nameserver 10.0.0.2
						nameserver fd7a:115c::53
						  nameserver   10.0.0.1  # trailing comment
						options rotate
						""");

		final var discovery = HostDnsServers.read(conf);

		assertEquals(List.of(new InetSocketAddress("10.0.0.2", 53), new InetSocketAddress("10.0.0.1", 53)),
						discovery.servers());
		assertEquals(List.of("fd7a:115c::53"), discovery.ignored());
	}

	@Test
	void failsWithoutUsableIpv4NameServer() throws IOException {
		final var conf = write("nameserver ::1\nsearch example.test\n");

		final var failure = assertThrows(IOException.class, () -> HostDnsServers.read(conf));

		assertTrue(failure.getMessage().contains("::1"));
		assertTrue(failure.getMessage().contains("explicit DNS server"));
	}

	private Path write(final String content) throws IOException {
		final var conf = dir.resolve("resolv.conf");
		Files.writeString(conf, content);
		return conf;
	}
}
