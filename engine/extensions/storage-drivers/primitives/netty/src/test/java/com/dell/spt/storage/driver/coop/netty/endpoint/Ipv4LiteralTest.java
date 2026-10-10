package com.dell.spt.storage.driver.coop.netty.endpoint;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class Ipv4LiteralTest {

	private static final List<String> NOT_IPV4 = Arrays.asList(
					null, "", "localhost", "s3.example.test", "10.0.0", "10.0.0.1.2", "10.0.0.256", "010.0.0.1",
					"10.0.0.-1", "10..0.1", "1e1.0.0.1", "::1", "10.0.0.1 ", "0x0a.0.0.1", "1234.0.0.1");

	@Test
	void parsesDottedQuads() {
		assertArrayEquals(octets(10, 0, 0, 1), Ipv4Literal.parse("10.0.0.1").orElseThrow().getAddress());
		assertArrayEquals(octets(0, 0, 0, 0), Ipv4Literal.parse("0.0.0.0").orElseThrow().getAddress());
		assertArrayEquals(octets(255, 255, 255, 255), Ipv4Literal.parse("255.255.255.255").orElseThrow().getAddress());
	}

	@Test
	void rejectsEverythingElseWithoutLookup() {
		for (final var text : NOT_IPV4) {
			assertTrue(Ipv4Literal.parse(text).isEmpty(), String.valueOf(text));
		}
	}

	private static byte[] octets(final int... values) {
		final var bytes = new byte[values.length];
		for (var i = 0; i < values.length; i++) {
			bytes[i] = (byte) values[i];
		}
		return bytes;
	}
}
