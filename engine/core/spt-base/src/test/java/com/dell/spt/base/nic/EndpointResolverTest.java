package com.dell.spt.base.nic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.dell.spt.base.nic.EndpointResolver.HostPort;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class EndpointResolverTest {

	@ParameterizedTest(name = "{0}")
	@CsvSource(nullValues = "-", value = {
			"192.0.2.101:9000, 192.0.2.101, 9000",
			"192.0.2.101, 192.0.2.101, -",
			"s3a.example:9020, s3a.example, 9020",
			"' s3a.example ', s3a.example, -",
			"[2001:db8::1]:9000, 2001:db8::1, 9000",
			"[2001:db8::1], 2001:db8::1, -",
			"2001:db8::1, 2001:db8::1, -",
			"s3a.example:http, s3a.example, -",
	})
	void hostAndPort(final String endpoint, final String host, final Integer port) {
		assertEquals(new HostPort(host, port), HostPort.parse(endpoint));
	}

	@Test
	void literalsResolveWithoutALookup() {
		assertEquals(Ipv4.parse("192.0.2.101"), EndpointResolver.SYSTEM.resolveIpv4("192.0.2.101"));
		assertNull(EndpointResolver.SYSTEM.resolveIpv4("::1"));
	}

	@Test
	void ipv4LiteralsAndTheirPrefixes() {
		assertNull(Ipv4.parse("192.0.2"));
		assertNull(Ipv4.parse("192.0.2.256"));
		assertNull(Ipv4.parse("192.0.2.-1"));
		assertNull(Ipv4.parse("fe80::1"));
		assertEquals("203.0.113.255", Ipv4.format(Ipv4.parse("203.0.113.255")));
		final var prefix = Ipv4Prefix.parse("192.0.2.11/25");
		assertEquals("192.0.2.11/25", prefix.toString());
		assertEquals(true, prefix.contains(Ipv4.parse("192.0.2.127")));
		assertEquals(false, prefix.contains(Ipv4.parse("192.0.2.128")));
		assertEquals(true, Ipv4Prefix.parse("0.0.0.0/0").contains(Ipv4.parse("198.51.100.1")));
	}
}
