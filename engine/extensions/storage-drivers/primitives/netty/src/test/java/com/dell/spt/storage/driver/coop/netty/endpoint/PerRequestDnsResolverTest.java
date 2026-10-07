package com.dell.spt.storage.driver.coop.netty.endpoint;

import static com.dell.spt.storage.driver.coop.netty.endpoint.ScriptedDnsServer.Transport.TCP;
import static com.dell.spt.storage.driver.coop.netty.endpoint.ScriptedDnsServer.Transport.UDP;
import static com.dell.spt.storage.driver.coop.netty.endpoint.ScriptedDnsServer.a;
import static com.dell.spt.storage.driver.coop.netty.endpoint.ScriptedDnsServer.nameRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.dell.spt.storage.driver.coop.netty.endpoint.DnsLookupException.Kind;
import com.dell.spt.storage.driver.coop.netty.endpoint.ScriptedDnsServer.Received;
import com.dell.spt.storage.driver.coop.netty.endpoint.ScriptedDnsServer.Reply;
import io.netty.handler.codec.dns.DnsRecordType;
import io.netty.handler.codec.dns.DnsResponseCode;
import io.netty.handler.codec.dns.DnsSection;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class PerRequestDnsResolverTest {

	private static final String NAME = "s3.example.test";
	private static final String QNAME = NAME + ".";
	private static final long TIMEOUT_MILLIS = 2_000;
	private static final long AWAIT_SECONDS = 10;

	private final List<AutoCloseable> resources = new ArrayList<>();

	@AfterEach
	void closeResources() throws Exception {
		for (var i = resources.size() - 1; i >= 0; i--) {
			resources.get(i).close();
		}
	}

	@Test
	void eachLookupSendsAFreshQuery() throws Exception {
		final var next = new AtomicInteger();
		final var dns = server(q -> Reply.answers(q.name(), "10.0.0." + (next.getAndIncrement() % 4 + 1)));
		final var resolver = resolver(NAME, TIMEOUT_MILLIS, dns);

		final List<String> selected = new ArrayList<>();
		for (var i = 0; i < 6; i++) {
			selected.add(await(resolver.resolve()).getHostAddress());
		}

		assertEquals(List.of("10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4", "10.0.0.1", "10.0.0.2"), selected);
		assertEquals(6, dns.count(UDP));
	}

	@Test
	void concurrentLookupsAreNotCoalesced() throws Exception {
		final var dns = server(q -> Reply.answers(q.name(), "10.0.0.1").delayed(200));
		final var resolver = resolver(NAME, TIMEOUT_MILLIS, dns);

		final List<Future<InetAddress>> lookups = new ArrayList<>();
		for (var i = 0; i < 8; i++) {
			lookups.add(resolver.resolve());
		}
		for (final var lookup : lookups) {
			await(lookup);
		}

		assertEquals(8, dns.count(UDP));
	}

	@Test
	void firstAnswerInWireOrderIsSelected() throws Exception {
		final var next = new AtomicInteger();
		final var dns = server(q -> next.getAndIncrement() == 0
						? Reply.answers(q.name(), "10.0.0.3", "10.0.0.1", "10.0.0.2")
						: Reply.answers(q.name(), "10.0.0.1", "10.0.0.2", "10.0.0.3"));
		final var resolver = resolver(NAME, TIMEOUT_MILLIS, dns);

		assertEquals("10.0.0.3", await(resolver.resolve()).getHostAddress());
		assertEquals("10.0.0.1", await(resolver.resolve()).getHostAddress());
	}

	@Test
	void hostsFileIsIgnored() throws Exception {
		final var dns = server(q -> Reply.answers(q.name(), "10.9.9.9"));
		final var resolver = resolver("localhost", TIMEOUT_MILLIS, dns);

		assertEquals("10.9.9.9", await(resolver.resolve()).getHostAddress());
		assertEquals(1, dns.count(UDP));
	}

	@Test
	void explicitServerTimeoutDoesNotFallBack() throws Exception {
		final var silent = server(q -> null);
		final var other = server(q -> Reply.answers(q.name(), "10.0.0.1"));
		final var resolver = resolver(NAME, 500, silent);

		assertEquals(Kind.TIMEOUT, awaitFailure(resolver.resolve()).kind());
		assertTrue(silent.count(UDP) >= 1);
		assertEquals(0, other.received().size());
	}

	@Test
	void serverFailureIsReportedWithoutRetryingElsewhere() throws Exception {
		final var failing = server(q -> Reply.code(DnsResponseCode.SERVFAIL));
		final var resolver = resolver(NAME, TIMEOUT_MILLIS, failing);

		assertEquals(Kind.SERVER_FAILURE, awaitFailure(resolver.resolve()).kind());
	}

	@Test
	void hostServersFailOverInConfiguredOrder() throws Exception {
		final var silent = server(q -> null);
		final var answering = server(q -> Reply.answers(q.name(), "10.0.0.7"));
		final var resolver = resolver(NAME, TIMEOUT_MILLIS, silent, answering);

		assertEquals("10.0.0.7", await(resolver.resolve()).getHostAddress());
		assertEquals("10.0.0.7", await(resolver.resolve()).getHostAddress());

		// Every lookup starts with the first configured server.
		assertEquals(2, silent.count(UDP));
		assertEquals(2, answering.count(UDP));
	}

	@Test
	void nxdomainAndNodataAreDistinguished() throws Exception {
		final var nxdomain = server(q -> Reply.code(DnsResponseCode.NXDOMAIN));
		final var nodata = server(q -> Reply.code(DnsResponseCode.NOERROR));

		assertEquals(Kind.NOT_FOUND, awaitFailure(resolver(NAME, TIMEOUT_MILLIS, nxdomain).resolve()).kind());
		assertEquals(Kind.NO_ADDRESS, awaitFailure(resolver(NAME, TIMEOUT_MILLIS, nodata).resolve()).kind());
	}

	@Test
	void cnameWithTargetInTheSameAnswerResolves() throws Exception {
		final var dns = server(q -> new Reply(DnsResponseCode.NOERROR, false, true, false, 0, List.of(
						nameRecord(DnsSection.ANSWER, q.name(), DnsRecordType.CNAME, "node.example.test."),
						a(DnsSection.ANSWER, "node.example.test.", "10.0.0.5"))));
		final var resolver = resolver(NAME, TIMEOUT_MILLIS, dns);

		assertEquals("10.0.0.5", await(resolver.resolve()).getHostAddress());
		assertEquals(1, dns.count(UDP));
	}

	@Test
	void cnameWithoutTargetIsFollowedWithAnotherQuery() throws Exception {
		final var dns = server(q -> QNAME.equals(q.name())
						? new Reply(DnsResponseCode.NOERROR, false, true, false, 0, List.of(
										nameRecord(DnsSection.ANSWER, q.name(), DnsRecordType.CNAME, "node.example.test.")))
						: Reply.answers(q.name(), "10.0.0.6"));
		final var resolver = resolver(NAME, TIMEOUT_MILLIS, dns);

		assertEquals("10.0.0.6", await(resolver.resolve()).getHostAddress());
		assertEquals(List.of(QNAME, "node.example.test."),
						dns.received().stream().map(Received::name).toList());
	}

	@Test
	void cnameLoopFailsWithinTheDeadline() throws Exception {
		final Function<Received, Reply> loop = q -> new Reply(DnsResponseCode.NOERROR, false, true, false, 0, List.of(
						nameRecord(DnsSection.ANSWER, q.name(), DnsRecordType.CNAME,
										QNAME.equals(q.name()) ? "loop.example.test." : QNAME)));
		final var dns = server(loop);
		final var resolver = resolver(NAME, TIMEOUT_MILLIS, dns);

		final var started = System.nanoTime();
		awaitFailure(resolver.resolve());
		assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) <= TIMEOUT_MILLIS + 500);
	}

	@Test
	void truncatedUdpAnswerIsRetriedOverTcp() throws Exception {
		final var dns = server(q -> q.transport() == UDP
						? new Reply(DnsResponseCode.NOERROR, false, true, true, 0, List.of())
						: Reply.answers(q.name(), "10.0.0.8"));
		final var resolver = resolver(NAME, TIMEOUT_MILLIS, dns);

		assertEquals("10.0.0.8", await(resolver.resolve()).getHostAddress());
		assertEquals(1, dns.count(UDP));
		assertEquals(1, dns.count(TCP));
	}

	@Test
	void authoritativeLoadBalancerAnswersAreUsedAsGiven() throws Exception {
		// Shape of a DNS load-balancing service: authoritative, no recursion, one A record with TTL 0
		// per response rotating across nodes, plus NS authority and glue records.
		final var next = new AtomicInteger();
		final var dns = server(q -> new Reply(DnsResponseCode.NOERROR, true, false, false, 0, List.of(
						a(DnsSection.ANSWER, q.name(), "10.1.0." + (next.getAndIncrement() % 12 + 40)),
						nameRecord(DnsSection.AUTHORITY, q.name(), DnsRecordType.NS, "ns.example.test."),
						a(DnsSection.ADDITIONAL, "ns.example.test.", "10.1.0.30"))));
		final var resolver = resolver(NAME, TIMEOUT_MILLIS, dns);

		final List<String> selected = new ArrayList<>();
		for (var i = 0; i < 12; i++) {
			selected.add(await(resolver.resolve()).getHostAddress());
		}

		final List<String> expected = new ArrayList<>();
		for (var i = 40; i < 52; i++) {
			expected.add("10.1.0." + i);
		}
		assertEquals(expected, selected);
		assertEquals(12, dns.received().size());
	}

	@Test
	void totalDeadlineBoundsALookup() throws Exception {
		final var silent = server(q -> null);
		final var resolver = resolver(NAME, 300, silent);

		final var started = System.nanoTime();
		final var failure = awaitFailure(resolver.resolve());
		final var elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

		assertEquals(Kind.TIMEOUT, failure.kind());
		assertTrue(elapsed >= 250 && elapsed <= 1_500, "elapsed " + elapsed + " ms");
	}

	@Test
	void closeTerminatesTheResolverAndRejectsNewLookups() throws Exception {
		final var dns = server(q -> Reply.answers(q.name(), "10.0.0.1"));
		final var resolver = new PerRequestDnsResolver(NAME, List.of(dns.address()), TIMEOUT_MILLIS,
						new DefaultThreadFactory("test-dns", true), null);
		await(resolver.resolve());

		resolver.close();

		assertTrue(resolver.isTerminated());
		final var afterClose = resolver.resolve();
		assertTrue(afterClose.awaitUninterruptibly(AWAIT_SECONDS, TimeUnit.SECONDS));
		assertFalse(afterClose.isSuccess());
	}

	@Test
	void rejectsInvalidConstruction() {
		final var threads = new DefaultThreadFactory("test-dns", true);
		assertThrows(IllegalArgumentException.class, () -> new PerRequestDnsResolver(NAME, List.of(), 1, threads, null));
	}

	private ScriptedDnsServer server(final Function<Received, Reply> script) throws InterruptedException {
		final var server = new ScriptedDnsServer(script);
		resources.add(server);
		return server;
	}

	private PerRequestDnsResolver resolver(final String name, final long timeoutMillis,
					final ScriptedDnsServer... servers) {
		final var resolver = new PerRequestDnsResolver(
						name,
						Arrays.stream(servers).map(ScriptedDnsServer::address).toList(),
						timeoutMillis,
						new DefaultThreadFactory("test-dns", true),
						null);
		resources.add(resolver);
		return resolver;
	}

	private static InetAddress await(final Future<InetAddress> lookup) {
		assertTrue(lookup.awaitUninterruptibly(AWAIT_SECONDS, TimeUnit.SECONDS), "lookup did not settle");
		if (!lookup.isSuccess()) {
			fail("lookup failed", lookup.cause());
		}
		return lookup.getNow();
	}

	private static DnsLookupException awaitFailure(final Future<InetAddress> lookup) {
		assertTrue(lookup.awaitUninterruptibly(AWAIT_SECONDS, TimeUnit.SECONDS), "lookup did not settle");
		assertFalse(lookup.isSuccess(), "lookup unexpectedly succeeded");
		return assertInstanceOf(DnsLookupException.class, lookup.cause());
	}
}
