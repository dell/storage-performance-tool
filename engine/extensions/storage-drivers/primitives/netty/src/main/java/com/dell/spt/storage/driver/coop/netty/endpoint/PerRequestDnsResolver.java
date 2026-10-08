package com.dell.spt.storage.driver.coop.netty.endpoint;

import io.netty.channel.EventLoop;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.resolver.ResolvedAddressTypes;
import io.netty.resolver.dns.DnsNameResolver;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import io.netty.resolver.dns.NoopAuthoritativeDnsServerCache;
import io.netty.resolver.dns.NoopDnsCache;
import io.netty.resolver.dns.NoopDnsCnameCache;
import io.netty.resolver.dns.SequentialDnsServerAddressStreamProvider;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Resolves one hostname afresh on every call. The resolver keeps no positive, negative, CNAME or
 * authoritative-server cache, ignores the hosts file and search domains, never coalesces
 * concurrent lookups, and completes with the first IPv4 address in answer order. Servers are
 * queried in the given order, starting with the first on every lookup; a truncated UDP answer is
 * retried over TCP with the same server. Each lookup has a total deadline, and the per-query
 * timeout divides it across the servers so ordered failover fits inside that deadline.
 */
public final class PerRequestDnsResolver implements AutoCloseable {

	private final String hostname;
	private final List<InetSocketAddress> servers;
	private final long timeoutMillis;
	private final NioEventLoopGroup group;
	private final EventLoop loop;
	private final DnsNameResolver resolver;
	private final Set<Promise<InetAddress>> outstanding = ConcurrentHashMap.newKeySet();
	private volatile boolean closed;

	public PerRequestDnsResolver(
					final String hostname,
					final List<InetSocketAddress> servers,
					final long timeoutMillis,
					final ThreadFactory threadFactory) {
		if (servers.isEmpty()) {
			throw new IllegalArgumentException("At least one DNS server is required");
		}
		if (timeoutMillis < 1) {
			throw new IllegalArgumentException("DNS lookup timeout must be positive: " + timeoutMillis);
		}
		this.hostname = hostname;
		this.servers = List.copyOf(servers);
		this.timeoutMillis = timeoutMillis;
		this.group = new NioEventLoopGroup(1, threadFactory);
		this.loop = group.next();
		try {
			this.resolver = new DnsNameResolverBuilder(loop)
							.datagramChannelType(NioDatagramChannel.class)
							// TCP is used only after a truncated UDP answer, never as a retry on timeout.
							.socketChannelType(NioSocketChannel.class, false)
							.nameServerProvider(new SequentialDnsServerAddressStreamProvider(this.servers))
							.resolveCache(NoopDnsCache.INSTANCE)
							.cnameCache(NoopDnsCnameCache.INSTANCE)
							.authoritativeDnsServerCache(NoopAuthoritativeDnsServerCache.INSTANCE)
							.consolidateCacheSize(0)
							.hostsFileEntriesResolver((name, types) -> null)
							.resolvedAddressTypes(ResolvedAddressTypes.IPV4_ONLY)
							.searchDomains(List.of())
							.queryTimeoutMillis(Math.max(1, timeoutMillis / this.servers.size()))
							.build();
		} catch (final RuntimeException e) {
			group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS);
			throw e;
		}
	}

	public String hostname() {
		return hostname;
	}

	public List<InetSocketAddress> servers() {
		return servers;
	}

	/**
	 * Starts a fresh lookup. The returned future completes with the first IPv4 answer, or fails with
	 * a {@link DnsLookupException} no later than the total deadline, or when the resolver closes.
	 */
	public Future<InetAddress> resolve() {
		if (closed) {
			return GlobalEventExecutor.INSTANCE.newFailedFuture(
							new DnsLookupException(DnsLookupException.Kind.CANCELLED, hostname, null));
		}
		final Promise<InetAddress> result;
		try {
			result = loop.newPromise();
			outstanding.add(result);
			result.addListener(done -> outstanding.remove(result));
			final var lookup = resolver.resolve(hostname);
			final var deadline = loop.schedule(
							() -> {
								if (result.tryFailure(new DnsLookupException(
												DnsLookupException.Kind.TIMEOUT, hostname, null))) {
									lookup.cancel(false);
								}
							},
							timeoutMillis,
							TimeUnit.MILLISECONDS);
			lookup.addListener((Future<InetAddress> done) -> {
				deadline.cancel(false);
				if (done.isSuccess()) {
					result.trySuccess(done.getNow());
				} else {
					result.tryFailure(DnsLookupException.classify(hostname, done.cause()));
				}
			});
		} catch (final RejectedExecutionException e) {
			return GlobalEventExecutor.INSTANCE.newFailedFuture(
							new DnsLookupException(DnsLookupException.Kind.CANCELLED, hostname, e));
		}
		return result;
	}

	/**
	 * Fails every outstanding lookup, closes the resolver and waits, within a bound, for its event
	 * loop to terminate. Lookup failures are delivered on that loop before it terminates.
	 */
	@Override
	public void close() {
		closed = true;
		for (final var lookup : new ArrayList<>(outstanding)) {
			lookup.tryFailure(new DnsLookupException(DnsLookupException.Kind.CANCELLED, hostname, null));
		}
		try {
			resolver.close();
		} finally {
			group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS)
							.awaitUninterruptibly(EndpointSelectionConstants.CLOSE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
		}
	}

	boolean isTerminated() {
		return group.isTerminated();
	}
}
