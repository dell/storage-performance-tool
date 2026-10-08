package com.dell.spt.storage.driver.coop.netty.http.s3;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.logging.LogContextThreadFactory;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.storage.driver.coop.netty.endpoint.DestinationSource;
import com.dell.spt.storage.driver.coop.netty.endpoint.DnsDestinations;
import com.dell.spt.storage.driver.coop.netty.endpoint.EndpointSelectionCounters;
import com.dell.spt.storage.driver.coop.netty.endpoint.EndpointSelectionSettings;
import com.dell.spt.storage.driver.coop.netty.endpoint.EndpointSelectionSettings.Mode;
import com.dell.spt.storage.driver.coop.netty.endpoint.HostDnsServers;
import com.dell.spt.storage.driver.coop.netty.endpoint.PerRequestDnsResolver;
import com.dell.spt.storage.driver.coop.netty.endpoint.RoundRobinDestinations;
import com.dell.spt.storage.driver.coop.netty.endpoint.SelectingConnectionPool;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.ssl.SslHandler;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SNIHostName;
import org.apache.logging.log4j.Level;

/**
 * Endpoint selection state shared by the object and Partial-object Read drivers: the settings and
 * discovered DNS servers, pool binding, the Logical Authority for Host and signing, TLS SNI, the
 * counters, and the driver's start, setup-failure and summary logging.
 */
final class EndpointSelectionSupport {

	private final String stepId;
	private final EndpointSelectionSettings settings;
	private final String logicalHostname;
	private final List<InetSocketAddress> dnsServers;
	private final boolean hostConfiguredDns;
	private final EndpointSelectionCounters counters = new EndpointSelectionCounters();
	private final AtomicBoolean setupFailureWarned = new AtomicBoolean();

	/**
	 * Creates the support for a non-default mode. DNS server discovery happens first, so a host
	 * without a usable resolver configuration fails before any network resource exists.
	 */
	static EndpointSelectionSupport create(final String stepId, final EndpointSelectionSettings settings,
					final Path resolvConf) throws IllegalConfigurationException {
		final List<InetSocketAddress> dnsServers = settings.mode() == Mode.PER_REQUEST_DNS
						? dnsServers(settings, resolvConf)
						: List.of();
		return new EndpointSelectionSupport(stepId, settings, dnsServers);
	}

	private static List<InetSocketAddress> dnsServers(final EndpointSelectionSettings settings, final Path resolvConf) {
		if (settings.dnsServer().isPresent()) {
			return List.of(settings.dnsServer().get());
		}
		try {
			return HostDnsServers.read(resolvConf).servers();
		} catch (final IOException e) {
			throw new IllegalConfigurationException("Per-request DNS cannot use the host DNS configuration: "
							+ e.getMessage(), e);
		}
	}

	private EndpointSelectionSupport(final String stepId, final EndpointSelectionSettings settings,
					final List<InetSocketAddress> dnsServers) {
		this.stepId = stepId;
		this.settings = settings;
		this.logicalHostname = settings.hostname().orElse(null);
		this.dnsServers = dnsServers;
		this.hostConfiguredDns = settings.mode() == Mode.PER_REQUEST_DNS && settings.dnsServer().isEmpty();
	}

	/**
	 * Binds the driver's pool to the mode once the driver's own fields exist, and returns the Host
	 * for helper requests, or {@code null} to keep the first endpoint. Per-request DNS also makes
	 * every request carry {@code Connection: close}.
	 */
	String bind(final SelectingConnectionPool pool, final int concurrencyLimit, final HttpHeaders sharedHeaders) {
		final DestinationSource destinations;
		final boolean pooled;
		final int idleLimit;
		final int firstPort;
		if (settings.mode() == Mode.ROUND_ROBIN) {
			destinations = new RoundRobinDestinations(settings.destinations());
			pooled = true;
			// Each destination may keep one connection beyond the in-flight limit, so strict rotation
			// can still reuse connections at low concurrency.
			idleLimit = concurrencyLimit > 0
							? (int) Math.min(Integer.MAX_VALUE, (long) concurrencyLimit + settings.destinations().size())
							: Integer.MAX_VALUE;
			firstPort = settings.destinations().get(0).getPort();
		} else {
			final var resolver = new PerRequestDnsResolver(logicalHostname, dnsServers, settings.dnsTimeoutMillis(),
							new LogContextThreadFactory("dnsResolver", true));
			destinations = new DnsDestinations(resolver, settings.port(), counters);
			pooled = false;
			idleLimit = 0;
			firstPort = settings.port();
			sharedHeaders.set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
		}
		pool.bind(destinations, pooled, idleLimit, settings.connectTimeoutMillis(),
						(long) settings.connectTimeoutMillis() + settings.dnsTimeoutMillis(), counters);
		if (settings.mode() == Mode.ROUND_ROBIN) {
			Loggers.MSG.info("{}: endpoint selection round-robin over {} destination(s), logical hostname: {}",
							stepId, settings.destinations().size(),
							logicalHostname == null ? "none (Host is the selected address)" : logicalHostname);
		} else {
			Loggers.MSG.info("{}: endpoint selection per-request-dns for {}:{} using {} DNS server(s) {}",
							stepId, logicalHostname, firstPort, hostConfiguredDns ? "host-configured" : "explicit",
							dnsServers);
		}
		return logicalHostname == null ? null : logicalHostname + ":" + firstPort;
	}

	/** True when every connection serves one request and is then closed. */
	boolean perRequestConnections() {
		return settings.mode() == Mode.PER_REQUEST_DNS;
	}

	/** Logical hostname with the selected port, or the selected {@code ip:port} without one. */
	String authority(final String nodeAddr) {
		if (logicalHostname == null || nodeAddr == null) {
			return nodeAddr;
		}
		return logicalHostname + nodeAddr.substring(nodeAddr.lastIndexOf(':'));
	}

	/** Adds the logical hostname as TLS SNI; call after the driver's handlers are in place. */
	void applySni(final Channel channel) {
		final var ssl = logicalHostname == null ? null : channel.pipeline().get(SslHandler.class);
		if (ssl != null) {
			final var engine = ssl.engine();
			final var parameters = engine.getSSLParameters();
			parameters.setServerNames(List.of(new SNIHostName(logicalHostname)));
			engine.setSSLParameters(parameters);
		}
	}

	/** Logs the host-DNS caching warning once per started driver. */
	void warnIfHostConfiguredDns() {
		if (hostConfiguredDns) {
			Loggers.MSG.warn("{}: Per-request DNS is using this worker's host DNS configuration {}. Resolver "
							+ "caching may prevent each lookup from reaching the DNS load-balancing service; "
							+ "configure a DNS server for direct queries.", stepId, dnsServers);
		}
	}

	void logSummary() {
		Loggers.MSG.info("{}: endpoint selection {} summary: {}", stepId, settings.mode().configValue(),
						counters.summary());
	}

	/** The first setup failure is a warning; later ones are logged at DEBUG. */
	void logSetupFailure(final Throwable failure) {
		if (setupFailureWarned.compareAndSet(false, true)) {
			LogUtil.exception(Level.WARN, failure,
							"Endpoint selection failed to set up a request; further failures are logged at DEBUG");
		} else {
			LogUtil.exception(Level.DEBUG, failure, "Endpoint selection failed to set up a request");
		}
	}

	EndpointSelectionSettings settings() {
		return settings;
	}

	List<InetSocketAddress> dnsServers() {
		return dnsServers;
	}

	EndpointSelectionCounters counters() {
		return counters;
	}

	boolean hostConfiguredDns() {
		return hostConfiguredDns;
	}
}
