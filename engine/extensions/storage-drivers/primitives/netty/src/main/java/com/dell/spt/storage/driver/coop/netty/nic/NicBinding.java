package com.dell.spt.storage.driver.coop.netty.nic;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.unix.RawUnixChannelOption;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Binds the connections of one NIC's Paths (item 9). The device binding is the product's; tests
 * that do not test binding use the source-address binding on loopback.
 */
@FunctionalInterface
public interface NicBinding {

	/** {@code SOL_SOCKET} on Linux. */
	int SOL_SOCKET = 1;

	/** {@code SO_BINDTODEVICE} on Linux. */
	int SO_BINDTODEVICE = 25;

	/**
	 * Applies the binding to the bootstrap clone of one connect. Bootstrap options are applied once
	 * per channel, before it binds and connects.
	 */
	void apply(Bootstrap connecting, String iface, Inet4Address address);

	/**
	 * {@code SO_BINDTODEVICE} to the interface, set once per socket, the IPv4 source address with an
	 * ephemeral port, and {@code IP_BIND_ADDRESS_NO_PORT}, so the kernel picks the port at connect
	 * time per destination. Requires the epoll transport.
	 */
	NicBinding DEVICE = (connecting, iface, address) -> {
		final var name = iface.getBytes(StandardCharsets.US_ASCII);
		connecting.localAddress(new InetSocketAddress(address, 0))
						.option(EpollChannelOption.IP_BIND_ADDRESS_NO_PORT, Boolean.TRUE)
						.option(new RawUnixChannelOption("SO_BINDTODEVICE", SOL_SOCKET, SO_BINDTODEVICE, name.length),
										ByteBuffer.wrap(name));
	};

	/** The source address only, for loopback tests with one address per simulated NIC. */
	NicBinding SOURCE_ADDRESS = (connecting, iface, address) -> connecting.localAddress(new InetSocketAddress(address, 0));
}
