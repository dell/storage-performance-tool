package com.dell.spt.storage.driver.coop.netty.endpoint;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.dns.DatagramDnsQuery;
import io.netty.handler.codec.dns.DatagramDnsQueryDecoder;
import io.netty.handler.codec.dns.DatagramDnsResponse;
import io.netty.handler.codec.dns.DatagramDnsResponseEncoder;
import io.netty.handler.codec.dns.DefaultDnsQuestion;
import io.netty.handler.codec.dns.DefaultDnsRawRecord;
import io.netty.handler.codec.dns.DefaultDnsResponse;
import io.netty.handler.codec.dns.DnsOpCode;
import io.netty.handler.codec.dns.DnsQuery;
import io.netty.handler.codec.dns.DnsQuestion;
import io.netty.handler.codec.dns.DnsRecordType;
import io.netty.handler.codec.dns.DnsResponse;
import io.netty.handler.codec.dns.DnsResponseCode;
import io.netty.handler.codec.dns.DnsSection;
import io.netty.handler.codec.dns.TcpDnsQueryDecoder;
import io.netty.handler.codec.dns.TcpDnsResponseEncoder;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Loopback UDP and TCP DNS server on one port whose answers come from a script. It records every
 * query it receives, which makes it the test oracle for query counts, order and transport.
 */
final class ScriptedDnsServer implements AutoCloseable {

	enum Transport {
		UDP, TCP
	}

	record Received(Transport transport, String name, DnsRecordType type) {}

	/** One record in a reply; {@code data} is already in wire form. */
	record Rr(DnsSection section, String name, DnsRecordType type, long ttl, byte[] data) {}

	/** A scripted reply; {@code null} from the script means "drop the query". */
	record Reply(DnsResponseCode code, boolean authoritative, boolean recursionAvailable, boolean truncated,
					long delayMillis, List<Rr> records) {

		static Reply answers(final String name, final String... ipv4) {
			final List<Rr> records = new ArrayList<>();
			for (final var ip : ipv4) {
				records.add(a(DnsSection.ANSWER, name, ip));
			}
			return new Reply(DnsResponseCode.NOERROR, false, true, false, 0, records);
		}

		static Reply code(final DnsResponseCode code) {
			return new Reply(code, false, true, false, 0, List.of());
		}

		Reply delayed(final long millis) {
			return new Reply(code, authoritative, recursionAvailable, truncated, millis, records);
		}
	}

	private static final int BIND_ATTEMPTS = 16;

	private final NioEventLoopGroup group = new NioEventLoopGroup(1);
	private final List<Received> received = new CopyOnWriteArrayList<>();
	private final Function<Received, Reply> script;
	private final Channel udp;
	private final Channel tcp;

	ScriptedDnsServer(final Function<Received, Reply> script) throws InterruptedException {
		this.script = script;
		Channel boundUdp = null;
		Channel boundTcp = null;
		for (var attempt = 0; attempt < BIND_ATTEMPTS && boundTcp == null; attempt++) {
			boundUdp = bindUdp();
			try {
				boundTcp = bindTcp(((InetSocketAddress) boundUdp.localAddress()).getPort());
			} catch (final Exception e) {
				if (!(e instanceof BindException)) {
					throw e;
				}
				boundUdp.close().sync();
			}
		}
		if (boundTcp == null) {
			group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS);
			throw new IllegalStateException("No free UDP+TCP port pair for the scripted DNS server");
		}
		this.udp = boundUdp;
		this.tcp = boundTcp;
	}

	InetSocketAddress address() {
		return (InetSocketAddress) udp.localAddress();
	}

	List<Received> received() {
		return List.copyOf(received);
	}

	long count(final Transport transport) {
		return received.stream().filter(r -> r.transport() == transport).count();
	}

	static Rr a(final DnsSection section, final String name, final String ip) {
		final var address = Ipv4Literal.parse(ip).orElseThrow();
		return new Rr(section, name, DnsRecordType.A, 0, address.getAddress());
	}

	static Rr nameRecord(final DnsSection section, final String name, final DnsRecordType type, final String target) {
		return new Rr(section, name, type, 0, wireName(target));
	}

	static byte[] wireName(final String name) {
		final var out = new java.io.ByteArrayOutputStream();
		for (final var label : name.split("\\.")) {
			if (label.isEmpty()) {
				continue;
			}
			final var bytes = label.getBytes(StandardCharsets.US_ASCII);
			out.write(bytes.length);
			out.writeBytes(bytes);
		}
		out.write(0);
		return out.toByteArray();
	}

	private Channel bindUdp() throws InterruptedException {
		return new Bootstrap()
						.group(group)
						.channel(NioDatagramChannel.class)
						.handler(new ChannelInitializer<DatagramChannel>() {
							@Override
							protected void initChannel(final DatagramChannel ch) {
								ch.pipeline().addLast(new DatagramDnsQueryDecoder(), new DatagramDnsResponseEncoder(),
												new SimpleChannelInboundHandler<DatagramDnsQuery>() {
													@Override
													protected void channelRead0(final ChannelHandlerContext ctx, final DatagramDnsQuery query) {
														final DnsQuestion question = query.recordAt(DnsSection.QUESTION);
														final var response = new DatagramDnsResponse(
																		query.recipient(), query.sender(), query.id(), DnsOpCode.QUERY);
														respond(ctx, Transport.UDP, question, response);
													}
												});
							}
						})
						.bind(new InetSocketAddress("127.0.0.1", 0))
						.sync()
						.channel();
	}

	private Channel bindTcp(final int port) throws InterruptedException {
		return new ServerBootstrap()
						.group(group)
						.channel(NioServerSocketChannel.class)
						.childHandler(new ChannelInitializer<SocketChannel>() {
							@Override
							protected void initChannel(final SocketChannel ch) {
								ch.pipeline().addLast(new TcpDnsQueryDecoder(), new TcpDnsResponseEncoder(),
												new SimpleChannelInboundHandler<DnsQuery>() {
													@Override
													protected void channelRead0(final ChannelHandlerContext ctx, final DnsQuery query) {
														final DnsQuestion question = query.recordAt(DnsSection.QUESTION);
														final var response = new DefaultDnsResponse(query.id(), DnsOpCode.QUERY);
														respond(ctx, Transport.TCP, question, response);
													}
												});
							}
						})
						.bind(new InetSocketAddress("127.0.0.1", port))
						.sync()
						.channel();
	}

	private void respond(final ChannelHandlerContext ctx, final Transport transport, final DnsQuestion question,
					final DnsResponse response) {
		final var request = new Received(transport, question.name(), question.type());
		received.add(request);
		final var reply = script.apply(request);
		if (reply == null) {
			response.release();
			return;
		}
		response.setCode(reply.code());
		response.setAuthoritativeAnswer(reply.authoritative());
		response.setRecursionAvailable(reply.recursionAvailable());
		response.setRecursionDesired(true);
		response.setTruncated(reply.truncated());
		response.addRecord(DnsSection.QUESTION, new DefaultDnsQuestion(question.name(), question.type()));
		for (final var rr : reply.records()) {
			final ByteBuf data = Unpooled.wrappedBuffer(rr.data());
			response.addRecord(rr.section(), new DefaultDnsRawRecord(rr.name(), rr.type(), rr.ttl(), data));
		}
		if (reply.delayMillis() > 0) {
			ctx.executor().schedule(() -> ctx.writeAndFlush(response), reply.delayMillis(), TimeUnit.MILLISECONDS);
		} else {
			ctx.writeAndFlush(response);
		}
	}

	@Override
	public void close() {
		udp.close().syncUninterruptibly();
		tcp.close().syncUninterruptibly();
		group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).syncUninterruptibly();
	}
}
