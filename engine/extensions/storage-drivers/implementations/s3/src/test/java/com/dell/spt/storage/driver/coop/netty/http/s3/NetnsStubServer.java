package com.dell.spt.storage.driver.coop.netty.http.s3;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The S3 stub of the network-namespace test, run by {@link NetnsTopology} as its own process in the
 * server namespace, with only the JDK on its class path. It serves one listener per
 * {@code address:port} argument and reads one command per line from stdin, answering each with
 * {@code ok} or {@code error <reason>}:
 *
 * <ul>
 *   <li>{@code listen}: starts every listener;
 *   <li>{@code refuse <i>}: stops listener {@code i} and closes its connections, so connects to it
 *       are refused;
 *   <li>{@code serve <i>}: starts listener {@code i} again;
 *   <li>{@code sources}: prints {@code sources <i> <address,...>} per listener, the client addresses
 *       of the PUTs it served.
 * </ul>
 *
 * <p>It prints {@code started} once running and exits when stdin closes.
 */
final class NetnsStubServer {

	private final List<InetSocketAddress> endpoints = new ArrayList<>();
	private final List<HttpServer> servers = new ArrayList<>();
	private final List<Set<String>> sources = new ArrayList<>();
	/** Per listener, so requests a silenced endpoint never completes hold up no other endpoint. */
	private final List<ExecutorService> handlers = new ArrayList<>();

	private NetnsStubServer(final String[] args) {
		for (final var arg : args) {
			final var colon = arg.lastIndexOf(':');
			endpoints.add(new InetSocketAddress(arg.substring(0, colon), Integer.parseInt(arg.substring(colon + 1))));
			servers.add(null);
			sources.add(ConcurrentHashMap.newKeySet());
			handlers.add(Executors.newCachedThreadPool());
		}
	}

	public static void main(final String[] args) throws IOException {
		final var stub = new NetnsStubServer(args);
		System.out.println("started");
		System.out.flush();
		final var commands = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
		String line;
		while ((line = commands.readLine()) != null) {
			try {
				stub.execute(line.trim().split(" ", -1));
				System.out.println("ok");
			} catch (final IOException | RuntimeException e) {
				System.out.println("error " + e);
			}
			System.out.flush();
		}
		for (var i = 0; i < stub.servers.size(); i++) {
			stub.stop(i);
		}
		stub.handlers.forEach(ExecutorService::shutdownNow);
	}

	private void execute(final String[] command) throws IOException {
		switch (command[0]) {
		case "listen" -> {
			for (var i = 0; i < endpoints.size(); i++) {
				serve(i);
			}
		}
		case "refuse" -> stop(Integer.parseInt(command[1]));
		case "serve" -> serve(Integer.parseInt(command[1]));
		case "sources" -> {
			for (var i = 0; i < sources.size(); i++) {
				System.out.println("sources " + i + " " + String.join(",", new TreeSet<>(sources.get(i))));
			}
		}
		default -> throw new IllegalArgumentException("unknown command " + command[0]);
		}
	}

	private void serve(final int index) throws IOException {
		if (servers.get(index) != null) {
			return;
		}
		final var server = HttpServer.create(endpoints.get(index), 128);
		server.createContext("/", exchange -> handle(index, exchange));
		server.setExecutor(handlers.get(index));
		server.start();
		servers.set(index, server);
	}

	/** Closes the listener and every connection it accepted. */
	private void stop(final int index) {
		final var server = servers.set(index, null);
		if (server != null) {
			server.stop(0);
		}
	}

	private void handle(final int index, final HttpExchange exchange) throws IOException {
		try (exchange) {
			exchange.getRequestBody().readAllBytes();
			if ("PUT".equals(exchange.getRequestMethod())) {
				sources.get(index).add(exchange.getRemoteAddress().getAddress().getHostAddress());
				exchange.getResponseHeaders().set("ETag", "\"stub\"");
			}
			exchange.sendResponseHeaders(200, -1);
		}
	}
}
