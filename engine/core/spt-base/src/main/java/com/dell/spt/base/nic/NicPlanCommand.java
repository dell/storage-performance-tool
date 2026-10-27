package com.dell.spt.base.nic;

import com.dell.spt.base.buildinfo.EngineBuildInfoProvider;
import com.dell.spt.base.env.Extension;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * The engine image's {@code /opt/spt/spt-nic-plan} command: prints this host's NIC inventory and,
 * with {@code --interfaces}, the NIC plan, as one {@code spt.nic-plan} JSON document on stdout.
 * Logs go to stderr. Exit codes: 0 when the output was produced, a plan with failures included; 2
 * for a usage error; 1 for an internal error.
 */
public final class NicPlanCommand {

	static final int EXIT_OK = 0;
	static final int EXIT_INTERNAL = 1;
	static final int EXIT_USAGE = 2;

	static final String USAGE = """
					Usage: spt-nic-plan [--interfaces auto|<entry>[,<entry>...]] [--endpoint <host:port>]... [--rdma] [--pretty]
					  --interfaces  the NIC set to plan: auto, or interface names, IPv4 addresses or RDMA devices
					  --endpoint    one of the engine's endpoints after slicing, in configured order; repeatable
					  --rdma        apply the RDMA checks, and query DC support and the RoCE active MTU
					  --pretty      indent the JSON
					Prints the inventory, and with --interfaces the plan, as JSON on stdout.""";

	private static final String LOG_CONFIG = "nic-plan-log4j2.yaml";

	private final Function<Boolean, SysfsInventoryReader> readers;
	private final EndpointResolver resolver;
	private final NicPlanJson.Engine engine;

	/**
	 * Creates the command with its host dependencies.
	 *
	 * @param readers the inventory reader for an RDMA ({@code true}) or a plain run
	 */
	NicPlanCommand(final Function<Boolean, SysfsInventoryReader> readers, final EndpointResolver resolver, final NicPlanJson.Engine engine) {
		this.readers = readers;
		this.resolver = resolver;
		this.engine = engine;
	}

	public static void main(final String... args) {
		// The JSON owns stdout: anything else that prints, logging included, goes to stderr.
		System.setProperty("log4j2.configurationFile", LOG_CONFIG);
		final var out = System.out;
		System.setOut(System.err);
		final int status = production().run(args, out, System.err);
		out.flush();
		System.exit(status);
	}

	private static NicPlanCommand production() {
		final var buildInfo = EngineBuildInfoProvider.global().snapshot();
		return new NicPlanCommand(
						rdma -> SysfsInventoryReader.system(rdma ? extensionProbe() : RdmaDeviceProbe.NONE),
						EndpointResolver.SYSTEM,
						new NicPlanJson.Engine(buildInfo.version(), buildInfo.revision()));
	}

	/** The {@code s3-rdma} extension's native probe. The loader stays open: the native library lives for the process. */
	private static RdmaDeviceProbe extensionProbe() {
		final var extDir = Extension.resolveExtDir();
		return extDir == null ? RdmaDeviceProbe.NONE : RdmaDeviceProbe.load(Extension.extClassLoader(extDir));
	}

	int run(final String[] args, final PrintStream out, final PrintStream err) {
		final Arguments arguments;
		try {
			arguments = Arguments.parse(args);
		} catch (final IllegalArgumentException e) {
			err.println("spt-nic-plan: " + e.getMessage());
			err.println(USAGE);
			return EXIT_USAGE;
		}
		if (arguments.help()) {
			err.println(USAGE);
			return EXIT_OK;
		}
		try {
			final var reader = readers.apply(arguments.rdma());
			final var inventory = reader.read();
			final var plan = arguments.interfaces() == null
							? null
							: new NicPlanner(resolver).plan(inventory, arguments.interfaces(), arguments.endpoints(), arguments.rdma());
			out.println(NicPlanJson.serialize(engine, reader.hostname(), inventory, plan, arguments.pretty()));
			out.flush();
			return EXIT_OK;
		} catch (final Exception e) {
			err.println("spt-nic-plan: internal error: " + e);
			return EXIT_INTERNAL;
		}
	}

	/** The parsed command line; {@code interfaces} is {@code null} without {@code --interfaces}. */
	record Arguments(List<String> interfaces, List<String> endpoints, boolean rdma, boolean pretty, boolean help) {

		static Arguments parse(final String[] args) {
			List<String> interfaces = null;
			final var endpoints = new ArrayList<String>();
			boolean rdma = false;
			boolean pretty = false;
			for (int i = 0; i < args.length; i++) {
				final var arg = args[i];
				switch (arg) {
					case "--help", "-h" -> {
						return new Arguments(null, List.of(), false, false, true);
					}
					case "--rdma" -> rdma = true;
					case "--pretty" -> pretty = true;
					default -> {
						final var option = arg.contains("=") ? arg.substring(0, arg.indexOf('=')) : arg;
						if (!option.equals("--interfaces") && !option.equals("--endpoint")) {
							throw new IllegalArgumentException("unknown argument " + arg);
						}
						final String value;
						if (arg.contains("=")) {
							value = arg.substring(arg.indexOf('=') + 1);
						} else if (i + 1 < args.length) {
							value = args[++i];
						} else {
							throw new IllegalArgumentException(option + " needs a value");
						}
						if (value.isBlank()) {
							throw new IllegalArgumentException(option + " needs a value");
						}
						if (option.equals("--endpoint")) {
							endpoints.add(value.trim());
						} else if (interfaces != null) {
							throw new IllegalArgumentException("--interfaces is given twice");
						} else {
							interfaces = Arrays.stream(value.split(",", -1)).map(String::trim).toList();
						}
					}
				}
			}
			return new Arguments(interfaces, endpoints, rdma, pretty, false);
		}
	}
}
