package com.dell.spt.storage.driver.coop.aws.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * A request the target answers after JVM shutdown has begun completes. The AWS CRT has a shutdown
 * hook of its own, which runs at once on {@code System.exit}, SIGINT or SIGTERM, beside the
 * engine's hook that stops the step; unless a driver holds a CRT shutdown reference, the CRT then
 * hands no result to Java and the stop cancels the request as {@code INTERRUPTED} although the
 * target completed it. Each case runs {@link S3AwsJvmShutdownProbe} in a child JVM.
 */
@EnabledOnOs(OS.LINUX)
final class S3AwsJvmShutdownTest {

	private static final Duration READY_BUDGET = Duration.ofSeconds(60);
	private static final Duration EXIT_BUDGET = Duration.ofSeconds(60);

	enum Shutdown {
		EXIT(0), SIGTERM(143), SIGINT(130);

		final int exitCode;

		Shutdown(final int exitCode) {
			this.exitCode = exitCode;
		}
	}

	@ParameterizedTest
	@EnumSource(Shutdown.class)
	void aRequestAnsweredAfterJvmShutdownBeganCompletes(final Shutdown shutdown, @TempDir final Path home)
					throws Exception {
		final List<String> command = new ArrayList<>();
		if (shutdown == Shutdown.SIGINT) {
			// A launcher started as a background job of a shell ignores SIGINT, and a child inherits
			// that, so the JVM would never see the signal. Restore its default disposition.
			if (envRestoresDefaultSignals()) {
				command.addAll(List.of("env", "--default-signal=INT"));
			} else {
				assumeFalse(sigintIgnored(), "SIGINT is ignored here and env cannot restore it");
			}
		}
		command.addAll(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
						"-cp", System.getProperty("java.class.path"),
						S3AwsJvmShutdownProbe.class.getName(),
						shutdown == Shutdown.EXIT ? "exit" : "signal", home.toString()));
		final Process child = new ProcessBuilder(command)
						.redirectErrorStream(true)
						.start();
		final List<String> output = new CopyOnWriteArrayList<>();
		final var ready = new CountDownLatch(1);
		final Thread reader = new Thread(() -> {
			try (final var lines = new BufferedReader(new InputStreamReader(child.getInputStream(), StandardCharsets.UTF_8))) {
				String line;
				while ((line = lines.readLine()) != null) {
					output.add(line);
					if (line.equals(S3AwsJvmShutdownProbe.READY)) {
						ready.countDown();
					}
				}
			} catch (final IOException ignored) {
				// The child has gone.
			}
		}, "probe-output");
		reader.setDaemon(true);
		reader.start();
		try {
			if (!ready.await(READY_BUDGET.toMillis(), TimeUnit.MILLISECONDS)) {
				fail("the probe never had its PUT in flight:\n" + String.join("\n", output));
			}
			if (shutdown != Shutdown.EXIT) {
				// Not Process.destroy(), which also closes the child's output on this side.
				final Process kill = new ProcessBuilder("kill", "-" + shutdown.name().substring(3), Long.toString(child.pid()))
								.inheritIO()
								.start();
				assertEquals(0, kill.waitFor(), "kill failed");
			}
			assertTrue(child.waitFor(EXIT_BUDGET.toMillis(), TimeUnit.MILLISECONDS),
							"the probe did not exit within " + EXIT_BUDGET + ":\n" + String.join("\n", output));
			reader.join(TimeUnit.SECONDS.toMillis(5));
			final String log = String.join("\n", output);
			assertTrue(output.contains(S3AwsJvmShutdownProbe.RESULT + "SUCC"),
							"the PUT the target answered after " + shutdown + " did not succeed:\n" + log);
			assertTrue(output.contains(S3AwsJvmShutdownProbe.CLOSED), "the driver did not close:\n" + log);
			assertEquals(shutdown.exitCode, child.exitValue(), log);
		} finally {
			child.destroyForcibly();
		}
	}

	/** Whether {@code env} has {@code --default-signal} (GNU coreutils 8.31 and later). */
	private static boolean envRestoresDefaultSignals() throws InterruptedException {
		try {
			final Process probe = new ProcessBuilder("env", "--default-signal=INT", "true")
							.redirectErrorStream(true)
							.redirectOutput(ProcessBuilder.Redirect.DISCARD)
							.start();
			return probe.waitFor(10, TimeUnit.SECONDS) && probe.exitValue() == 0;
		} catch (final IOException e) {
			return false;
		}
	}

	/** Whether this JVM, and so a child it starts, ignores SIGINT: bit 2 of {@code SigIgn}. */
	private static boolean sigintIgnored() throws IOException {
		for (final String line : Files.readAllLines(Path.of("/proc/self/status"), StandardCharsets.US_ASCII)) {
			if (line.startsWith("SigIgn:")) {
				return (Long.parseUnsignedLong(line.substring("SigIgn:".length()).trim(), 16) & 0x2L) != 0;
			}
		}
		return false;
	}
}
