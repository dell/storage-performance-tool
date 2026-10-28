package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectMethod;

import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.junit.platform.engine.DiscoverySelector;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TagFilter;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;

/**
 * Runs the {@code netns} tests in this JVM. {@code engine/tools/netns-test.sh} starts it inside its
 * network namespace with the test class path, as Gradle cannot run there beside other Gradle
 * processes: their shared cache locks are negotiated over the loopback of the host's namespace.
 *
 * <p>Arguments, if any, are test method names of {@link S3NicSetNamespaceTest} to run alone. It
 * exits with 0 only when tests ran and none failed.
 */
final class NetnsTestMain {

	private NetnsTestMain() {}

	public static void main(final String[] args) {
		final DiscoverySelector[] selectors = args.length == 0
						? new DiscoverySelector[]{selectClass(S3NicSetNamespaceTest.class)
						}
						: Arrays.stream(args).map(name -> selectMethod(S3NicSetNamespaceTest.class, name)).toArray(DiscoverySelector[]::new);
		final var request = LauncherDiscoveryRequestBuilder.request()
						.selectors(selectors)
						.filters(TagFilter.includeTags("netns"))
						.build();
		final var summary = new SummaryGeneratingListener();
		LauncherFactory.create().execute(request, summary, new Progress());

		final var out = new PrintWriter(System.out, true, StandardCharsets.UTF_8);
		summary.getSummary().printTo(out);
		summary.getSummary().printFailuresTo(out, 25);
		final var ok = summary.getSummary().getTestsSucceededCount() > 0 && summary.getSummary().getTotalFailureCount() == 0;
		System.exit(ok ? 0 : 1);
	}

	/** Prints each test as it starts and finishes. */
	private static final class Progress implements TestExecutionListener {

		private long startedNanos;

		@Override
		public void executionStarted(final TestIdentifier test) {
			if (test.isTest()) {
				startedNanos = System.nanoTime();
				System.out.println(test.getDisplayName() + " STARTED");
			}
		}

		@Override
		public void executionFinished(final TestIdentifier test, final TestExecutionResult result) {
			if (test.isTest() || result.getStatus() != TestExecutionResult.Status.SUCCESSFUL) {
				final var millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
				System.out.println(test.getDisplayName() + " " + result.getStatus() + " (" + millis + " ms)"
								+ result.getThrowable().map(failure -> ": " + failure).orElse(""));
			}
		}
	}
}
