package com.dell.spt.base.load.step.linear;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.item.io.IntegrityManifestItemInput;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.storage.driver.mock.DummyStorageDriverMockExtension;
import com.github.akurilov.confuse.Config;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The {@code op.trace.csv} rows of a step that takes its items from a canonical manifest over two
 * buckets, as write-verify's verify and cleanup steps, {@code read-verify} and {@code read
 * --items-file} do.
 *
 * <p>The step, its item input, its load generator and its trace output are the real ones; only the
 * storage driver is the dummy one, which completes every operation at once.
 */
@Timeout(60)
class LinearLoadStepLocalOpTraceTest {

	/** Manifest rows: bucket, then key, with keys that share prefixes across the two buckets. */
	private static final List<List<String>> MANIFEST = List.of(
					List.of("bucket-a", "s0/k0"),
					List.of("bucket-a", "s0/k1"),
					List.of("bucket-a", "s1/k2"),
					List.of("bucket-b", "s0/k3"),
					List.of("bucket-b", "s1/k4"),
					List.of("bucket-b", "s1/k0"));

	@TempDir
	Path temp;

	private final MetricsManagerImpl metrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
	private final LinkedBlockingQueue<String> events = new LinkedBlockingQueue<>();
	private final Logger traces = (Logger) Loggers.OP_TRACES;
	private Appender appender;
	private Level previousLevel;

	@BeforeEach
	void captureTraces() {
		appender = mock(Appender.class);
		when(appender.getName()).thenReturn("op-trace-manifest-test");
		when(appender.isStarted()).thenReturn(true);
		doAnswer(call -> {
			final LogEvent event = call.getArgument(0);
			events.add(event.getMessage().getFormattedMessage());
			return null;
		}).when(appender).append(any());
		previousLevel = traces.getLevel();
		traces.addAppender(appender);
		traces.setLevel(Level.INFO);
	}

	@AfterEach
	void releaseTraces() {
		traces.removeAppender(appender);
		traces.setLevel(previousLevel);
	}

	@ParameterizedTest
	@ValueSource(strings = {"read", "delete"
	})
	void everyRowOfAManifestDrivenStepNamesTheBucketOfItsObject(final String opType) throws Exception {
		final Path manifest = temp.resolve("written.csv");
		final var lines = new ArrayList<String>();
		lines.add(String.join(",", IntegrityManifestItemInput.HEADER));
		MANIFEST.forEach(row -> lines.add(row.get(0) + "," + row.get(1) + ",16,"));
		Files.write(manifest, lines);

		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", "op-trace-manifest-" + opType + "-" + System.nanoTime());
		config.val("load-op-type", opType);
		config.val("item-input-file", manifest.toString());
		config.val("item-input-path", null);
		config.val("output-metrics-trace-persist", true);
		config.val("output-metrics-timing-persist", false);
		config.val("item-data-input-compressibility", 0.0);
		config.val("item-data-dedupable", false);
		config.val("item-data-verify", false);
		config.val("item-data-ranges-concat", null);
		config.val("load-op-wait-finish", true);
		config.val("load-op-wait-limit", 0);
		final var step = new LinearLoadStepLocal(
						config, List.of(new DummyStorageDriverMockExtension<>()), List.of(), metrics);
		try {
			step.start();
			assertTrue(step.await(30, TimeUnit.SECONDS), "the step must finish: " + step.state());
		} finally {
			step.close();
		}

		final String op = opType.toUpperCase(Locale.ROOT);
		final List<String> expected = MANIFEST.stream()
						.map(row -> "/" + row.get(0) + "/" + row.get(1) + "," + op + ",SUCC")
						.sorted()
						.toList();
		assertEquals(expected, traceRows().stream()
						.map(row -> String.join(",", List.of(row.split(",", -1)).subList(1, 4)))
						.sorted()
						.toList());
	}

	/** The rows logged so far: a fence logged after them arrives after them on the logger thread. */
	private List<String> traceRows() throws InterruptedException {
		final String fence = "op-trace-manifest-fence-" + System.nanoTime();
		traces.info(fence);
		final var rows = new ArrayList<String>();
		while (true) {
			final String event = events.poll(10, TimeUnit.SECONDS);
			assertNotNull(event, "the fence must arrive; rows so far: " + rows);
			if (fence.equals(event)) {
				return rows;
			}
			event.lines().forEach(rows::add);
		}
	}
}
