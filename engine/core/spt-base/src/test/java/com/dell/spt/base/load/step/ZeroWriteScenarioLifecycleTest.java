package com.dell.spt.base.load.step;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.control.run.RunImpl;
import com.dell.spt.base.integrity.IntegrityInputProvenance;
import com.dell.spt.base.integrity.IntegrityManifestCompletion;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.load.step.client.CsvArtifactAggregator;
import com.dell.spt.base.load.step.file.FileManager;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.script.ScriptEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ZeroWriteScenarioLifecycleTest {
	@TempDir
	Path tempDir;

	@Test
	void generatedTwoStepShapePublishesEmptyCreateAndNeverEntersRead() throws Exception {
		final long runId = 716;
		final Path manifest = tempDir.resolve("written.csv");
		Files.writeString(manifest, "bucket,key,size,version_id\n");
		final var aggregation = new CsvArtifactAggregator(
						"create-step",
						List.of(FileManager.INSTANCE),
						List.of(),
						manifest.toString(),
						runId,
						0);
		final var readConfigured = new AtomicBoolean();
		final var readObjectIOEntered = new AtomicBoolean();
		final ScriptEngine engine = ScenarioUtil.scriptEngineByDefault(
						Thread.currentThread().getContextClassLoader());
		assertNotNull(engine, "default JavaScript engine must be available for scenario execution");
		engine.put("CreateLoad", new ZeroWriteCreateLoad(aggregation));
		engine.put("ReadLoad", new ObservedReadLoad(readConfigured, readObjectIOEntered));
		final String scenario = """
						CreateLoad.config({"load":{"step":{"id":"create-step"}}}).run();
						ReadLoad.config({"item":{"input":{"file":"written.csv"}},"load":{"step":{"id":"read-step"}}}).run();
						""";
		final var run = new RunImpl("zero-write generated lifecycle", scenario, engine, runId);

		final var failure = assertThrows(IntegrityTerminalException.class, run::run);

		assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
		assertTrue(failure.getMessage().contains("zero successful objects"));
		assertFalse(readConfigured.get(), "dependent READ configuration ran after CREATE failure");
		assertFalse(readObjectIOEntered.get(), "dependent READ object I/O ran after CREATE failure");
		final var completion = IntegrityManifestCompletion.validate(
						manifest, runId, IntegrityInputProvenance.ENGINE_STEP, "create-step");
		assertEquals(0, completion.selectedRecordCount());
		assertFalse(Files.exists(tempDir.resolve("verified.csv")));
	}

	@Test
	void incompleteSeedInventoryStopsBeforeDeleteConfiguration() throws Exception {
		final long runId = 717;
		final Path manifest = tempDir.resolve("written.csv");
		Files.writeString(
						manifest,
						"bucket,key,size,version_id\n"
										+ "bucket,only-one,1024,returned-version\n");
		final var aggregation = new CsvArtifactAggregator(
						"seed-step",
						List.of(FileManager.INSTANCE),
						List.of(),
						manifest.toString(),
						runId,
						2,
						com.dell.spt.base.item.op.OpType.CREATE,
						true);
		final var deleteConfigured = new AtomicBoolean();
		final var deleteObjectIOEntered = new AtomicBoolean();
		final ScriptEngine engine = ScenarioUtil.scriptEngineByDefault(
						Thread.currentThread().getContextClassLoader());
		assertNotNull(engine, "default JavaScript engine must be available for scenario execution");
		engine.put("CreateLoad", new ZeroWriteCreateLoad(aggregation));
		engine.put("DeleteLoad", new ObservedReadLoad(deleteConfigured, deleteObjectIOEntered));
		final String scenario = """
						CreateLoad.config({"load":{"step":{"id":"seed-step"}}}).run();
						DeleteLoad.config({"item":{"input":{"file":"written.csv"}},"load":{"step":{"id":"delete-step"}}}).run();
						""";

		final var failure = assertThrows(
						IntegrityTerminalException.class,
						new RunImpl("incomplete seeded DELETE lifecycle", scenario, engine, runId)::run);

		assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
		assertTrue(failure.getMessage().contains("expected 2 successful objects but froze 1"));
		assertFalse(deleteConfigured.get(), "dependent DELETE configuration ran after incomplete seed");
		assertFalse(deleteObjectIOEntered.get(), "dependent DELETE object I/O ran after incomplete seed");
		assertFalse(Files.exists(IntegrityManifestCompletion.completionPath(manifest)));
	}

	public static final class ZeroWriteCreateLoad {
		private final CsvArtifactAggregator aggregation;

		ZeroWriteCreateLoad(final CsvArtifactAggregator aggregation) {
			this.aggregation = aggregation;
		}

		public ZeroWriteCreateStep config(final Object ignored) {
			return new ZeroWriteCreateStep(aggregation);
		}
	}

	public static final class ZeroWriteCreateStep {
		private final CsvArtifactAggregator aggregation;

		ZeroWriteCreateStep(final CsvArtifactAggregator aggregation) {
			this.aggregation = aggregation;
		}

		public void run() {
			aggregation.close();
		}
	}

	public static final class ObservedReadLoad {
		private final AtomicBoolean configured;
		private final AtomicBoolean objectIOEntered;

		ObservedReadLoad(final AtomicBoolean configured, final AtomicBoolean objectIOEntered) {
			this.configured = configured;
			this.objectIOEntered = objectIOEntered;
		}

		public ObservedReadStep config(final Object ignored) {
			configured.set(true);
			return new ObservedReadStep(objectIOEntered);
		}
	}

	public static final class ObservedReadStep {
		private final AtomicBoolean objectIOEntered;

		ObservedReadStep(final AtomicBoolean objectIOEntered) {
			this.objectIOEntered = objectIOEntered;
		}

		public void run() {
			objectIOEntered.set(true);
		}
	}
}
