package com.dell.spt.base.item.op.deletion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.metrics.snapshot.DeleteMetricsSnapshot;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StandaloneDeleteSelectionTest {

	@TempDir
	Path tempDir;

	@Test
	void freezesCanonicalManifestIdentityBeforeDispatchWithBoundedBuckets() throws Exception {
		final Path manifest = tempDir.resolve("selected.csv");
		final var content = new StringBuilder("bucket,key,size,version_id\r\n");
		for (int i = 0; i < DeleteMetricsSnapshot.MAX_BUCKET_METRICS + 2; i++) {
			content.append("bucket-").append(i).append(",key-").append(i).append(",1,")
							.append(i % 2 == 0 ? "\r\n" : "version-" + i + "\r\n");
		}
		Files.writeString(manifest, content, StandardCharsets.UTF_8);

		final StandaloneDeleteSelection selection = StandaloneDeleteSelection.fromManifest(manifest.toString());

		assertEquals(DeleteMetricsSnapshot.MAX_BUCKET_METRICS + 2L, selection.selected());
		assertEquals((DeleteMetricsSnapshot.MAX_BUCKET_METRICS + 3L) / 2L, selection.selectedCurrentKey());
		assertEquals((DeleteMetricsSnapshot.MAX_BUCKET_METRICS + 2L) / 2L, selection.selectedExactVersion());
		assertEquals(DeleteMetricsSnapshot.MAX_BUCKET_METRICS + 1, selection.selectedBuckets().size());
		assertEquals(
						1,
						selection.selectedBuckets().stream()
										.filter(value -> value.equals(DeleteMetricsSnapshot.OVERFLOW_BUCKET + "=2"))
										.count());
	}

	@Test
	void freezesMissingDirectEngineSelectionMetadataFromCanonicalManifest() throws Exception {
		final Path manifest = tempDir.resolve("direct-engine.csv");
		Files.writeString(
						manifest,
						"bucket,key,size,version_id\n"
										+ "bucket-a,current-a,1,\n"
										+ "bucket-a,version-a,1,v1\n"
										+ "bucket-b,current-b,1,\n"
										+ "bucket-b,version-b,1,v2\n");
		final var config = TestConfigBuilder.config();
		config.val("load-op-type", "delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-step-limit-time", "0s");
		config.val("item-input-file", manifest.toString());

		StandaloneDeleteSelection.ensureFrozen(
						config.configVal("load"), config.configVal("item"));

		assertEquals(4L, config.longVal("load-op-delete-selected"));
		assertEquals(2L, config.longVal("load-op-delete-selectedCurrentKey"));
		assertEquals(2L, config.longVal("load-op-delete-selectedExactVersion"));
		assertEquals(java.util.List.of("bucket-a=2", "bucket-b=2"),
						config.listVal("load-op-delete-selectedBuckets"));
	}
}
