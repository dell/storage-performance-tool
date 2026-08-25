package com.dell.spt.base.load.step.client;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.config.ConstantValueInputImpl;
import com.dell.spt.base.integrity.IntegrityCsvFormat;
import com.dell.spt.base.integrity.IntegrityInputProvenance;
import com.dell.spt.base.integrity.IntegrityManifestCompletion;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactoryImpl;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.io.CsvItemInput;
import com.dell.spt.base.item.io.IntegrityManifestItemInput;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationsBuilderImpl;
import com.dell.spt.base.item.op.deletion.DeleteRequestAssembler;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import com.dell.spt.base.item.op.deletion.StandaloneDeleteSelection;
import com.dell.spt.base.load.generator.LoadGeneratorImpl;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.load.step.file.FileManagerImpl;
import com.dell.spt.base.load.step.service.file.FileManagerService;
import com.dell.spt.base.load.step.service.file.FileManagerServiceImpl;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;
import com.github.akurilov.commons.io.Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("ItemInputFileSlicer & ItemOutputFileAggregator")
class ItemFileSlicerAggregatorTest {
	private static final long RUN_ID = 25L;
	private static final String PRODUCER_ID = "ticket-25-canonical-stager";

	private final List<Path> filesToCleanup = new ArrayList<>();

	@AfterEach
	void cleanup() {
		for (Path p : filesToCleanup) {
			try {
				Files.deleteIfExists(p);
			} catch (final Exception e) {
				fail("Failed to delete temp file " + p, e);
			}
		}
		filesToCleanup.clear();
	}

	@Test
	@DisplayName("ItemInputFileSlicer distributes items and cleans up")
	void testItemInputFileSlicerDistributesAndCleansUp() throws Exception {
		// Arrange: 2 local file managers and 2 config slices
		List<FileManager> fileMgrs = List.of(new FileManagerImpl(), new FileManagerImpl());
		Config baseCfg = TestConfigBuilder.config();
		List<Config> configSlices = List.of(new BasicConfig(baseCfg), new BasicConfig(baseCfg));

		// Input with 5 simple items
		String data = String.join("\n", List.of("a", "b", "c", "d", "e")) + "\n";
		CsvItemInput<Item> itemInput = new CsvItemInput<>(
						new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)), new ItemFactoryImpl<>());

		// Act: construct slicer which performs slicing in constructor
		int batchSize = 2;
		ItemInputFileSlicer slicer = new ItemInputFileSlicer(
						"test-step-slicer", fileMgrs, configSlices, itemInput, batchSize);

		// Assert: each config slice has an item-input-file set and files exist with some data
		for (int i = 0; i < configSlices.size(); i++) {
			String itemInputFile = configSlices.get(i).stringVal("item-input-file");
			assertNotNull(itemInputFile);
			assertFalse(itemInputFile.isEmpty());
			Path p = Path.of(itemInputFile);
			assertTrue(Files.exists(p), "Slice file should exist before close");
			assertTrue(Files.size(p) > 0, "Slice file should contain data");
			filesToCleanup.add(p); // in case close fails
		}

		// Cleanup via slicer.close() should delete the temporary files
		slicer.close();
		for (Config cfg : configSlices) {
			String f = cfg.stringVal("item-input-file");
			assertFalse(Files.exists(Path.of(f)), "Slice file should be deleted on close");
		}
	}

	@Test
	@DisplayName("ItemInputFileSlicer carries round-robin ownership across input batches")
	void itemInputFileSlicerCarriesRoundRobinAcrossInputBatches() throws Exception {
		final List<FileManager> fileMgrs = List.of(
						mock(FileManager.class), mock(FileManager.class), mock(FileManager.class));
		final List<ByteArrayOutputStream> captured = List.of(
						new ByteArrayOutputStream(), new ByteArrayOutputStream(), new ByteArrayOutputStream());
		for (int i = 0; i < fileMgrs.size(); i++) {
			final FileManager fileMgr = fileMgrs.get(i);
			final ByteArrayOutputStream bytes = captured.get(i);
			when(fileMgr.newTmpFileName()).thenReturn("slice-" + i);
			doAnswer(invocation -> {
				bytes.write(invocation.<byte[]> getArgument(1));
				return null;
			}).when(fileMgr).writeToFile(anyString(), any(byte[].class));
		}
		final Config baseCfg = TestConfigBuilder.config();
		final List<Config> configSlices = List.of(
						new BasicConfig(baseCfg), new BasicConfig(baseCfg), new BasicConfig(baseCfg));
		final String data = String.join("\n", List.of("a", "b", "c", "d", "e", "f", "g", "h")) + "\n";
		final CsvItemInput<Item> itemInput = new CsvItemInput<>(
						new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)), new ItemFactoryImpl<>());

		try (final var ignored = new ItemInputFileSlicer(
						"persistent-round-robin", fileMgrs, configSlices, itemInput, 2)) {
			assertEquals(List.of("a", "d", "g"), deserializeNames(captured.get(0)));
			assertEquals(List.of("b", "e", "h"), deserializeNames(captured.get(1)));
			assertEquals(List.of("c", "f"), deserializeNames(captured.get(2)));
		}
	}

	private static List<String> deserializeNames(final ByteArrayOutputStream bytes) throws Exception {
		final List<String> names = new ArrayList<>();
		try (final var input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
			while (true) {
				try {
					names.add(((Item) input.readUnshared()).name());
				} catch (final EOFException ignored) {
					return names;
				}
			}
		}
	}

	@Test
	void strictSlicerKeepsCanonicalManifestFormatAndExactCounts() throws Exception {
		final Path source = Files.createTempFile("spt-delete-manifest-", ".csv");
		filesToCleanup.add(source);
		Files.writeString(
						source,
						"bucket,key,size,version_id\n"
										+ "bucket,alpha,9,\n"
										+ "bucket,\"comma,key\",7,version-comma\n"
										+ "bucket,gamma,5,\n"
										+ "bucket,omega,3,version-omega\n",
						StandardCharsets.UTF_8);
		final List<FileManager> fileMgrs = List.of(mock(FileManager.class), mock(FileManager.class));
		final List<ByteArrayOutputStream> captured = List.of(
						new ByteArrayOutputStream(), new ByteArrayOutputStream());
		for (int i = 0; i < fileMgrs.size(); i++) {
			final FileManager fileMgr = fileMgrs.get(i);
			final ByteArrayOutputStream bytes = captured.get(i);
			when(fileMgr.newTmpFileName()).thenReturn("canonical-slice-" + i);
			doAnswer(invocation -> {
				bytes.write(invocation.<byte[]> getArgument(1));
				return null;
			}).when(fileMgr).writeToFile(anyString(), any(byte[].class));
		}
		final Config baseCfg = TestConfigBuilder.config();
		baseCfg.val("load-op-type", "delete");
		baseCfg.val("load-op-delete-standalone", true);
		final List<Config> configSlices = List.of(new BasicConfig(baseCfg), new BasicConfig(baseCfg));

		try (final var input = new IntegrityManifestItemInput(source);
						final var ignored = new ItemInputFileSlicer(
										"canonical-delete-slicer", fileMgrs, configSlices, input, 2, true)) {
			assertEquals("canonical-slice-0.csv", configSlices.get(0).stringVal("item-input-file"));
			assertEquals("canonical-slice-1.csv", configSlices.get(1).stringVal("item-input-file"));
			for (int i = 0; i < configSlices.size(); i++) {
				final Config slice = configSlices.get(i);
				assertEquals(2L, slice.longVal("load-op-delete-selected"));
				assertEquals(i == 0 ? 2L : 0L, slice.longVal("load-op-delete-selectedCurrentKey"));
				assertEquals(i == 0 ? 0L : 2L, slice.longVal("load-op-delete-selectedExactVersion"));
				assertEquals(List.of("bucket=2"), slice.listVal("load-op-delete-selectedBuckets"));
				assertEquals("canonical", slice.stringVal("load-op-delete-selectionOrder"));
			}
			assertEquals(
							"bucket,key,size,version_id\nbucket,alpha,9,\nbucket,gamma,5,\n",
							captured.get(0).toString(StandardCharsets.UTF_8));
			assertEquals(
							"bucket,key,size,version_id\nbucket,\"comma,key\",7,version-comma\n"
											+ "bucket,omega,3,version-omega\n",
							captured.get(1).toString(StandardCharsets.UTF_8));
		}
	}

	@Test
	void strictSlicerKeepsOneOwnerRoundRobinAcrossNonDivisibleInputReads() throws Exception {
		final String manifest = "bucket,key,size,version_id\n"
						+ "bucket,alpha,8,\n"
						+ "bucket,bravo,7,version-bravo\n"
						+ "bucket,charlie,6,\n"
						+ "bucket,delta,5,version-delta\n"
						+ "bucket,echo,4,\n"
						+ "bucket,foxtrot,3,version-foxtrot\n"
						+ "bucket,golf,2,\n"
						+ "bucket,hotel,1,version-hotel\n";

		try (final var slicing = strictSlicing("canonical-non-divisible", manifest, 3, 2)) {
			assertCanonicalSlice(
							slicing,
							0,
							"bucket,key,size,version_id\n"
											+ "bucket,alpha,8,\n"
											+ "bucket,delta,5,version-delta\n"
											+ "bucket,golf,2,\n",
							3,
							2,
							1);
			assertCanonicalSlice(
							slicing,
							1,
							"bucket,key,size,version_id\n"
											+ "bucket,bravo,7,version-bravo\n"
											+ "bucket,echo,4,\n"
											+ "bucket,hotel,1,version-hotel\n",
							3,
							1,
							2);
			assertCanonicalSlice(
							slicing,
							2,
							"bucket,key,size,version_id\n"
											+ "bucket,charlie,6,\n"
											+ "bucket,foxtrot,3,version-foxtrot\n",
							2,
							1,
							1);
			assertExactOneOwner(slicing, manifest);
		}
	}

	@Test
	void strictSlicerMakesSparseWorkersHeaderOnlyAndImmediatelyExhausted() throws Exception {
		final String manifest = "bucket,key,size,version_id\n"
						+ "bucket,only,1,\n";
		final String header = "bucket,key,size,version_id\n";

		try (final var slicing = strictSlicing("canonical-sparse", manifest, 3, 2)) {
			assertCanonicalSlice(slicing, 0, manifest, 1, 1, 0);
			assertCanonicalSlice(slicing, 1, header, 0, 0, 0);
			assertCanonicalSlice(slicing, 2, header, 0, 0, 0);
			assertExactOneOwner(slicing, manifest);

			final Path headerOnlySlice = Files.createTempFile("spt-delete-header-only-", ".csv");
			Files.write(headerOnlySlice, slicing.captured().get(1).toByteArray());
			filesToCleanup.add(headerOnlySlice);
			@SuppressWarnings("unchecked")
			final Output<DeleteRequestOperation> output = mock(Output.class);
			final var operationsBuilder = new OperationsBuilderImpl<IntegrityManifestDataItem, Operation<IntegrityManifestDataItem>>(1);
			operationsBuilder.opType(OpType.DELETE)
							.credentialInput(new ConstantValueInputImpl<>(Credential.NONE));
			try (final var input = new IntegrityManifestItemInput(headerOnlySlice);
							final var generator = new LoadGeneratorImpl<IntegrityManifestDataItem, DeleteRequestOperation>(
											input,
											new DeleteRequestAssembler(operationsBuilder, 100),
											List.of(),
											output,
											2,
											0,
											100,
											false,
											false)) {
				final long beforeExhaustion = System.nanoTime();
				generator.start();
				assertTrue(generator.await(1, TimeUnit.SECONDS));
				final long afterExhaustion = System.nanoTime();

				assertEquals(0, generator.consumedItemCount());
				assertEquals(0, generator.generatedOpCount());
				assertTrue(generator.schedulingExhaustionNanos().isPresent());
				assertTrue(generator.schedulingExhaustedAtNanos() >= beforeExhaustion);
				assertTrue(generator.schedulingExhaustedAtNanos() <= afterExhaustion);
				verifyNoInteractions(output);
			}
		}
	}

	@Test
	void strictSingleSlicePreservesCanonicalManifestAndProvenance() throws Exception {
		final String manifest = "bucket,key,size,version_id\n"
						+ "bucket,alpha,4,\n"
						+ "bucket,bravo,3,version-bravo\n"
						+ "bucket,\"comma,key\",2,\n"
						+ "bucket,delta,1,version-delta\n";

		try (final var slicing = strictSlicing("canonical-single", manifest, 1, 3)) {
			assertCanonicalSlice(slicing, 0, manifest, 4, 2, 2);
			assertExactOneOwner(slicing, manifest);
			final IntegrityManifestCompletion validated = IntegrityManifestCompletion.validate(
							slicing.source(), RUN_ID, IntegrityInputProvenance.CLI_STAGER, PRODUCER_ID);
			assertEquals(4, validated.sourceRecordCount());
			assertEquals(4, validated.uniqueRecordCount());
			assertEquals(4, validated.selectedRecordCount());
			assertEquals(slicing.sourceCompletion().manifestBytes(), validated.manifestBytes());
			assertEquals(slicing.sourceCompletion().manifestSha256(), validated.manifestSha256());
			assertEquals(sha256(manifest), validated.manifestSha256());
			assertEquals(validated.manifestSha256(), sha256(slicing.csv(0)));
		}
	}

	private StrictSlicing strictSlicing(
					final String stepId,
					final String manifest,
					final int sliceCount,
					final int readBatchSize) throws Exception {
		final Path source = Files.createTempFile("spt-delete-canonical-", ".csv");
		Files.writeString(source, manifest, StandardCharsets.UTF_8);
		filesToCleanup.add(source);
		final long selected = StandaloneDeleteSelection.fromManifest(source.toString()).selected();
		final IntegrityManifestCompletion completion = IntegrityManifestCompletion.create(
						source,
						RUN_ID,
						IntegrityManifestCompletion.PRODUCER_CLI_STAGER,
						PRODUCER_ID,
						selected,
						selected,
						selected);
		completion.publish(source);
		filesToCleanup.add(IntegrityManifestCompletion.completionPath(source));

		final List<FileManager> fileMgrs = new ArrayList<>(sliceCount);
		final List<ByteArrayOutputStream> captured = new ArrayList<>(sliceCount);
		for (int i = 0; i < sliceCount; i++) {
			final FileManager fileMgr = mock(FileManager.class);
			final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			when(fileMgr.newTmpFileName()).thenReturn(stepId + "-slice-" + i);
			doAnswer(invocation -> {
				bytes.write(invocation.<byte[]> getArgument(1));
				return null;
			}).when(fileMgr).writeToFile(anyString(), any(byte[].class));
			fileMgrs.add(fileMgr);
			captured.add(bytes);
		}
		final Config baseConfig = TestConfigBuilder.config();
		baseConfig.val("run-id", RUN_ID);
		baseConfig.val("storage-integrity-input-provenance", IntegrityInputProvenance.CLI_STAGER.value());
		baseConfig.val("storage-integrity-input-expectedProducerId", PRODUCER_ID);
		baseConfig.val("load-op-type", "delete");
		baseConfig.val("load-op-delete-standalone", true);
		final List<Config> configSlices = new ArrayList<>(sliceCount);
		for (int i = 0; i < sliceCount; i++) {
			configSlices.add(new BasicConfig(baseConfig));
		}

		final ItemInputFileSlicer slicer;
		try (final var input = new IntegrityManifestItemInput(source)) {
			slicer = new ItemInputFileSlicer(
							stepId, fileMgrs, configSlices, input, readBatchSize, true);
		}
		return new StrictSlicing(source, completion, configSlices, captured, slicer);
	}

	private static void assertCanonicalSlice(
					final StrictSlicing slicing,
					final int sliceIndex,
					final String expectedCsv,
					final long expectedSelected,
					final long expectedCurrentKey,
					final long expectedExactVersion) throws Exception {
		final String actualCsv = slicing.csv(sliceIndex);
		assertEquals(expectedCsv, actualCsv);
		final List<List<String>> identities = manifestIdentities(actualCsv);
		assertEquals(expectedSelected, identities.size());
		final Config config = slicing.configSlices().get(sliceIndex);
		assertEquals(expectedSelected, config.longVal("load-op-delete-selected"));
		assertEquals(expectedCurrentKey, config.longVal("load-op-delete-selectedCurrentKey"));
		assertEquals(expectedExactVersion, config.longVal("load-op-delete-selectedExactVersion"));
		assertEquals(List.of("bucket=" + expectedSelected), config.listVal("load-op-delete-selectedBuckets"));
		assertEquals("canonical", config.stringVal("load-op-delete-selectionOrder"));
		assertEquals(RUN_ID, config.longVal("run-id"));
		assertEquals(
						IntegrityInputProvenance.CLI_STAGER.value(),
						config.stringVal("storage-integrity-input-provenance"));
		assertEquals(PRODUCER_ID, config.stringVal("storage-integrity-input-expectedProducerId"));
	}

	private static void assertExactOneOwner(
					final StrictSlicing slicing, final String sourceManifest) throws Exception {
		final Set<List<String>> expected = new LinkedHashSet<>(manifestIdentities(sourceManifest));
		final Set<List<String>> union = new LinkedHashSet<>();
		for (int i = 0; i < slicing.captured().size(); i++) {
			for (final List<String> identity : manifestIdentities(slicing.csv(i))) {
				assertTrue(union.add(identity), "identity has more than one slice owner: " + identity);
			}
		}
		assertEquals(expected, union, "slice union must equal the frozen canonical selection");
		final IntegrityManifestCompletion validated = IntegrityManifestCompletion.validate(
						slicing.source(), RUN_ID, IntegrityInputProvenance.CLI_STAGER, PRODUCER_ID);
		assertEquals(expected.size(), validated.selectedRecordCount());
		assertEquals(sha256(sourceManifest), validated.manifestSha256());
		assertEquals(slicing.sourceCompletion().manifestSha256(), validated.manifestSha256());
	}

	private static List<List<String>> manifestIdentities(final String manifest) throws Exception {
		final var records = IntegrityCsvFormat.RFC4180_LF.parse(new StringReader(manifest)).getRecords();
		assertFalse(records.isEmpty());
		assertEquals(IntegrityManifestItemInput.HEADER, records.get(0).toList());
		return records.stream().skip(1).map(record -> List.copyOf(record.toList())).toList();
	}

	private static String sha256(final String value) throws Exception {
		return HexFormat.of().formatHex(
						MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
	}

	private record StrictSlicing(
					Path source,
					IntegrityManifestCompletion sourceCompletion,
					List<Config> configSlices,
					List<ByteArrayOutputStream> captured,
					ItemInputFileSlicer slicer) implements AutoCloseable {

	private String csv(final int sliceIndex) {
		return captured.get(sliceIndex).toString(StandardCharsets.UTF_8);
	}

	@Override
	public void close() {
		slicer.close();
	}

	}

	@Test
	void strictSlicerPublishesOneCanonicalBucketMappingToEveryWorker() throws Exception {
		final Path source = Files.createTempFile("spt-delete-many-buckets-", ".csv");
		filesToCleanup.add(source);
		final StringBuilder manifest = new StringBuilder("bucket,key,size,version_id\n");
		for (int i = 0; i <= com.dell.spt.base.metrics.snapshot.DeleteMetricsSnapshot.MAX_BUCKET_METRICS; i++) {
			manifest.append(String.format(java.util.Locale.ROOT, "bucket-%03d,key-%03d,1,\n", i, i));
		}
		manifest.append("bucket-099,repeat-a,1,\n");
		manifest.append("bucket-099,repeat-b,1,\n");
		Files.writeString(source, manifest, StandardCharsets.UTF_8);

		final List<FileManager> fileMgrs = List.of(mock(FileManager.class), mock(FileManager.class));
		for (int i = 0; i < fileMgrs.size(); i++) {
			when(fileMgrs.get(i).newTmpFileName()).thenReturn("many-bucket-slice-" + i);
		}
		final Config baseCfg = TestConfigBuilder.config();
		baseCfg.val("load-op-type", "delete");
		baseCfg.val("load-op-delete-standalone", true);
		final List<Config> configSlices = List.of(new BasicConfig(baseCfg), new BasicConfig(baseCfg));

		try (final var input = new IntegrityManifestItemInput(source);
						final var ignored = new ItemInputFileSlicer(
										"canonical-bucket-map", fileMgrs, configSlices, input, 16, true)) {
			final Map<String, Long> first = selectedBuckets(configSlices.get(0));
			final Map<String, Long> second = selectedBuckets(configSlices.get(1));
			assertEquals(first.keySet(), second.keySet(),
							"every worker must receive the same retained bucket names");
			assertEquals(
							com.dell.spt.base.metrics.snapshot.DeleteMetricsSnapshot.MAX_BUCKET_METRICS + 1,
							first.size());
			assertTrue(first.containsKey("bucket-099"));
			assertTrue(first.containsKey(com.dell.spt.base.metrics.snapshot.DeleteMetricsSnapshot.OVERFLOW_BUCKET));
			assertEquals(1L, first.get("bucket-099"));
			assertEquals(2L, second.get("bucket-099"));
			assertEquals(1L, first.get(com.dell.spt.base.metrics.snapshot.DeleteMetricsSnapshot.OVERFLOW_BUCKET));
			assertEquals(0L, second.get(com.dell.spt.base.metrics.snapshot.DeleteMetricsSnapshot.OVERFLOW_BUCKET));
		}
	}

	private static Map<String, Long> selectedBuckets(final Config config) {
		final Map<String, Long> result = new TreeMap<>();
		for (final Object raw : config.listVal("load-op-delete-selectedBuckets")) {
			final String value = String.valueOf(raw);
			final int separator = value.lastIndexOf('=');
			result.put(value.substring(0, separator), Long.parseLong(value.substring(separator + 1)));
		}
		return result;
	}

	@Test
	@DisplayName("ItemOutputFileAggregator collects remote slices into local output and deletes remotes")
	void testItemOutputFileAggregatorCollectsFromRemoteSlices() throws Exception {
		// Arrange: first file manager is local, second is remote (service)
		FileManager localMgr = new FileManagerImpl();
		FileManagerService remoteMgr = new FileManagerServiceImpl(0); // not started, used as a marker and delegate
		List<FileManager> fileMgrs = List.of(localMgr, remoteMgr);

		Config baseCfg = TestConfigBuilder.config();
		List<Config> configSlices = List.of(new BasicConfig(baseCfg), new BasicConfig(baseCfg));

		// Target local output file
		Path localOutput = Files.createTempFile("spt-itm-agg-", ".out");
		filesToCleanup.add(localOutput);

		// Construct aggregator to prepare remote slice filenames in configSlices[1]
		ItemOutputFileAggregator aggregator = new ItemOutputFileAggregator(
						"test-step-agg", fileMgrs, configSlices, localOutput.toString());

		// Prepare remote content: write to the remote slice file that aggregator configured
		String remoteFile = configSlices.get(1).stringVal("item-output-file");
		assertNotNull(remoteFile);
		assertFalse(remoteFile.isEmpty());
		byte[] payload = "hello\nworld\n".getBytes(StandardCharsets.UTF_8);
		remoteMgr.writeToFile(remoteFile, payload);

		// Act: close aggregator which collects remote to local and deletes remote
		aggregator.close();

		// Assert: local file contains the payload and remote file is deleted
		byte[] collected = Files.readAllBytes(localOutput);
		assertArrayEquals(payload, collected);
		assertFalse(Files.exists(Path.of(remoteFile)), "Remote slice should be deleted after collect");
	}

	@Test
	@DisplayName("ItemInputFileSlicer logs and skips when stream initialization fails")
	void itemInputFileSlicerHandlesStreamFactoryFailure() throws Exception {
		final FileManager mockMgr = mock(FileManager.class);
		final Path tmp = Files.createTempFile("spt-slicer", ".input");
		filesToCleanup.add(tmp);
		when(mockMgr.newTmpFileName()).thenReturn(tmp.toString());
		doAnswer(invocation -> {
			Files.deleteIfExists(tmp);
			return null;
		}).when(mockMgr).deleteFile(anyString());

		final Config baseCfg = TestConfigBuilder.config();
		final List<Config> slices = List.of(new BasicConfig(baseCfg));

		final String data = "alpha\n";
		final CsvItemInput<Item> itemInput = new CsvItemInput<>(
						new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)), new ItemFactoryImpl<>());

		ItemInputFileSlicer.setObjectOutputStreamFactoryForTesting(out -> {
			throw new IOException("stream header failure");
		});
		try {
			final ItemInputFileSlicer slicer = new ItemInputFileSlicer(
							"test-step-stream-failure", List.of(mockMgr), slices, itemInput, 1);
			assertEquals(tmp.toString(), slices.get(0).stringVal("item-input-file"));
			verify(mockMgr, never()).writeToFile(anyString(), any(byte[].class));
			slicer.close();
		} finally {
			ItemInputFileSlicer.resetObjectOutputStreamFactoryForTesting();
		}
	}
}
