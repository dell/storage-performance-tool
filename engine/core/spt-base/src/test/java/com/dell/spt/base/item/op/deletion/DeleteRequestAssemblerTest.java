package com.dell.spt.base.item.op.deletion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.ConstantValueInputImpl;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationImpl;
import com.dell.spt.base.item.op.OperationAssemblyStopReason;
import com.dell.spt.base.item.op.OperationsBuilder;
import com.dell.spt.base.item.op.OperationsBuilderImpl;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.io.Input;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class DeleteRequestAssemblerTest {
	@Test
	void batchSizeOneEmitsOneLogicalRequestPerIdentityWithoutATail() throws Exception {
		final var assembler = assembler(1);
		final var operations = new ArrayList<DeleteRequestOperation>();

		final var assembled = assembler.assemble(List.of(item("a"), item("b")), operations);

		assertEquals(2, assembled.consumedIdentityCount());
		assertEquals(2, assembled.emittedOperationCount());
		assertEquals(List.of(List.of("a"), List.of("b")), operations.stream()
						.map(DeleteRequestAssemblerTest::keys).toList());
		operations.clear();
		assertEquals(0, assembler.finish(OperationAssemblyStopReason.NORMAL_COMPLETION, operations)
						.emittedOperationCount());
	}

	@Test
	void carriesOnePartialBatchAcrossReadsAndFlushesOneTailOnlyAtNormalCompletion()
					throws Exception {
		final var assembler = assembler(3);
		final var operations = new ArrayList<DeleteRequestOperation>();

		final var first = assembler.assemble(List.of(item("a"), item("b")), operations);
		assertEquals(2, first.consumedIdentityCount());
		assertEquals(0, first.emittedOperationCount());
		assertEquals(List.of(), operations);

		final var second = assembler.assemble(List.of(item("c"), item("d")), operations);
		assertEquals(2, second.consumedIdentityCount());
		assertEquals(1, second.emittedOperationCount());
		assertEquals(List.of("a", "b", "c"), keys(operations.get(0)));

		operations.clear();
		final var finish = assembler.finish(OperationAssemblyStopReason.NORMAL_COMPLETION, operations);
		assertEquals(0, finish.consumedIdentityCount());
		assertEquals(1, finish.emittedOperationCount());
		assertEquals(List.of("d"), keys(operations.get(0)));
		operations.clear();
		assertEquals(0, assembler.finish(OperationAssemblyStopReason.NORMAL_COMPLETION, operations)
						.emittedOperationCount());
	}

	@ParameterizedTest
	@EnumSource(value = OperationAssemblyStopReason.class, names = {"ADMISSION_CLOSED", "ABORTED"
	})
	void nonNormalStopReturnsRetainedTailForUnattemptedAccountingWithoutDispatch(
					final OperationAssemblyStopReason stopReason) throws Exception {
		final var assembler = assembler(100);
		final var operations = new ArrayList<DeleteRequestOperation>();

		assembler.assemble(List.of(item("a"), item("b")), operations);
		final var stopped = assembler.finish(stopReason, operations);

		assertEquals(1, stopped.emittedOperationCount());
		assertEquals(List.of("a", "b"), keys(operations.get(0)));
	}

	@Test
	void failedAssemblyRecoversEveryPreviouslyRetainedAndCurrentCallIdentity()
					throws Exception {
		final var credentialInput = new ConstantValueInputImpl<>(Credential.NONE);
		final var builder = new OperationsBuilderImpl<IntegrityManifestDataItem, Operation<IntegrityManifestDataItem>>(9) {
			private int selected;

			@Override
			public Credential nextCredential(final IntegrityManifestDataItem item)
							throws IOException {
				if (++selected == 4) {
					throw new IOException("injected assembly failure");
				}
				return super.nextCredential(item);
			}
		};
		builder.opType(OpType.DELETE).credentialInput(credentialInput);
		final var assembler = new DeleteRequestAssembler(builder, 2);
		final var operations = new ArrayList<DeleteRequestOperation>();
		assembler.assemble(List.of(item("a")), operations);

		assertThrows(
						IOException.class,
						() -> assembler.assemble(List.of(item("b"), item("c"), item("d")), operations));

		assertEquals(List.of(), operations, "tentative requests must not escape a failed assembly");
		assembler.finish(OperationAssemblyStopReason.ABORTED, operations);
		assertEquals(2, operations.size());
		assertEquals(
						List.of("a", "b", "c", "d"),
						operations.stream().flatMap(operation -> keys(operation).stream()).toList());
	}

	@Test
	void failedTargetConversionStillRecoversLaterValidIdentities() throws Exception {
		final var assembler = assembler(2);
		final var operations = new ArrayList<DeleteRequestOperation>();
		final var invalid = mock(IntegrityManifestDataItem.class);
		when(invalid.bucket()).thenReturn("bucket");
		when(invalid.name()).thenReturn("");
		when(invalid.size()).thenReturn(1L);

		assertThrows(
						IllegalArgumentException.class,
						() -> assembler.assemble(List.of(item("a"), invalid, item("c")), operations));

		assembler.finish(OperationAssemblyStopReason.ABORTED, operations);
		assertEquals(
						List.of("a", "c"),
						operations.stream().flatMap(operation -> keys(operation).stream()).toList());
		assertEquals(1, assembler.unrecoverableIdentityCount());
	}

	@Test
	void neverCombinesDifferentBucketsIntoOneRequest() throws Exception {
		final var assembler = assembler(3);
		final var operations = new ArrayList<DeleteRequestOperation>();

		assembler.assemble(
						List.of(item("bucket-a", "a"), item("bucket-b", "b")), operations);

		assertEquals(1, operations.size());
		assertEquals("bucket-a", operations.get(0).deleteRequest().bucket());
		assertEquals(List.of("a"), keys(operations.get(0)));
		operations.clear();
		assembler.finish(OperationAssemblyStopReason.NORMAL_COMPLETION, operations);
		assertEquals("bucket-b", operations.get(0).deleteRequest().bucket());
		assertEquals(List.of("b"), keys(operations.get(0)));
	}

	@Test
	void neverCombinesDifferentEffectiveCredentialsIntoOneRequest() throws Exception {
		final var firstCredential = Credential.getInstance("first", "secret-a");
		final var secondCredential = Credential.getInstance("second", "secret-b");
		final var credentials = new Input<Credential>() {
			private int next;

			@Override
			public Credential get() {
				return next++ == 0 ? firstCredential : secondCredential;
			}

			@Override
			public int get(final List<Credential> buffer, final int limit) {
				return 0;
			}

			@Override
			public long skip(final long count) {
				return 0;
			}

			@Override
			public void reset() {
				next = 0;
			}

			@Override
			public void close() {}
		};
		final var builder = new OperationsBuilderImpl<IntegrityManifestDataItem, Operation<IntegrityManifestDataItem>>(9)
						.opType(OpType.DELETE)
						.credentialInput(credentials);
		final var assembler = new DeleteRequestAssembler(builder, 3);
		final var operations = new ArrayList<DeleteRequestOperation>();

		assembler.assemble(List.of(item("a"), item("b")), operations);

		assertEquals(1, operations.size());
		assertEquals(firstCredential, operations.get(0).deleteRequest().credential());
		assertEquals(List.of("a"), keys(operations.get(0)));
		operations.clear();
		assembler.finish(OperationAssemblyStopReason.NORMAL_COMPLETION, operations);
		assertEquals(secondCredential, operations.get(0).deleteRequest().credential());
		assertEquals(List.of("b"), keys(operations.get(0)));
	}

	@Test
	void baseBuilderSelectsCredentialsWithoutBuildingDiscardedCompatibilityOperations()
					throws Exception {
		final var builtOperationCount = new AtomicInteger();
		final var builder = new OperationsBuilderImpl<IntegrityManifestDataItem, Operation<IntegrityManifestDataItem>>(9) {
			@Override
			public Operation<IntegrityManifestDataItem> buildOp(final IntegrityManifestDataItem item)
							throws IOException {
				builtOperationCount.incrementAndGet();
				return super.buildOp(item);
			}
		};
		builder.opType(OpType.DELETE).credentialInput(new ConstantValueInputImpl<>(Credential.NONE));
		final var operations = new ArrayList<DeleteRequestOperation>();

		new DeleteRequestAssembler(builder, 2).assemble(List.of(item("a"), item("b")), operations);

		assertEquals(0, builtOperationCount.get());
		assertEquals(List.of("a", "b"), keys(operations.get(0)));
	}

	@Test
	@SuppressWarnings("unchecked")
	void compatibilityDefaultPreservesCustomBuilderCredentialSelection() throws Exception {
		final var credential = Credential.getInstance("custom", "secret");
		final var builtOperationCount = new AtomicInteger();
		final var builder = (OperationsBuilder<IntegrityManifestDataItem, Operation<IntegrityManifestDataItem>>) mock(
						OperationsBuilder.class, CALLS_REAL_METHODS);
		when(builder.opType()).thenReturn(OpType.DELETE);
		when(builder.originIndex()).thenReturn(9);
		when(builder.buildOp(any())).thenAnswer(invocation -> {
			builtOperationCount.incrementAndGet();
			final var selectedItem = invocation.<IntegrityManifestDataItem> getArgument(0);
			return new OperationImpl<>(9, OpType.DELETE, selectedItem, null, null, credential);
		});
		final var operations = new ArrayList<DeleteRequestOperation>();

		new DeleteRequestAssembler(builder, 2).assemble(List.of(item("a"), item("b")), operations);

		assertEquals(2, builtOperationCount.get());
		assertEquals(credential, operations.get(0).deleteRequest().credential());
	}

	@SuppressWarnings({"rawtypes", "unchecked"
	})
	private static DeleteRequestAssembler assembler(final int batchSize) {
		final var builder = new OperationsBuilderImpl<IntegrityManifestDataItem, Operation<IntegrityManifestDataItem>>(9)
						.opType(OpType.DELETE)
						.credentialInput(new ConstantValueInputImpl<>(Credential.NONE));
		return new DeleteRequestAssembler(builder, batchSize);
	}

	private static IntegrityManifestDataItem item(final String key) {
		return item("bucket", key);
	}

	private static IntegrityManifestDataItem item(final String bucket, final String key) {
		return new IntegrityManifestDataItem(bucket, key, 1, null);
	}

	private static List<String> keys(final DeleteRequestOperation operation) {
		return operation.deleteRequest().targets().stream().map(DeleteTarget::key).toList();
	}
}
