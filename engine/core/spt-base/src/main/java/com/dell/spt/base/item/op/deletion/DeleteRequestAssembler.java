package com.dell.spt.base.item.op.deletion;

import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationAssembler;
import com.dell.spt.base.item.op.OperationAssemblyResult;
import com.dell.spt.base.item.op.OperationAssemblyStopReason;
import com.dell.spt.base.item.op.OperationsBuilder;
import com.dell.spt.base.storage.Credential;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/** Streaming same-bucket standalone DELETE request assembler. */
public final class DeleteRequestAssembler
				implements OperationAssembler<IntegrityManifestDataItem, DeleteRequestOperation> {
	private record RecoveryTarget(DeleteTarget target, Credential credential) {}

	private final OperationsBuilder<IntegrityManifestDataItem, Operation<IntegrityManifestDataItem>> operationsBuilder;
	private final int batchSize;
	private final ArrayList<DeleteTarget> retainedTargets;
	private final ArrayList<RecoveryTarget> abortRecoveryTargets;
	private String retainedBucket;
	private Credential retainedCredential;
	private int unrecoverableIdentityCount;
	private long nextSelectionIndex;
	private boolean assemblyFailed;
	private boolean finished;

	/**
	 * Creates a same-bucket, same-credential streaming assembler while retaining ownership of the
	 * configured single-item builder.
	 */
	@SuppressWarnings("unchecked")
	public DeleteRequestAssembler(
					final OperationsBuilder<IntegrityManifestDataItem, ? extends Operation<IntegrityManifestDataItem>> operationsBuilder,
					final int batchSize) {
		if (batchSize < 1 || batchSize > DeleteRequest.MAX_TARGET_COUNT) {
			throw new IllegalArgumentException(
							"Standalone DELETE batch size must be between 1 and "
											+ DeleteRequest.MAX_TARGET_COUNT);
		}
		this.operationsBuilder = (OperationsBuilder<IntegrityManifestDataItem, Operation<IntegrityManifestDataItem>>) operationsBuilder;
		if (this.operationsBuilder.opType() != OpType.DELETE) {
			throw new IllegalArgumentException("Standalone DELETE assembler requires DELETE operations");
		}
		this.batchSize = batchSize;
		this.retainedTargets = new ArrayList<>(batchSize);
		this.abortRecoveryTargets = new ArrayList<>(batchSize);
	}

	@Override
	public int originIndex() {
		return operationsBuilder.originIndex();
	}

	@Override
	public OpType opType() {
		return OpType.DELETE;
	}

	@Override
	public OperationAssemblyResult assemble(
					final List<IntegrityManifestDataItem> items,
					final List<DeleteRequestOperation> operations)
					throws IOException {
		Objects.requireNonNull(items, "DELETE assembler items");
		Objects.requireNonNull(operations, "DELETE assembler output");
		if (finished || assemblyFailed) {
			throw new IllegalStateException("Standalone DELETE assembler is already finished");
		}
		final var outputSizeBefore = operations.size();
		final var assembledOperations = new ArrayList<DeleteRequestOperation>();
		final var workingTargets = new ArrayList<>(retainedTargets);
		final var inputTargets = new ArrayList<DeleteTarget>(items.size());
		final var recoveryTargets = new ArrayList<RecoveryTarget>(items.size());
		var workingBucket = retainedBucket;
		var workingCredential = retainedCredential;
		RuntimeException targetFailure = null;
		for (final var item : items) {
			try {
				inputTargets.add(new DeleteTarget(item, nextSelectionIndex));
				nextSelectionIndex = Math.addExact(nextSelectionIndex, 1);
			} catch (final RuntimeException e) {
				unrecoverableIdentityCount++;
				if (targetFailure == null) {
					targetFailure = e;
				} else {
					targetFailure.addSuppressed(e);
				}
			}
		}
		if (targetFailure != null) {
			for (final var target : inputTargets) {
				recoveryTargets.add(new RecoveryTarget(target, Credential.NONE));
			}
			prepareAbortRecovery(recoveryTargets);
			throw targetFailure;
		}
		try {
			for (final var target : inputTargets) {
				recoveryTargets.add(new RecoveryTarget(target, Credential.NONE));
				final var selectedCredential = operationsBuilder.nextCredential(target.item());
				final var credential = selectedCredential == null ? Credential.NONE : selectedCredential;
				recoveryTargets.set(
								recoveryTargets.size() - 1, new RecoveryTarget(target, credential));
				if (!workingTargets.isEmpty()
								&& (!target.bucket().equals(workingBucket)
												|| !credential.equals(workingCredential))) {
					emit(workingTargets, workingBucket, workingCredential, assembledOperations);
				}
				if (workingTargets.isEmpty()) {
					workingBucket = target.bucket();
					workingCredential = credential;
				}
				workingTargets.add(target);
				if (workingTargets.size() == batchSize) {
					emit(workingTargets, workingBucket, workingCredential, assembledOperations);
					workingBucket = null;
					workingCredential = null;
				}
			}
			operations.addAll(assembledOperations);
			retainedTargets.clear();
			retainedTargets.addAll(workingTargets);
			retainedBucket = workingBucket;
			retainedCredential = workingCredential;
			return new OperationAssemblyResult(items.size(), operations.size() - outputSizeBefore);
		} catch (final IOException | RuntimeException e) {
			while (recoveryTargets.size() < inputTargets.size()) {
				recoveryTargets.add(new RecoveryTarget(
								inputTargets.get(recoveryTargets.size()), Credential.NONE));
			}
			prepareAbortRecovery(recoveryTargets);
			throw e;
		}
	}

	/** Returns failed-call identities which could not be converted into canonical recovery targets. */
	public int unrecoverableIdentityCount() {
		return unrecoverableIdentityCount;
	}

	@Override
	public OperationAssemblyResult finish(
					final OperationAssemblyStopReason reason,
					final List<DeleteRequestOperation> operations) {
		Objects.requireNonNull(reason, "DELETE assembler stop reason");
		Objects.requireNonNull(operations, "DELETE assembler output");
		if (finished) {
			return new OperationAssemblyResult(0, 0);
		}
		if (assemblyFailed && reason != OperationAssemblyStopReason.ABORTED) {
			throw new IllegalStateException(
							"Failed standalone DELETE assembly must finish as ABORTED");
		}
		finished = true;
		final var outputSizeBefore = operations.size();
		if (assemblyFailed) {
			emitAbortRecovery(operations);
		} else {
			emitRetained(operations);
		}
		return new OperationAssemblyResult(0, operations.size() - outputSizeBefore);
	}

	private void prepareAbortRecovery(final List<RecoveryTarget> currentCallTargets) {
		assemblyFailed = true;
		for (final var target : retainedTargets) {
			abortRecoveryTargets.add(new RecoveryTarget(target, retainedCredential));
		}
		abortRecoveryTargets.addAll(currentCallTargets);
		retainedTargets.clear();
		retainedBucket = null;
		retainedCredential = null;
	}

	private void emitAbortRecovery(final List<DeleteRequestOperation> operations) {
		final var targets = new ArrayList<DeleteTarget>(batchSize);
		final var identities = new HashSet<DeleteTargetIdentity>(batchSize);
		String bucket = null;
		Credential credential = null;
		for (final var recovery : abortRecoveryTargets) {
			final var target = recovery.target();
			if (!targets.isEmpty()
							&& (targets.size() == batchSize
											|| !target.bucket().equals(bucket)
											|| !recovery.credential().equals(credential)
											|| identities.contains(target.identity()))) {
				emit(targets, bucket, credential, operations);
				identities.clear();
			}
			if (targets.isEmpty()) {
				bucket = target.bucket();
				credential = recovery.credential();
			}
			targets.add(target);
			identities.add(target.identity());
		}
		if (!targets.isEmpty()) {
			emit(targets, bucket, credential, operations);
		}
		abortRecoveryTargets.clear();
	}

	private void emitRetained(final List<DeleteRequestOperation> operations) {
		if (retainedTargets.isEmpty()) {
			return;
		}
		emit(retainedTargets, retainedBucket, retainedCredential, operations);
		retainedBucket = null;
		retainedCredential = null;
	}

	private void emit(
					final List<DeleteTarget> targets,
					final String bucket,
					final Credential credential,
					final List<DeleteRequestOperation> operations) {
		operations.add(new DeleteRequestOperationImpl(
						originIndex(), new DeleteRequest(bucket, credential, targets)));
		targets.clear();
	}

	@Override
	public void close() {
		finished = true;
		retainedTargets.clear();
		abortRecoveryTargets.clear();
		operationsBuilder.close();
	}
}
