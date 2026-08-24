package com.dell.spt.base.load.step.client;

import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;

import com.dell.spt.base.integrity.FailurePreservingCleanup;
import com.dell.spt.base.integrity.IntegrityManifestCompletion;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException.Category;
import com.dell.spt.base.item.op.deletion.DeleteArtifacts;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** Collects per-node DELETE evidence and publishes the canonical aggregate last. */
final class DeleteArtifactAggregator implements AutoCloseable {
	private record SourceNames(
					String totals, String requests, String objects, String residual, String verification) {}

	private final String stepId;
	private final List<FileManager> fileManagers;
	private final List<String> contributorIds;
	private final List<SourceNames> sourceNames;
	private final Path outputDirectory;
	private final Path selection;
	private final Path selectionCompletion;

	DeleteArtifactAggregator(
					final String stepId,
					final List<FileManager> fileManagers,
					final List<String> contributorIds,
					final Path selection,
					final Path selectionCompletion)
					throws IOException {
		if (fileManagers.isEmpty() || fileManagers.size() != contributorIds.size()) {
			throw new IOException("DELETE contributor identity is incomplete");
		}
		this.stepId = stepId;
		this.fileManagers = List.copyOf(fileManagers);
		this.contributorIds = List.copyOf(contributorIds);
		this.selection = selection.toAbsolutePath().normalize();
		this.selectionCompletion = selectionCompletion.toAbsolutePath().normalize();
		this.sourceNames = new ArrayList<>(fileManagers.size());
		for (final FileManager fileManager : fileManagers) {
			sourceNames.add(new SourceNames(
							fileManager.logFileName(Loggers.DELETE_METRICS_TOTAL.getName(), stepId),
							fileManager.logFileName(Loggers.DELETE_REQUESTS.getName(), stepId),
							fileManager.logFileName(Loggers.DELETE_OBJECTS.getName(), stepId),
							fileManager.logFileName(Loggers.DELETE_RESIDUAL.getName(), stepId),
							fileManager.logFileName(Loggers.DELETE_VERIFICATION.getName(), stepId)));
		}
		final SourceNames local = sourceNames.get(0);
		this.outputDirectory = requireLocalCanonical(local.totals(), DeleteArtifacts.METRICS_FILE_NAME).getParent();
		requireSameOutput(local.requests(), DeleteArtifacts.REQUESTS_FILE_NAME);
		requireSameOutput(local.objects(), DeleteArtifacts.OBJECTS_FILE_NAME);
		requireSameOutput(local.residual(), DeleteArtifacts.RESIDUAL_FILE_NAME);
		requireSameOutput(local.verification(), DeleteArtifacts.VERIFICATION_FILE_NAME);
	}

	@Override
	public void close() {
		try {
			collectAndPublish();
		} catch (final IntegrityTerminalException e) {
			throw e;
		} catch (final Exception e) {
			throw new IntegrityTerminalException(
							Category.AGGREGATION, stepId, "failed to collect and publish DELETE evidence", e);
		}
	}

	private void collectAndPublish() throws IOException {
		DeleteArtifactAggregation.retainSelection(
						stepId, outputDirectory, selection, selectionCompletion);
		LogUtil.flushAll();
		Files.createDirectories(outputDirectory);
		final List<DeleteArtifactAggregation.NodeSource> sources = new ArrayList<>(sourceNames.size());
		for (int nodeIndex = 0; nodeIndex < sourceNames.size(); nodeIndex++) {
			final SourceNames names = sourceNames.get(nodeIndex);
			sources.add(new DeleteArtifactAggregation.NodeSource(
							collect(nodeIndex, names.totals(), DeleteArtifacts.METRICS_FILE_NAME),
							collect(nodeIndex, names.requests(), DeleteArtifacts.REQUESTS_FILE_NAME),
							collect(nodeIndex, names.objects(), DeleteArtifacts.OBJECTS_FILE_NAME),
							collect(nodeIndex, names.residual(), DeleteArtifacts.RESIDUAL_FILE_NAME),
							collect(nodeIndex, names.verification(), DeleteArtifacts.VERIFICATION_FILE_NAME)));
		}
		DeleteArtifactAggregation.publish(
						stepId, outputDirectory, sources, contributorIds, selection, selectionCompletion);
	}

	private Path collect(final int nodeIndex, final String remoteName, final String artifactName)
					throws IOException {
		final Path canonical = outputDirectory.resolve(artifactName);
		final Path nodeSource = nodeSourcePath(canonical, nodeIndex);
		if (Files.isRegularFile(nodeSource)) {
			return nodeSource;
		}
		if (nodeIndex == 0) {
			if (!canonical.equals(Path.of(remoteName).toAbsolutePath().normalize())
							|| !Files.isRegularFile(canonical)) {
				throw new IOException("entry-node DELETE artifact is missing: " + canonical);
			}
			IntegrityManifestCompletion.atomicMove(canonical, nodeSource);
			return nodeSource;
		}
		copyRemoteSource(fileManagers.get(nodeIndex), remoteName, nodeSource);
		return nodeSource;
	}

	private static void copyRemoteSource(
					final FileManager fileManager, final String remoteName, final Path target) throws IOException {
		final Path staging = Files.createTempFile(
						target.getParent(), "." + target.getFileName() + ".", ".fetching");
		FailurePreservingCleanup.always(() -> {
			long offset = 0;
			boolean sawData = false;
			try {
				while (true) {
					final byte[] bytes = fileManager.readFromFile(remoteName, offset);
					if (bytes.length == 0) {
						break;
					}
					Files.write(
									staging,
									bytes,
									sawData
													? new StandardOpenOption[]{StandardOpenOption.APPEND
					}
													: new StandardOpenOption[]{StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE
					});
					sawData = true;
					offset = Math.addExact(offset, bytes.length);
				}
			} catch (final EOFException expected) {
				// FileManager signals normal EOF with this exception.
			} catch (final Exception e) {
				throwUncheckedIfInterrupted(e);
				throw new IOException("failed to fetch remote DELETE artifact " + remoteName, e);
			}
			if (!sawData) {
				throw new IOException("remote DELETE artifact was empty or missing: " + remoteName);
			}
			IntegrityManifestCompletion.atomicMove(staging, target);
			return null;
		}, () -> Files.deleteIfExists(staging));
	}

	private Path requireLocalCanonical(final String path, final String artifactName) throws IOException {
		final Path canonical = Path.of(path).toAbsolutePath().normalize();
		if (!artifactName.equals(canonical.getFileName().toString())) {
			throw new IOException("DELETE logger path " + canonical + " does not match " + artifactName);
		}
		return canonical;
	}

	private void requireSameOutput(final String path, final String artifactName) throws IOException {
		final Path canonical = requireLocalCanonical(path, artifactName);
		if (!outputDirectory.equals(canonical.getParent())) {
			throw new IOException("DELETE logger artifacts do not share one step-scoped directory");
		}
	}

	static Path nodeSourcePath(final Path artifact, final int nodeIndex) {
		final String name = artifact.getFileName().toString();
		final int suffix = name.toLowerCase(Locale.ROOT).lastIndexOf(".csv");
		final String stem = suffix >= 0 ? name.substring(0, suffix) : name;
		return artifact.resolveSibling(String.format(Locale.ROOT, "%s.node-%03d.csv", stem, nodeIndex));
	}
}
