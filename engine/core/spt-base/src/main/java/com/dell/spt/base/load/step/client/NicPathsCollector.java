package com.dell.spt.base.load.step.client;

import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;

import com.dell.spt.base.integrity.IntegrityManifestCompletion;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.nic.NicPathsArtifact;
import com.dell.spt.base.nic.NicSetArtifact;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.logging.log4j.Logger;

/**
 * Copies each node's {@link NicPathsArtifact} and {@link NicSetArtifact} into the Entry Node's step
 * folder as {@code nic.paths.node-NNN.csv} and {@code nic.set.node-NNN.json}, where NNN is the
 * slice index (item 18, D64). The files are not merged, and a node's missing file is a WARN rather
 * than a step failure (D60); the CLI reports it.
 */
final class NicPathsCollector implements AutoCloseable {

	/** One file each NIC-set node writes, and its name in the messages. */
	private record Evidence(Logger logger, String fileName, String what) {}

	private static final List<Evidence> EVIDENCE = List.of(
					new Evidence(Loggers.NIC_PATHS, NicPathsArtifact.FILE_NAME, "NIC paths"),
					new Evidence(Loggers.NIC_SET, NicSetArtifact.FILE_NAME, "NIC set"));

	private final String loadStepId;
	private final List<FileManager> fileManagers;
	private final List<String> nodeNames;
	private final List<Boolean> expected;

	/**
	 * @param fileManagers one per slice, the Entry Node's first
	 * @param nodeNames one per slice, for the messages
	 * @param expected one per slice: whether that slice requested a NIC set
	 */
	NicPathsCollector(
					final String loadStepId,
					final List<FileManager> fileManagers,
					final List<String> nodeNames,
					final List<Boolean> expected) {
		if (fileManagers.size() != nodeNames.size() || fileManagers.size() != expected.size()) {
			throw new IllegalArgumentException("one file manager, name and expectation per slice");
		}
		this.loadStepId = loadStepId;
		this.fileManagers = List.copyOf(fileManagers);
		this.nodeNames = List.copyOf(nodeNames);
		this.expected = List.copyOf(expected);
	}

	@Override
	public void close() {
		for (int i = 0; i < fileManagers.size(); i++) {
			if (!expected.get(i)) {
				continue;
			}
			for (final var evidence : EVIDENCE) {
				try {
					collect(i, evidence);
				} catch (final Exception e) {
					throwUncheckedIfInterrupted(e);
					Loggers.MSG.warn("{}: no {} file from node {} ({}): {}", loadStepId, evidence.what(),
									String.format("%03d", i), nodeNames.get(i), e.getMessage());
				}
			}
		}
	}

	private void collect(final int index, final Evidence evidence) throws IOException {
		final var fileManager = fileManagers.get(index);
		final var sourceName = fileManager.logFileName(evidence.logger().getName(), loadStepId);
		// Every slice shares the step id, so the Entry Node's own name places the copies.
		final var canonical = Path.of(fileManagers.get(0).logFileName(evidence.logger().getName(), loadStepId))
						.toAbsolutePath();
		if (!evidence.fileName().equals(canonical.getFileName().toString())) {
			throw new IOException("logger path " + canonical + " does not match " + evidence.fileName());
		}
		final var target = nodeCopyPath(canonical, index);
		if (Files.exists(target)) {
			throw new IOException("a stale node file exists: " + target);
		}
		if (index == 0) {
			if (!Files.isRegularFile(canonical)) {
				throw new IOException("the file is missing: " + canonical);
			}
			IntegrityManifestCompletion.atomicMove(canonical, target);
			return;
		}
		Files.createDirectories(canonical.getParent());
		final var staging = Files.createTempFile(canonical.getParent(), ".nic-node-", ".tmp");
		try {
			copyRemote(fileManager, sourceName, staging);
			IntegrityManifestCompletion.atomicMove(staging, target);
		} finally {
			Files.deleteIfExists(staging);
		}
	}

	/** {@code nic.paths.csv} as {@code nic.paths.node-NNN.csv}: the slice index before the extension. */
	static Path nodeCopyPath(final Path canonical, final int index) {
		final var name = canonical.getFileName().toString();
		final var dot = name.lastIndexOf('.');
		return canonical.resolveSibling(
						String.format(Locale.ROOT, "%s.node-%03d%s", name.substring(0, dot), index, name.substring(dot)));
	}

	private static void copyRemote(final FileManager fileManager, final String remoteName, final Path target)
					throws IOException {
		long offset = 0;
		try {
			while (true) {
				final byte[] bytes = fileManager.readFromFile(remoteName, offset);
				if (bytes.length == 0) {
					break;
				}
				Files.write(target, bytes, StandardOpenOption.APPEND);
				offset += bytes.length;
			}
		} catch (final EOFException expected) {
			// FileManager signals normal EOF with this exception.
		} catch (final Exception e) {
			throwUncheckedIfInterrupted(e);
			throw new IOException("failed to fetch " + remoteName, e);
		}
		if (offset == 0) {
			throw new IOException("the file is empty or missing: " + remoteName);
		}
	}

	/** One name per slice for the messages: {@code entry}, then the node addresses. */
	static List<String> nodeNames(final List<String> nodeAddrs) {
		final var names = new ArrayList<String>(1 + nodeAddrs.size());
		names.add("entry");
		names.addAll(nodeAddrs);
		return names;
	}
}
