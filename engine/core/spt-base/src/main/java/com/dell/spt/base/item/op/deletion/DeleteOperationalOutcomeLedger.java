package com.dell.spt.base.item.op.deletion;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;

/** Disk-backed, selection-indexed operational outcomes with bounded heap use. */
public final class DeleteOperationalOutcomeLedger implements AutoCloseable {
	private static final int UNATTEMPTED = 0;
	private static final int OPERATIONAL_UNRESOLVED = 3;

	private final long selected;
	private final Path path;
	private final RandomAccessFile file;
	private boolean closed;

	private DeleteOperationalOutcomeLedger(final long selected) throws IOException {
		if (selected < 0) {
			throw new IllegalArgumentException("DELETE operational outcome count must be nonnegative");
		}
		this.selected = selected;
		path = Files.createTempFile("spt-delete-operational-outcomes-", ".bin");
		file = new RandomAccessFile(path.toFile(), "rw");
		file.setLength(selected);
	}

	/** Creates a sparse one-byte-per-identity ledger outside the managed heap. */
	public static DeleteOperationalOutcomeLedger create(final long selected) throws IOException {
		return new DeleteOperationalOutcomeLedger(selected);
	}

	/** Marks an identity as dispatched unless a terminal result is already present. */
	public synchronized void markDispatched(final long selectionIndex) {
		try {
			seek(selectionIndex);
			final int current = file.read();
			if (current == UNATTEMPTED) {
				file.seek(selectionIndex);
				file.write(OPERATIONAL_UNRESOLVED);
			}
		} catch (final IOException failure) {
			throw new IllegalStateException("Failed to record a DELETE dispatched outcome", failure);
		}
	}

	/** Records the final accepted (1) or failed (2) outcome for an identity. */
	public synchronized void markTerminal(final long selectionIndex, final boolean accepted) {
		try {
			seek(selectionIndex);
			file.write(accepted ? 1 : 2);
		} catch (final IOException failure) {
			throw new IllegalStateException("Failed to record a DELETE terminal outcome", failure);
		}
	}

	/** Returns the compact operational outcome code at one selection index. */
	public synchronized int outcome(final long selectionIndex) {
		try {
			seek(selectionIndex);
			return file.readUnsignedByte();
		} catch (final IOException failure) {
			throw new IllegalStateException("Failed to read a DELETE operational outcome", failure);
		}
	}

	/** Returns the exact disk ledger length for scale/resource assertions. */
	long storageBytes() throws IOException {
		return Files.size(path);
	}

	long selected() {
		return selected;
	}

	Cursor cursor() throws IOException {
		if (closed) {
			throw new IOException("DELETE operational outcome ledger is closed");
		}
		return new Cursor(Files.newInputStream(path), selected);
	}

	private void seek(final long selectionIndex) throws IOException {
		if (closed) {
			throw new IOException("DELETE operational outcome ledger is closed");
		}
		if (selectionIndex < 0 || selectionIndex >= selected) {
			throw new IOException("DELETE operational outcome selection index is outside the frozen inventory");
		}
		file.seek(selectionIndex);
	}

	@Override
	public synchronized void close() throws IOException {
		if (closed) {
			return;
		}
		closed = true;
		IOException failure = null;
		try {
			file.close();
		} catch (final IOException closeFailure) {
			failure = closeFailure;
		}
		try {
			Files.deleteIfExists(path);
		} catch (final IOException deleteFailure) {
			if (failure == null) {
				failure = deleteFailure;
			} else {
				failure.addSuppressed(deleteFailure);
			}
		}
		if (failure != null) {
			throw failure;
		}
	}

	static final class Cursor implements AutoCloseable {
		private final InputStream input;
		private long remaining;

		private Cursor(final InputStream input, final long remaining) {
			this.input = new BufferedInputStream(input);
			this.remaining = remaining;
		}

		int next() throws IOException {
			if (remaining <= 0) {
				throw new IOException("DELETE operational outcome ledger was over-read");
			}
			final int value = input.read();
			if (value < 0) {
				throw new IOException("DELETE operational outcome ledger ended early");
			}
			remaining--;
			return value;
		}

		@Override
		public void close() throws IOException {
			input.close();
		}
	}
}
