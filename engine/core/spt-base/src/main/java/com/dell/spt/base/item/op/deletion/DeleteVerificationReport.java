package com.dell.spt.base.item.op.deletion;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

/** Immutable, disk-backed, selection-indexed evidence from one complete verification phase. */
public final class DeleteVerificationReport implements Serializable, AutoCloseable {
	private static final long serialVersionUID = 1L;
	private static final DeleteVerificationProbe.Presence[] PRESENCE_VALUES = DeleteVerificationProbe.Presence.values();

	private final DeleteVerificationPhase phase;
	private final String presencePath;
	private final long selected;
	private final boolean completePass;
	private final long elapsedNanos;
	private transient RandomAccessFile reader;
	private transient boolean closed;

	DeleteVerificationReport(
					final DeleteVerificationPhase phase,
					final Path presencePath,
					final long selected,
					final boolean completePass,
					final long elapsedNanos) {
		this.phase = java.util.Objects.requireNonNull(phase, "phase");
		this.presencePath = java.util.Objects.requireNonNull(presencePath, "presencePath").toString();
		this.selected = selected;
		this.completePass = completePass;
		this.elapsedNanos = Math.max(0, elapsedNanos);
	}

	DeleteVerificationReport(
					final DeleteVerificationPhase phase,
					final DeleteVerificationProbe.Presence[] presence,
					final boolean completePass,
					final long elapsedNanos) {
		this(phase, writePresenceFixture(presence), presence.length, completePass, elapsedNanos);
	}

	public DeleteVerificationPhase phase() {
		return phase;
	}

	public long selected() {
		return selected;
	}

	public boolean completePass() {
		return completePass;
	}

	public long elapsedNanos() {
		return elapsedNanos;
	}

	public Duration elapsed() {
		return Duration.ofNanos(elapsedNanos);
	}

	public synchronized DeleteVerificationProbe.Presence presence(final long selectionIndex) {
		if (selectionIndex < 0 || selectionIndex >= selected) {
			throw new IndexOutOfBoundsException("DELETE verification selection index: " + selectionIndex);
		}
		try {
			if (closed) {
				throw new IOException("DELETE verification report is closed");
			}
			if (reader == null) {
				reader = new RandomAccessFile(presencePath, "r");
			}
			reader.seek(selectionIndex);
			return decode(reader.readByte());
		} catch (final IOException failure) {
			throw new IllegalStateException("Failed to read DELETE verification evidence", failure);
		}
	}

	public long present() {
		return count(DeleteVerificationProbe.Presence.PRESENT);
	}

	public long absent() {
		return count(DeleteVerificationProbe.Presence.ABSENT);
	}

	public long unresolved() {
		return count(DeleteVerificationProbe.Presence.UNRESOLVED);
	}

	public long failureCount() {
		return phase == DeleteVerificationPhase.PRE_DELETE
						? absent() + unresolved()
						: present() + unresolved();
	}

	public boolean successful() {
		return completePass && failureCount() == 0;
	}

	Cursor cursor() throws IOException {
		if (closed) {
			throw new IOException("DELETE verification report is closed");
		}
		return new Cursor(Files.newInputStream(Path.of(presencePath)), selected);
	}

	private long count(final DeleteVerificationProbe.Presence expected) {
		long count = 0;
		try (var cursor = cursor()) {
			for (long index = 0; index < selected; index++) {
				count += cursor.next() == expected ? 1 : 0;
			}
			return count;
		} catch (final IOException failure) {
			throw new IllegalStateException("Failed to count DELETE verification evidence", failure);
		}
	}

	static byte encode(final DeleteVerificationProbe.Presence value) {
		return value.code();
	}

	static DeleteVerificationProbe.Presence decode(final byte value) {
		for (final var presence : PRESENCE_VALUES) {
			if (presence.code() == value) {
				return presence;
			}
		}
		throw new IllegalStateException("DELETE verification evidence contains an invalid presence");
	}

	private static Path writePresenceFixture(final DeleteVerificationProbe.Presence[] values) {
		Path path = null;
		try {
			path = Files.createTempFile("spt-delete-verification-fixture-", ".bin");
			try (OutputStream output = new BufferedOutputStream(Files.newOutputStream(path))) {
				for (final var value : values) {
					output.write(encode(value));
				}
			}
			return path;
		} catch (final IOException failure) {
			if (path != null) {
				try {
					Files.deleteIfExists(path);
				} catch (final IOException cleanupFailure) {
					failure.addSuppressed(cleanupFailure);
				}
			}
			throw new IllegalStateException("Failed to create DELETE verification fixture", failure);
		}
	}

	@Override
	public synchronized void close() throws IOException {
		if (closed) {
			return;
		}
		closed = true;
		IOException failure = null;
		if (reader != null) {
			try {
				reader.close();
			} catch (final IOException closeFailure) {
				failure = closeFailure;
			}
		}
		try {
			Files.deleteIfExists(Path.of(presencePath));
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

		DeleteVerificationProbe.Presence next() throws IOException {
			if (remaining <= 0) {
				throw new IOException("DELETE verification report was over-read");
			}
			final int value = input.read();
			if (value < 0) {
				throw new IOException("DELETE verification report ended early");
			}
			remaining--;
			return decode((byte) value);
		}

		@Override
		public void close() throws IOException {
			input.close();
		}
	}
}
