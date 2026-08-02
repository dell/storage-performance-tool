package com.dell.spt.base.integrity;

import com.dell.spt.base.item.io.IntegrityManifestItemInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Iterator;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/** One-pass structural and content validator for canonical integrity manifests. */
final class IntegrityManifestValidator {

	private IntegrityManifestValidator() {}

	static Evidence validate(final Path path) throws IOException {
		final MessageDigest digest;
		try {
			digest = MessageDigest.getInstance("SHA-256");
		} catch (final NoSuchAlgorithmException e) {
			throw new AssertionError(e);
		}
		try (InputStream in = Files.newInputStream(path);
						CountingDigestInputStream hashing = new CountingDigestInputStream(in, digest);
						Reader reader = new InputStreamReader(
										hashing,
										StandardCharsets.UTF_8.newDecoder()
														.onMalformedInput(CodingErrorAction.REPORT)
														.onUnmappableCharacter(CodingErrorAction.REPORT));
						CSVParser parser = CSVFormat.RFC4180.parse(reader)) {
			final Iterator<CSVRecord> records = parser.iterator();
			if (!records.hasNext() || !IntegrityManifestItemInput.HEADER.equals(records.next().toList())) {
				throw new IOException("integrity input manifest has a noncanonical header: " + path);
			}
			long rows = 0;
			ManifestIdentity prior = null;
			while (records.hasNext()) {
				final CSVRecord record = records.next();
				final ManifestIdentity current = parseIdentity(record, path);
				rows = Math.addExact(rows, 1);
				if (prior != null) {
					final int compared = IntegrityManifestOrder.compareIdentity(
									prior.bucket, prior.key, prior.versionId,
									current.bucket, current.key, current.versionId);
					if (compared == 0) {
						throw new IOException(
										"integrity manifest has duplicate identity at record "
														+ record.getRecordNumber() + ": " + path);
					}
					if (compared > 0) {
						throw new IOException(
										"integrity manifest is not strictly ordered at record "
														+ record.getRecordNumber() + ": " + path);
					}
				}
				prior = current;
			}
			return new Evidence(
							rows, hashing.count(), HexFormat.of().formatHex(digest.digest()));
		} catch (final RuntimeException e) {
			throw new IOException("invalid canonical integrity manifest " + path, e);
		}
	}

	private static ManifestIdentity parseIdentity(final CSVRecord record, final Path path)
					throws IOException {
		if (record.size() != IntegrityManifestItemInput.HEADER.size()
						|| record.get(0).isEmpty() || record.get(1).isEmpty()) {
			throw new IOException(
							"invalid integrity manifest record " + record.getRecordNumber() + ": " + path);
		}
		final long size;
		try {
			size = Long.parseLong(record.get(2));
		} catch (final NumberFormatException e) {
			throw new IOException(
							"invalid integrity manifest size at record " + record.getRecordNumber(), e);
		}
		if (size < 0) {
			throw new IOException(
							"negative integrity manifest size at record " + record.getRecordNumber());
		}
		return new ManifestIdentity(record.get(0), record.get(1), record.get(3));
	}

	static final class Evidence {
		private final long rows;
		private final long bytes;
		private final String sha256;

		private Evidence(final long rows, final long bytes, final String sha256) {
			this.rows = rows;
			this.bytes = bytes;
			this.sha256 = sha256;
		}

		long rows() {
			return rows;
		}

		long bytes() {
			return bytes;
		}

		String sha256() {
			return sha256;
		}
	}

	private static final class ManifestIdentity {
		private final String bucket;
		private final String key;
		private final String versionId;

		private ManifestIdentity(
						final String bucket,
						final String key,
						final String versionId) {
			this.bucket = bucket;
			this.key = key;
			this.versionId = versionId;
		}
	}

	private static final class CountingDigestInputStream extends DigestInputStream {
		private long count;

		private CountingDigestInputStream(final InputStream input, final MessageDigest digest) {
			super(input, digest);
		}

		@Override
		public int read() throws IOException {
			final int value = super.read();
			if (value >= 0) {
				count++;
			}
			return value;
		}

		@Override
		public int read(final byte[] data, final int offset, final int length) throws IOException {
			final int read = super.read(data, offset, length);
			if (read > 0) {
				count = Math.addExact(count, read);
			}
			return read;
		}

		private long count() {
			return count;
		}
	}
}
