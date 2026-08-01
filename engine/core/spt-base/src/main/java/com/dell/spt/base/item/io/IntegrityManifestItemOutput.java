package com.dell.spt.base.item.io;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;

import com.dell.spt.base.integrity.IntegrityCsvFormat;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.io.file.FileOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.csv.CSVPrinter;

/** Canonical manifest source writer that seeds its header at construction. */
public final class IntegrityManifestItemOutput
				implements Output<IntegrityManifestDataItem>, FileOutput<IntegrityManifestDataItem> {

	private final Path manifestPath;
	private final CSVPrinter printer;

	public IntegrityManifestItemOutput(final Path manifestPath) throws IOException {
		this.manifestPath = manifestPath;
		printer = new CSVPrinter(
						Files.newBufferedWriter(manifestPath, StandardCharsets.UTF_8, CREATE_NEW, WRITE),
						IntegrityCsvFormat.RFC4180_LF);
		printer.printRecord(IntegrityManifestItemInput.HEADER);
		printer.flush();
	}

	@Override
	public boolean put(final IntegrityManifestDataItem item) {
		try {
			printer.printRecord(
							item.bucket(),
							item.name(),
							item.size(),
							item.versionId() == null ? "" : item.versionId());
			return true;
		} catch (final IOException e) {
			throwUnchecked(e);
			return false;
		}
	}

	@Override
	public int put(final List<IntegrityManifestDataItem> items, final int from, final int to) {
		int index = from;
		while (index < to && put(items.get(index))) {
			index++;
		}
		return index - from;
	}

	@Override
	public int put(final List<IntegrityManifestDataItem> items) {
		return put(items, 0, items.size());
	}

	public void flush() throws IOException {
		printer.flush();
	}

	@Override
	public IntegrityManifestItemInput getInput() {
		try {
			printer.flush();
			return new IntegrityManifestItemInput(manifestPath);
		} catch (final IOException e) {
			throwUnchecked(e);
			return null;
		}
	}

	@Override
	public Path filePath() {
		return manifestPath;
	}

	@Override
	public void close() {
		try {
			printer.close(true);
		} catch (final IOException e) {
			throwUnchecked(e);
		}
	}
}
