package com.dell.spt.base.data;

import static java.nio.ByteBuffer.allocate;
import static java.nio.file.StandardOpenOption.READ;

import com.github.akurilov.commons.math.MathUtil;
import com.github.akurilov.commons.system.SizeInBytes;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 Created by kurila on 29.09.14. A finite data input for data generation purposes.
 */
public interface DataInput
				extends Closeable {

	enum Type {
		FILE, SEED
	}

	int getSize();

	ByteBuffer getLayer(final int layerIndex);

	boolean isDedupable();

	static DataInput instance(
					final String inputFilePath, final String seed, final SizeInBytes layerSize, final int layerCacheLimit,
					final boolean isInHeapMem, final double compressibility, final boolean isDedupable) throws IOException, IllegalStateException, IllegalArgumentException {
		final DataInput instance;
		final long layerSizeBytes = layerSize.get();
		if (layerSizeBytes > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Item data layer size should be less than 2GB");
		}
		if (inputFilePath != null && !inputFilePath.isEmpty()) {
			final Path p = Paths.get(inputFilePath);
			if (Files.exists(p) && !Files.isDirectory(p) && Files.isReadable(p)) {
				final File f = p.toFile();
				final long fileSize = f.length();
				if (fileSize > 0) {
					if (fileSize > Integer.MAX_VALUE) {
						throw new AssertionError("Item data input file size should be less than 2GB");
					}
					try (final ReadableByteChannel rbc = Files.newByteChannel(p, READ)) {
						instance = new ExternalDataInput(rbc, (int) layerSizeBytes, layerCacheLimit, isInHeapMem);
					}
				} else {
					throw new AssertionError("Item data input file @" + p.toAbsolutePath() + " is empty");
				}
			} else {
				throw new AssertionError(
								"Item data input file @" + p.toAbsolutePath() + " doesn't exist/not readable/is a directory");
			}
		} else {
			instance = new SeedDataInput(Long.parseLong(seed, 0x10), (int) layerSizeBytes, layerCacheLimit, isInHeapMem, compressibility, isDedupable);
		}
		return instance;
	}

	static void generateData(final ByteBuffer byteLayer, final long seed, final double compressibility) {
		final int ringBuffSize = byteLayer.capacity();
		final int countWordBytes = Long.SIZE / Byte.SIZE;
		
		final int chunkSize = 4096;
		final int uncompressibleChunkSize = (int) (chunkSize * (1.0 - (compressibility / 100.0)));
		final int uncompressibleWords = uncompressibleChunkSize / countWordBytes;
		final int uncompressibleTailBytes = uncompressibleChunkSize % countWordBytes;

		long word = seed;
		byteLayer.clear();
		
		int pos = 0;
		while (pos < ringBuffSize) {
			int currentChunkSize = Math.min(chunkSize, ringBuffSize - pos);
			int currentUncompressibleSize = Math.min(uncompressibleChunkSize, currentChunkSize);
			
			// Fill uncompressible part
			int wordsToWrite = Math.min(uncompressibleWords, currentUncompressibleSize / countWordBytes);
			for (int i = 0; i < wordsToWrite; i++) {
				byteLayer.putLong(word);
				word = MathUtil.xorShift(word);
			}
			
			int tailBytesToWrite = currentUncompressibleSize - (wordsToWrite * countWordBytes);
			if (tailBytesToWrite > 0) {
				final ByteBuffer tailBytes = allocate(countWordBytes);
				tailBytes.asLongBuffer().put(word).rewind();
				for (int i = 0; i < tailBytesToWrite; i++) {
					byteLayer.put(tailBytes.get(i));
				}
				word = MathUtil.xorShift(word);
			}
			
			// Fill compressible part (zeros)
			int compressibleSize = currentChunkSize - currentUncompressibleSize;
			for (int i = 0; i < compressibleSize; i++) {
				byteLayer.put((byte) 0);
			}
			
			pos += currentChunkSize;
		}
	}
}
