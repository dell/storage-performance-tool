package com.dell.spt.base.data;

import static com.dell.spt.base.data.DataInput.generateData;
import static com.github.akurilov.commons.math.MathUtil.xorShift;
import static java.nio.ByteBuffer.allocate;
import static java.nio.ByteBuffer.allocateDirect;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.IdentityHashMap;
import java.util.Map;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntIterator;

/**
 * Data input that lazily generates and caches layers to avoid repeated allocations.
 *
 * <p>The allocated off-heap memory is calculated as layersCacheCountLimit * layerSize (worst case).
 */
public class CachedDataInput
				extends DataInputBase {

	private static final ThreadLocal<Map<CachedDataInput, Int2ObjectOpenHashMap<ByteBuffer>>> THREAD_LAYER_CACHE = ThreadLocal.withInitial(IdentityHashMap::new);

	private int layersCacheCountLimit;
	private boolean isInHeapMem;

	public CachedDataInput() {
		super();
	}

	public CachedDataInput(final ByteBuffer initialLayer, final int layersCacheCountLimit, final boolean isInHeapMem) {
		super(initialLayer);
		if (layersCacheCountLimit < 1) {
			throw new IllegalArgumentException("Cache limit value should be more than 1");
		}
		this.layersCacheCountLimit = layersCacheCountLimit;
		this.isInHeapMem = isInHeapMem;
	}

	public CachedDataInput(final CachedDataInput other) {
		super(other);
		this.layersCacheCountLimit = other.layersCacheCountLimit;
		this.isInHeapMem = other.isInHeapMem;
	}

	private long getInitialSeed() {
		return inputBuff.getLong(0);
	}

	@Override
	public final ByteBuffer getLayer(final int layerIndex)
					throws OutOfMemoryError {
		if (layerIndex == 0) {
			return inputBuff;
		}
		final Map<CachedDataInput, Int2ObjectOpenHashMap<ByteBuffer>> allCaches = THREAD_LAYER_CACHE.get();
		var layersCache = allCaches.get(this);
		if (layersCache == null) {
			final int initialCapacity = Math.max(1, layersCacheCountLimit - 1);
			layersCache = new Int2ObjectOpenHashMap<>(initialCapacity);
			allCaches.put(this, layersCache);
		}
		// check if layer exists
		var layer = layersCache.get(layerIndex - 1);
		if (layer == null) {
			final int layerSize = inputBuff.capacity();
			final int cacheCapacity = Math.max(0, layersCacheCountLimit - 1);
			if (cacheCapacity == 0) {
				layersCache.clear();
			} else {
				while (layersCache.size() >= cacheCapacity) {
					final IntIterator iterator = layersCache.keySet().iterator();
					if (!iterator.hasNext()) {
						break;
					}
					iterator.nextInt();
					iterator.remove();
				}
				layersCache.trim();
			}
			// generate the layer
			layer = isInHeapMem ? allocate(layerSize) : allocateDirect(layerSize);
			final var layerSeed = Long.reverseBytes((xorShift(getInitialSeed()) << layerIndex) ^ layerIndex);
			generateData(layer, layerSeed);
			layersCache.put(layerIndex - 1, layer);
		}
		return layer;
	}

	@Override
	public void close()
					throws IOException {
		super.close();

		final Map<CachedDataInput, Int2ObjectOpenHashMap<ByteBuffer>> allCaches = THREAD_LAYER_CACHE.get();
		final Int2ObjectMap<ByteBuffer> layersCache = allCaches.remove(this);
		if (layersCache != null) {
			layersCache.clear();
		}
	}

	@Override
	public final String toString() {
		return Long.toHexString(getInitialSeed()) + ',' + Integer.toHexString(inputBuff.capacity());
	}
}
