package com.dell.spt.base.data;

import static com.dell.spt.base.data.DataInput.generateData;
import static com.github.akurilov.commons.math.MathUtil.xorShift;
import static java.nio.ByteBuffer.allocate;
import static java.nio.ByteBuffer.allocateDirect;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Data input that lazily generates and caches layers to avoid repeated allocations.
 *
 * <p>The allocated off-heap memory is calculated as layersCacheCountLimit * layerSize (worst case).
 */
public class CachedDataInput
				extends DataInputBase {

	private int layersCacheCountLimit;
	private boolean isInHeapMem;
	@SuppressWarnings("ThreadLocalNotStaticFinal")
	private final ThreadLocal<Int2ObjectOpenHashMap<ByteBuffer>> thrLocLayersCache = new ThreadLocal<>();

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
		var layersCache = thrLocLayersCache.get();
		if (layersCache == null) {
			layersCache = new Int2ObjectOpenHashMap<>(layersCacheCountLimit - 1);
			thrLocLayersCache.set(layersCache);
		}
		// check if layer exists
		var layer = layersCache.get(layerIndex - 1);
		if (layer == null) {
			// check if it's necessary to free the space first
			var layersCountToFree = layersCacheCountLimit - layersCache.size() + 1;
			final var layerSize = inputBuff.capacity();
			if (layersCountToFree > 0) {
				for (final int i : layersCache.keySet()) {
					layer = layersCache.remove(i);
					if (layer != null) {
						layersCountToFree--;
						if (layersCountToFree == 0) {
							break;
						}
					}
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
		final var layersCache = (Int2ObjectMap<ByteBuffer>) thrLocLayersCache.get();
		if (layersCache != null) {
			layersCache.clear();
			thrLocLayersCache.set(null);
		}
	}

	@Override
	public final String toString() {
		return Long.toHexString(getInitialSeed()) + ',' + Integer.toHexString(inputBuff.capacity());
	}
}
