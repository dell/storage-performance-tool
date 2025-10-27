package com.dell.spt.base.data;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class CachedDataInputTest {

	private CachedDataInput inputUnderTest;

	@AfterEach
	void tearDown() throws Exception {
		if (inputUnderTest != null) {
			inputUnderTest.close();
		}
	}

	@Test
	void layerZeroReturnsInitialBuffer() {
		final ByteBuffer initial = ByteBuffer.allocate(16);
		initial.putLong(0, 0x1234_5678_9ABCDEFL);
		inputUnderTest = new CachedDataInput(initial, /*layersCacheCountLimit*/ 4, true);

		ByteBuffer layerZero = inputUnderTest.getLayer(0);
		assertSame(initial, layerZero, "Layer 0 should be the initial buffer");
		assertEquals(16, layerZero.capacity());
	}

	@Test
	void layersAreCachedWithinSameThread() throws Exception {
		final ByteBuffer initial = ByteBuffer.allocate(16);
		initial.putLong(0, 0xCAFEBABECAFED00DL);
		inputUnderTest = new CachedDataInput(initial, 4, true);

		ByteBuffer first = inputUnderTest.getLayer(1);
		ByteBuffer second = inputUnderTest.getLayer(1);
		assertSame(first, second, "Layer should be cached for the same thread");

		AtomicReference<ByteBuffer> otherThreadLayer = new AtomicReference<>();
		CountDownLatch ready = new CountDownLatch(1);
		CountDownLatch done = new CountDownLatch(1);

		Thread worker = new Thread(() -> {
			ready.countDown();
			try {
				otherThreadLayer.set(inputUnderTest.getLayer(1));
			} finally {
				done.countDown();
			}
		});

		worker.start();
		ready.await();
		done.await();
		worker.join();

		assertNotSame(first, otherThreadLayer.get(), "Each thread should receive its own cached layer");
	}

	@Test
	void closeClearsThreadLocalCache() throws Exception {
		final ByteBuffer initial = ByteBuffer.allocate(16);
		initial.putLong(0, 0x0FEDCBA987654321L);
		inputUnderTest = new CachedDataInput(initial, 4, true);

		inputUnderTest.getLayer(1);
		ThreadLocal<Map<CachedDataInput, ?>> cache = accessLayersThreadLocal();
		assertTrue(cache.get().containsKey(inputUnderTest), "Cache should be populated before close");

		inputUnderTest.close();
		assertFalse(cache.get().containsKey(inputUnderTest), "Cache should be cleared after close");
		inputUnderTest = null;
	}

	@Test
	void cacheLimitIsRespected() throws Exception {
		final ByteBuffer initial = ByteBuffer.allocate(16);
		initial.putLong(0, 0x1111_2222_3333_4444L);
		inputUnderTest = new CachedDataInput(initial, 3, true);

		inputUnderTest.getLayer(1);
		inputUnderTest.getLayer(2);
		inputUnderTest.getLayer(3);

		ThreadLocal<Map<CachedDataInput, ?>> cache = accessLayersThreadLocal();
		@SuppressWarnings("unchecked")
		var layersCache = (Map<?, ?>) cache.get().get(inputUnderTest);
		assertNotNull(layersCache);
		assertEquals(2, layersCache.size(), "Cache should not exceed configured limit");
	}

	@SuppressWarnings("unchecked")
	private static ThreadLocal<Map<CachedDataInput, ?>> accessLayersThreadLocal() throws Exception {
		Field field = CachedDataInput.class.getDeclaredField("THREAD_LAYER_CACHE");
		field.setAccessible(true);
		return (ThreadLocal<Map<CachedDataInput, ?>>) field.get(null);
	}
}
