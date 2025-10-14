package com.dell.spt.storage.driver.coop.nio.fs;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.op.Operation;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Test utility: a minimal Output that collects operations in a queue
 * and allows awaiting a result with timeout.
 */
class CollectingOpOutput<I extends DataItem> implements Output<Operation<I>> {

	private final LinkedBlockingQueue<Operation<I>> results = new LinkedBlockingQueue<>();

	@Override
	public boolean put(final Operation<I> value) {
		return results.offer(value);
	}

	@Override
	public int put(final List<Operation<I>> values, final int from, final int to) {
		int n = 0;
		for (int i = from; i < to; i++) {
			if (results.offer(values.get(i))) {
				n++;
			} else {
				break;
			}
		}
		return n;
	}

	@Override
	public int put(final List<Operation<I>> values) {
		return put(values, 0, values.size());
	}

	@Override
	public Input<Operation<I>> getInput() {
		return new Input<>() {
			@Override
			public Operation<I> get() {
				return results.poll();
			}

			@Override
			public int get(final List<Operation<I>> buffer, final int limit) {
				int n = 0;
				Operation<I> v;
				while (n < limit && (v = results.poll()) != null) {
					buffer.add(v);
					n++;
				}
				return n;
			}

			@Override
			public long skip(final long count) {
				long skipped = 0;
				while (skipped < count && results.poll() != null) {
					skipped++;
				}
				return skipped;
			}

			@Override
			public void reset() {}

			@Override
			public void close() {}
		};
	}

	@Override
	public void close() {
		results.clear();
	}

	Operation<I> await(final long timeoutMs) throws InterruptedException {
		return results.poll(timeoutMs, TimeUnit.MILLISECONDS);
	}
}
