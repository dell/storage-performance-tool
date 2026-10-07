package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/** Shared fixtures for endpoint-selection driver tests. */
final class EndpointSelectionTestSupport {

	static final Credential CREDENTIAL = Credential.getInstance("user1", "u5QtPuQx+W5nrrQQEg7nArBqSgC8qLiDt2RhQthb");
	static final int ITEM_SIZE = 1024;
	static final long RESULT_TIMEOUT_SECONDS = 10;
	static final String HOSTNAME = "s3.example.test";

	private EndpointSelectionTestSupport() {}

	static DataOperation<DataItem> dataOp(final OpType type, final String name) {
		return new DataOperationImpl<>(0, type, new DataItemImpl(name, name.hashCode() & 0xFFFF, ITEM_SIZE), null,
						"/bucket", CREDENTIAL, null, 0);
	}

	/** Collects published operation results. */
	static final class Results implements Output<Operation<DataItem>> {

		private final LinkedBlockingQueue<Operation<DataItem>> results = new LinkedBlockingQueue<>();

		@Override
		public boolean put(final Operation<DataItem> value) {
			return results.offer(value);
		}

		@Override
		public int put(final List<Operation<DataItem>> values, final int from, final int to) {
			for (var i = from; i < to; i++) {
				results.offer(values.get(i));
			}
			return to - from;
		}

		@Override
		public int put(final List<Operation<DataItem>> values) {
			return put(values, 0, values.size());
		}

		@Override
		public Input<Operation<DataItem>> getInput() {
			return null;
		}

		@Override
		public void close() {}

		Operation<?> poll() throws InterruptedException {
			return poll(10);
		}

		Operation<?> poll(final long millis) throws InterruptedException {
			return results.poll(millis, TimeUnit.MILLISECONDS);
		}

		Operation<?> await() throws InterruptedException {
			final var result = results.poll(RESULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			assertNotNull(result, "no operation result before the timeout");
			return result;
		}
	}
}
