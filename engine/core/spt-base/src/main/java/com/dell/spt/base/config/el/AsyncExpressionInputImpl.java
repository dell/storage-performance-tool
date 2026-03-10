package com.dell.spt.base.config.el;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.concurrent.TaskBase;
import com.github.akurilov.commons.io.el.ExpressionInput;
import com.github.akurilov.commons.io.el.SynchronousExpressionInput;
import java.util.List;
import javax.el.ELException;
import javax.el.PropertyNotFoundException;

class AsyncExpressionInputImpl<T> extends TaskBase implements AsyncExpressionInput<T> {

	private ExpressionInput<T> input;

	AsyncExpressionInputImpl(final ExpressionInput<T> input) {
		super(ServiceTaskExecutor.VT_EXECUTOR);
		if (input instanceof SynchronousExpressionInput) {
			throw new IllegalArgumentException("An expression input to wrap should not be synchronous");
		}
		this.input = input;
	}

	@Override
	public final T call() {
		return last();
	}

	@Override
	public T last() {
		return input.last();
	}

	@Override
	public T get() throws PropertyNotFoundException, ELException {
		return input.get();
	}

	@Override
	public int get(final List<T> buffer, final int limit)
					throws PropertyNotFoundException, ELException {
		return input.get(buffer, limit);
	}

	@Override
	public long skip(final long count) {
		return input.skip(count);
	}

	@Override
	public void reset() {
		input.reset();
	}

	@Override
	public String expr() {
		return input.expr();
	}

	@Override
	protected final void doWork() throws Exception {
		input.call();
		Thread.sleep(1);
	}

	@Override
	protected final void doClose() {
		try {
			input.close();
		} catch (final Exception e) {
			throwUnchecked(e);
		}
	}
}
