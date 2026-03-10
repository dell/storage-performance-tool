package com.dell.spt.base.config.el;

import com.dell.spt.base.concurrent.Task;
import com.github.akurilov.commons.io.el.ExpressionInput;

public interface AsyncExpressionInput<T> extends ExpressionInput<T>, Task {

	/**
	* @return last value, without re-evaluation
	*/
	@Override
	T call();
}
