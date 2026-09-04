package com.dell.spt.base.item.io;

import com.github.akurilov.commons.io.Input;

/**
 * Finite input which exposes its unread identity count without consuming or materializing those
 * identities.
 *
 * <p>The count is used by bounded cancellation paths which must reconcile a frozen selection
 * without walking the unread suffix during shutdown.
 */
public interface RemainingItemCountInput<T> extends Input<T> {

	/**
	 * Returns the exact number of identities not yet returned by this input in bounded time and
	 * without input I/O.
	 */
	long remainingItemCount();
}
