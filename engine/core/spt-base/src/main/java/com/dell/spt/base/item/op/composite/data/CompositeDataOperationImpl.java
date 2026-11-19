package com.dell.spt.base.item.op.composite.data;

import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.partial.data.PartialDataOperation;
import com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.collection.Range;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Created by andrey on 25.11.16. */
public class CompositeDataOperationImpl<I extends DataItem> extends DataOperationImpl<I>
				implements CompositeDataOperation<I> {

	private long sizeThreshold;
	private final AtomicInteger pendingSubTasksCount = new AtomicInteger(-1);

	private final Map<String, String> contextData = new ConcurrentHashMap<>();
	private final List<PartialDataOperation<I>> subTasks = new ArrayList<>();
	private final AtomicInteger subTaskYieldIndex = new AtomicInteger(0);
	private volatile boolean rangesSentAsRequests;

	public CompositeDataOperationImpl() {
		super();
	}

	public CompositeDataOperationImpl(
					final int originIndex,
					final OpType opType,
					final I item,
					final String srcPath,
					final String dstPath,
					final Credential credential,
					final List<Range> fixedRanges,
					final int randomRangesCount,
					final long sizeThreshold) {
		super(originIndex, opType, item, srcPath, dstPath, credential, fixedRanges, randomRangesCount);
		this.sizeThreshold = sizeThreshold;
	}

	protected CompositeDataOperationImpl(final CompositeDataOperationImpl<I> other) {
		super(other);
		this.sizeThreshold = other.sizeThreshold;
		this.pendingSubTasksCount.set(other.pendingSubTasksCount.get());
		this.rangesSentAsRequests = other.rangesSentAsRequests;
	}

	/** Also clears the {@link #rangesSentAsRequests()} mark, which belongs to the attempt it was set in. */
	@Override
	public void reset() throws IllegalArgumentException {
		super.reset();
		rangesSentAsRequests = false;
	}

	/**
	 * Whether the driver sent this attempt's sub-operations as its only requests: the operation's own
	 * passes sent no request and did no I/O, so its sub-operations' timings are the only request timings
	 * it has, and the load step records theirs and none of its own. Only a READ is read for it.
	 *
	 * <p>The driver sets it with {@link #markRangesSentAsRequests()}; {@link #reset()}, which admission
	 * calls before each attempt, and {@link #resetSubOperations()}, which a whole retry calls, clear it.
	 * {@link #result()} copies it. A subclass must not use these two names for anything else.
	 */
	public boolean rangesSentAsRequests() {
		return rangesSentAsRequests;
	}

	/**
	 * Called by a driver that sends this attempt's sub-operations as its only requests, as the {@code
	 * s3}, {@code s3-rdma} and {@code s3-aws} drivers do for a read split by the size threshold.
	 */
	public void markRangesSentAsRequests() {
		rangesSentAsRequests = true;
	}

	@Override
	public final String get(final String key) {
		return contextData.get(key);
	}

	@Override
	public final void put(final String key, final String value) {
		contextData.put(key, value);
	}

	@Override
	public final List<? extends PartialDataOperation<I>> subOperations() {

		if (!subTasks.isEmpty()) {
			return subTasks;
		}

		final int equalPartsCount = sizeThreshold > 0 ? (int) (contentSize / sizeThreshold) : 0;
		final long tailPartSize = contentSize % sizeThreshold;
		I nextPart;
		PartialDataOperation<I> nextSubTask;
		for (int i = 0; i < equalPartsCount; i++) {
			nextPart = (I) item.slice(i * sizeThreshold, sizeThreshold);
			nextSubTask = new PartialDataOperationImpl<>(
							originIndex, opType, nextPart, srcPath, dstPath, credential, i, this);
			nextSubTask.srcPath(srcPath);
			subTasks.add(nextSubTask);
		}
		if (tailPartSize > 0) {
			nextPart = (I) item.slice(equalPartsCount * sizeThreshold, tailPartSize);
			nextSubTask = new PartialDataOperationImpl<>(
							originIndex, opType, nextPart, srcPath, dstPath, credential, equalPartsCount, this);
			nextSubTask.srcPath(srcPath);
			subTasks.add(nextSubTask);
		}

		pendingSubTasksCount.set(subTasks.size());

		return subTasks;
	}

	@Override
	public final List<? extends PartialDataOperation<I>> nextSubOperations(int limit) {
		final List<? extends PartialDataOperation<I>> allOps = subOperations();
		if (limit <= 0 || limit >= allOps.size()) {
			int current = subTaskYieldIndex.getAndSet(allOps.size());
			if (current >= allOps.size()) {
				return java.util.Collections.emptyList();
			}
			return allOps.subList(current, allOps.size());
		}

		while (true) {
			int current = subTaskYieldIndex.get();
			if (current >= allOps.size()) {
				return java.util.Collections.emptyList();
			}
			int next = Math.min(current + limit, allOps.size());
			if (subTaskYieldIndex.compareAndSet(current, next)) {
				return allOps.subList(current, next);
			}
		}
	}

	@Override
	public final void markSubTaskCompleted() {
		pendingSubTasksCount.decrementAndGet();
	}

	@Override
	public final void undoMarkSubTaskCompleted() {
		pendingSubTasksCount.incrementAndGet();
	}

	@Override
	public final boolean allSubOperationsDone() {
		return pendingSubTasksCount.get() == 0;
	}

	@Override
	public final void resetSubOperations() {
		subTasks.clear();
		subTaskYieldIndex.set(0);
		contextData.clear();
		pendingSubTasksCount.set(-1);
		rangesSentAsRequests = false;
	}

	@Override
	public final long sizeThreshold() {
		return sizeThreshold;
	}

	@Override
	@SuppressWarnings("unchecked")
	public final CompositeDataOperationImpl<I> result() {
		buildItemPath(item, dstPath == null ? srcPath : dstPath);
		return new CompositeDataOperationImpl<>(this);
	}
}
