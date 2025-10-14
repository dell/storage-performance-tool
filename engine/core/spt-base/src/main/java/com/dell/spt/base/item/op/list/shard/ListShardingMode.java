package com.dell.spt.base.item.op.list.shard;

/** Supported strategies for distributing LIST workloads across local workers. */
public enum ListShardingMode {
	/**
	 * Automatic selection based on runtime concurrency:
	 * - concurrency <= 1: behaves like {@link #NONE}
	 * - concurrency  > 1: behaves like {@link #STATIC}
	 */
	AUTO,
	/** Single LIST stream over the namespace. */
	NONE,
	/** Seed multiple prefixes so local workers enumerate disjoint slices. */
	STATIC
}
