package com.dell.spt.base.storage.driver;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A storage driver whose NIC Pools the step's preparation checks and warms: {@code s3-rdma} with a
 * NIC set. The step's preparation calls it after the driver started and before admission opens, so
 * nothing here runs per operation.
 */
public interface PoolDemandDriver {

	/**
	 * The resolved plan the demand is computed from.
	 *
	 * @param host this engine's host name, for messages
	 * @param limitBytes the effective {@code MaxDirectMemorySize}, or the heap limit when it is unset
	 * @param concurrency {@code P}; 0 or less is unlimited
	 * @param failOnShortfall whether {@code storage.rdma.poolShortfall} is {@code fail}
	 * @param nics the NICs in layout order
	 */
	record Layout(
					String host, long limitBytes, int concurrency, long thresholdBytes, int multipartObjects, int multipartParts,
					boolean failOnShortfall, List<String> nics) {

		public Layout {
			nics = List.copyOf(nics);
		}
	}

	/** Registers {@code buffers} buffers of {@code sizeClass} bytes in {@code nic}'s pool. */
	record PrewarmOrder(String nic, int buffers, int sizeClass) {}

	/**
	 * What one NIC's lane registered.
	 *
	 * @param failure what stopped it short, or {@code null}: a registration or allocation failure, which is a short pool
	 * @param stopped whether a stop or a preparation timeout ended it
	 */
	record PrewarmResult(String nic, int buffers, long millis, String failure, boolean stopped) {}

	/** The driver's plan, or {@code null} when it has no NIC Pools, as with {@code storage.rdma.enabled=false}. */
	Layout poolLayout();

	/**
	 * Starts each order on its NIC's lane, all at once, and returns at once. Each future completes,
	 * never exceptionally, when its lane is done or stopped.
	 */
	List<CompletableFuture<PrewarmResult>> startPrewarm(List<PrewarmOrder> orders);
}
