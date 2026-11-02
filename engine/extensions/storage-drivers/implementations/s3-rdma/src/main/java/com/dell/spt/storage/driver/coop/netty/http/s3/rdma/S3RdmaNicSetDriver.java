package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;
import static com.dell.spt.storage.driver.coop.netty.NettyStorageDriver.ATTR_KEY_OPERATION;
import static com.dell.spt.storage.driver.coop.netty.NettyStorageDriver.ATTR_KEY_RELEASED;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import static com.github.akurilov.netty.connection.pool.NonBlockingConnPool.ATTR_KEY_NODE;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.CompositeOperation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.load.lifecycle.OperationLifecycle;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker.DispatchToken;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.snapshot.DriverStatsSnapshot;
import com.dell.spt.base.nic.EndpointResolver;
import com.dell.spt.base.nic.NicPathCounting;
import com.dell.spt.base.nic.NicPathRows;
import com.dell.spt.base.nic.NicSetResources;
import com.dell.spt.base.nic.RdmaDeviceProbe;
import com.dell.spt.base.nic.SysfsInventoryReader;
import com.dell.spt.base.nic.pool.PoolDemandConfig;
import com.dell.spt.base.storage.driver.AdmissionPhase;
import com.dell.spt.base.storage.driver.PoolDemandDriver;
import com.dell.spt.storage.driver.coop.netty.nic.NicBinding;
import com.dell.spt.storage.driver.coop.netty.nic.NicPathCounters;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetConnectionPool;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetLayout;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetSettings;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetSupport;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.netty.connection.pool.NonBlockingConnPool;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.logging.log4j.Level;

/**
 * S3-RDMA driver with a NIC set ({@code storage.net.interfaces}). At start it plans its NICs as the
 * {@code s3} route does, with the RDMA plan, and opens one native context, one NIC Pool, one copier
 * and one Preparation Lane per NIC. Each attempt then takes its concurrency permit and a NIC with
 * room on the dispatch task, and keeps that NIC until it ends: every request of the attempt uses the
 * NIC's Paths, and its token names the NIC's DC target and the buffer's registration in the NIC's
 * pool. A retry chooses again.
 *
 * <p>An attempt that needs an RDMA buffer is handed to its NIC's lane, which prepares it and asks the
 * NIC's Path Pool for a connection. One that needs none, such as an HTTP payload, is sent by the
 * dispatch task. Neither thread waits for a connect.
 *
 * <p>Custody passes at the claim, on the channel's event loop just before the request is built
 * (D65): until then the operation stays queued in the tracker, so a stop or a stale circulation
 * writes nothing, the attempt gives back its buffer and its permit, and queue recovery owns the
 * operation. Direct dispatch stays off, as it is for this driver without a NIC set (D37).
 *
 * <p>This repeats the custody and the row accounting of {@code S3NicSetDriver}, whose route is
 * qualified on its own and stays untouched; only this route's lanes and contexts differ.
 *
 * <p>The contexts open in {@code doStart}, which runs in the barrier's preparation when a step holds
 * one. The driver's state lock serializes it with a stop, so no context opens while the driver closes.
 */
final class S3RdmaNicSetDriver<I extends Item, O extends Operation<I>> extends S3RdmaStorageDriver<I, O>
				implements NicPathCounting, PoolDemandDriver {

	/**
	 * How long close waits for every lane together, against one budget (item 12b). Filling a 1 GiB
	 * buffer, the longest call a lane cannot leave, took about 0.5 s on the SC26 client, so this allows
	 * ten of them, and it matches the 5.16 copier's wait and the I/O workers' budget.
	 */
	static final long LANE_STOP_BUDGET_MILLIS = 5_000;

	// The row of the attempt a connection carries, from its request binding to its completion.
	private static final AttributeKey<NicPathCounters.Row> ATTR_KEY_ROW = AttributeKey.valueOf("rdmaNicPathRow");

	/** What one opening made: replaced as a whole, so a later opening owns only its own. */
	record OpenRdma(List<RdmaNic> nics, List<RdmaPreparationLane> lanes) {}

	private final NicSetSupport nicSet;
	private final Function<RdmaConfig, RdmaTransport> transportFactory;
	private final int multipartObjects;
	private final int multipartParts;
	// Assigned by createConnectionPool() while the superclass constructor runs; no initializer, so
	// the assignment survives this class's own field initialization.
	private NicSetConnectionPool nicPool;
	// Assigned at start before the dispatch task and the NICs' event loops run.
	private NicPathCounters counters;
	private volatile OpenRdma rdma;
	private volatile boolean laneDied;
	/** Replaced only by tests of a lane past the budget. */
	long laneStopBudgetMillis = LANE_STOP_BUDGET_MILLIS;
	/** The JVM's direct-memory limit; replaced only by tests of a short pool. */
	long directMemoryLimitBytes = RdmaBufferPool.directMemoryLimitBytes();
	/** Replaced only by tests of a lane that cannot start. */
	Consumer<RdmaPreparationLane> laneStarter = RdmaPreparationLane::start;

	static <I extends Item, O extends Operation<I>> S3RdmaNicSetDriver<I, O> create(
					final String stepId,
					final DataInput itemDataInput,
					final Config storageConfig,
					final boolean verifyFlag,
					final int batchSize,
					final NicSetSettings settings)
					throws IllegalConfigurationException, InterruptedException {
		final var environment = new NicSetSupport.Environment(
						SysfsInventoryReader.system(new RdmaNativeDeviceProbe())::read, EndpointResolver.SYSTEM, NicBinding.DEVICE);
		return create(stepId, itemDataInput, storageConfig, verifyFlag, batchSize, settings, environment, RdmaTransport::new);
	}

	static <I extends Item, O extends Operation<I>> S3RdmaNicSetDriver<I, O> create(
					final String stepId,
					final DataInput itemDataInput,
					final Config storageConfig,
					final boolean verifyFlag,
					final int batchSize,
					final NicSetSettings settings,
					final NicSetSupport.Environment environment,
					final Function<RdmaConfig, RdmaTransport> transportFactory)
					throws IllegalConfigurationException, InterruptedException {
		return new S3RdmaNicSetDriver<>(
						stepId, itemDataInput, storageConfig, verifyFlag, batchSize, settings, environment, transportFactory);
	}

	private S3RdmaNicSetDriver(
					final String stepId,
					final DataInput itemDataInput,
					final Config storageConfig,
					final boolean verifyFlag,
					final int batchSize,
					final NicSetSettings settings,
					final NicSetSupport.Environment environment,
					final Function<RdmaConfig, RdmaTransport> transportFactory)
					throws IllegalConfigurationException, InterruptedException {
		super(stepId, itemDataInput, storageConfig, verifyFlag, batchSize, null, true);
		this.nicSet = new NicSetSupport(toString(), settings, environment);
		final var shortfall = rdmaConfig().getPoolShortfall();
		if (!PoolDemandConfig.SHORTFALL_FAIL.equals(shortfall) && !PoolDemandConfig.SHORTFALL_WARN.equals(shortfall)) {
			throw new IllegalConfigurationException(PoolDemandConfig.SHORTFALL_SETTING + " must be " + PoolDemandConfig.SHORTFALL_FAIL
							+ " or " + PoolDemandConfig.SHORTFALL_WARN + ", not \"" + shortfall + "\"");
		}
		this.multipartObjects = limitOf(storageConfig, "driver-limit-multipart-objects");
		this.multipartParts = limitOf(storageConfig, "driver-limit-multipart-parts");
		this.transportFactory = Objects.requireNonNull(transportFactory, "RDMA transport factory");
	}

	private static int limitOf(final Config storageConfig, final String path) {
		try {
			return Math.max(0, storageConfig.intVal(path));
		} catch (final RuntimeException absent) {
			return 0;
		}
	}

	/** Empty until start opens it; the bootstrap snapshot gives every Path the release's socket options. */
	@Override
	protected NonBlockingConnPool createConnectionPool() {
		nicPool = new NicSetConnectionPool(bootstrap.clone(), this);
		return nicPool;
	}

	@Override
	protected void doStart() throws IllegalStateException {
		final boolean rdmaEnabled = rdmaConfig().isEnabled();
		try {
			nicSet.open(nicPool, List.of(storageNodeAddrs), concurrencyLimit, this::createIoGroup, this::shutdownExtraIoGroups, rdmaEnabled);
			counters = nicSet.counters();
			if (rdmaEnabled) {
				try {
					rdma = openRdma();
				} catch (final RuntimeException | InterruptedException | Error e) {
					nicPool.close();
					shutdownExtraIoGroups();
					throw e;
				}
			} else {
				Loggers.MSG.info("{}: RDMA disabled by configuration, using HTTP on the NICs' Paths", stepId);
			}
			super.doStart();
			nicSet.started(nicPool, concurrencyLimit);
		} catch (final InterruptedException e) {
			throwUnchecked(e);
		}
	}

	/**
	 * Opens each NIC's context strictly, one after another; a failure closes those already opened and
	 * fails the step, whatever {@code storage.rdma.fallback} says. Then each NIC gets its pool, copier
	 * and lane. Package-private for a test of a restart while an old lane still runs: each opening
	 * owns only what it made.
	 */
	OpenRdma openRdma() throws InterruptedException {
		AdmissionPhase.report("opening RDMA contexts");
		final var layout = nicSet.layout().nics();
		final var nics = new ArrayList<RdmaNic>(layout.size());
		final var limits = new ArrayList<RdmaNicPoolLimits>(layout.size());
		final long driverBudget = directMemoryLimitBytes / RdmaBufferPool.DIRECT_MEMORY_SHARE_DIVISOR;
		try {
			for (var i = 0; i < layout.size(); i++) {
				if (Thread.currentThread().isInterrupted()) {
					throw new InterruptedException("Interrupted while opening the RDMA contexts");
				}
				limits.add(RdmaNicPoolLimits.of(concurrencyLimit, layout.size(), i, driverBudget, UNLIMITED_CONCURRENCY_POOL_LIMIT));
				nics.add(openNic(i, layout.get(i), limits.get(i)));
			}
		} catch (final RuntimeException | InterruptedException | Error e) {
			nics.forEach(RdmaNic::close);
			throw e;
		}
		final var admission = nicPool.admission();
		final var lanes = new ArrayList<RdmaPreparationLane>(nics.size());
		final var startedLanes = new ArrayList<RdmaPreparationLane>(nics.size());
		try {
			for (var i = 0; i < nics.size(); i++) {
				final var nic = nics.get(i);
				lanes.add(new RdmaPreparationLane(stepId, nic, admission.limit(i), cause -> laneDied(nic, cause)));
			}
			for (final var lane : lanes) {
				laneStarter.accept(lane);
				startedLanes.add(lane);
			}
		} catch (final RuntimeException | Error e) {
			abandonOpening(nics, startedLanes);
			throw e;
		}
		for (var i = 0; i < nics.size(); i++) {
			final var nic = nics.get(i);
			final var limit = limits.get(i);
			Loggers.MSG.info("{}: NIC {}: RDMA context on {}; pool budget {} bytes; per size class, minimum {} and headroom {} "
							+ "of cap {} buffers; {} copy thread(s); lane queue {}",
							stepId, nic.name, nicSet.rdmaDevice(nic.name), limit.budgetBytes(), limit.minimum(), limit.headroom(), limit.cap(),
							rdmaConfig().getCopyThreads(), admission.limit(i));
		}
		return new OpenRdma(List.copyOf(nics), List.copyOf(lanes));
	}

	/** Undoes an opening whose lanes did not all start. Lanes start in NIC order, so the started ones are the first NICs'. */
	private void abandonOpening(final List<RdmaNic> nics, final List<RdmaPreparationLane> started) {
		started.forEach(RdmaPreparationLane::cancel);
		final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(laneStopBudgetMillis);
		started.forEach(lane -> lane.awaitExit(deadline));
		for (var i = 0; i < nics.size(); i++) {
			if (i < started.size() && !started.get(i).exited()) {
				started.get(i).closeWhenDone();
			} else {
				nics.get(i).close();
			}
		}
	}

	private RdmaNic openNic(final int index, final NicSetLayout.Nic nic, final RdmaNicPoolLimits limits) {
		final var device = nicSet.rdmaDevice(nic.name());
		final var address = nic.address().getHostAddress();
		final var transport = Objects.requireNonNull(transportFactory.apply(rdmaConfig()), "RDMA transport");
		try {
			transport.initStrict(device, address);
		} catch (final RuntimeException e) {
			transport.close();
			throw new RunTerminalException(IntegrityTerminalException.Category.EXECUTION,
							"storage.net.interfaces: cannot open the RDMA context of NIC " + nic.name() + " (device " + device + ", address "
											+ address + "): " + e.getMessage(),
							e);
		}
		final var pool = rdmaConfig().isBufferPoolEnabled()
						? new RdmaBufferPool(transport, limits.cap(), limits.budgetBytes())
						: null;
		final var copier = RdmaPayloadCopier.stoppable(rdmaConfig().getCopyThreads(), stepId + "-" + nic.name() + "-rdma-copy-");
		final var opened = new RdmaNic(index, nic.name(), transport, pool, copier, new RdmaPathStats());
		if (pool != null) {
			pool.registrationListener = buffer -> logPoolBuffer(opened, buffer);
		}
		return opened;
	}

	@Override
	public Layout poolLayout() {
		final var open = rdma;
		if (open == null || !rdmaConfig().isBufferPoolEnabled()) {
			return null;
		}
		return new Layout(hostName(), directMemoryLimitBytes, concurrencyLimit, rdmaConfig().getThresholdBytes(), multipartObjects,
						multipartParts, PoolDemandConfig.failsOnShortfall(rdmaConfig().getPoolShortfall()),
						open.nics().stream().map(nic -> nic.name).toList());
	}

	private static String hostName() {
		final var name = SysfsInventoryReader.system(RdmaDeviceProbe.NONE).hostname();
		return name == null ? "unknown" : name;
	}

	@Override
	public List<CompletableFuture<PrewarmResult>> startPrewarm(final List<PrewarmOrder> orders) {
		final var open = rdma;
		final var running = new ArrayList<CompletableFuture<PrewarmResult>>(orders.size());
		for (final var order : orders) {
			final var future = new CompletableFuture<PrewarmResult>();
			running.add(future);
			if (open == null) {
				future.complete(new PrewarmResult(order.nic(), 0, 0, null, true));
				continue;
			}
			for (var i = 0; i < open.nics().size(); i++) {
				if (open.nics().get(i).name.equals(order.nic())) {
					final var lane = open.lanes().get(i);
					if (!lane.offer(new Prewarm(order, open.nics().get(i), lane, future))) {
						future.complete(new PrewarmResult(order.nic(), 0, 0, null, true));
					}
				}
			}
		}
		return running;
	}

	/**
	 * One NIC's Pool Pre-warm, on its lane, so the lane first touches its buffers as it does on a miss.
	 * It holds every buffer until the last, or a later acquire would reuse the first instead of making
	 * one. A cancel stops it between buffers, and whatever it holds goes back to the pool, which close
	 * then drains in ticket 11's order.
	 */
	private static final class Prewarm implements RdmaPreparationLane.Task {

		private final PrewarmOrder order;
		private final RdmaNic nic;
		private final RdmaPreparationLane lane;
		private final CompletableFuture<PrewarmResult> future;

		Prewarm(final PrewarmOrder order, final RdmaNic nic, final RdmaPreparationLane lane, final CompletableFuture<PrewarmResult> future) {
			this.order = order;
			this.nic = nic;
			this.lane = lane;
			this.future = future;
		}

		@Override
		public void prepare() {
			final long start = System.nanoTime();
			final var held = new ArrayList<RdmaBufferPool.PooledBuffer>(order.buffers());
			String failure = null;
			boolean stopped = false;
			try {
				while (held.size() < order.buffers()) {
					if (lane.cancelled()) {
						stopped = true;
						break;
					}
					final long allocationFailures = nic.pool.allocationFailed.sum();
					final long registrationFailures = nic.pool.registrationFailed.sum();
					final var buffer = nic.pool.acquire(order.sizeClass());
					if (buffer == null) {
						failure = nic.pool.allocationFailed.sum() > allocationFailures
										? "direct memory exhausted"
										: nic.pool.registrationFailed.sum() > registrationFailures
														? "buffer registration failed"
														: "its pool refused another buffer";
						break;
					}
					held.add(buffer);
				}
			} finally {
				held.forEach(nic.pool::release);
				nic.prewarmedBuffers = held.size();
				final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
				future.complete(new PrewarmResult(order.nic(), held.size(), millis, failure, stopped));
			}
		}

		@Override
		public void interrupted() {
			future.complete(new PrewarmResult(order.nic(), 0, 0, null, true));
		}
	}

	/**
	 * The NUMA diagnostic (S3, T2): each pool buffer's address range, which the per-page query reads
	 * from the host. The address comes from a token, so it is the one the server is given. Off unless
	 * DEBUG is on, and it runs once per buffer, as the buffer is made.
	 */
	private void logPoolBuffer(final RdmaNic nic, final RdmaBufferPool.PooledBuffer buffer) {
		if (Loggers.MSG.isDebugEnabled()) {
			final var token = nic.transport.generateToken(buffer.mrHandle(), buffer.capacity());
			if (token != null) {
				Loggers.MSG.debug("{}: NIC {} pool buffer address=0x{} capacity={}", stepId, nic.name, token.split(":", -1)[0],
								buffer.capacity());
			}
		}
	}

	private void laneDied(final RdmaNic nic, final Throwable cause) {
		laneDied = true;
		recordTerminalFailure(new IntegrityTerminalException(IntegrityTerminalException.Category.EXECUTION,
						"The RDMA preparation lane of NIC " + nic.name + " died: " + cause, cause));
	}

	/** No connect or preparation starts during the drain, and waiting attempts give back their NIC and permit. */
	@Override
	protected void onAdmissionClosed() {
		stopAdmittedWork();
	}

	@Override
	protected void beforeIoShutdown() {
		stopAdmittedWork();
	}

	private void stopAdmittedWork() {
		// Lanes first: closing admission fails waiting attempts on this thread, and without a pool each one
		// deregisters its buffer, a native call that cannot be interrupted.
		final var open = rdma;
		if (open != null) {
			open.lanes().forEach(RdmaPreparationLane::cancel);
		}
		nicPool.closeAdmission();
	}

	/**
	 * Item 12b: admission is closed; every lane is cancelled at once and waited for against one budget;
	 * then per NIC, the pool and the context close. A lane past the budget is in a call that cannot be
	 * interrupted: it keeps its NIC open and closes it when the call returns. If an I/O worker or the
	 * reaper outlasted the stop, it can still be in a native call or building a request, so every NIC
	 * and every unfinished attempt's buffer stay as they are until the process ends: a leak in a rare
	 * case, rather than a closed context or a deregistered buffer in use.
	 */
	@Override
	void closeRdmaResources() {
		final var open = rdma;
		if (open == null) {
			final var stats = super.pathStats();
			Loggers.MSG.info("{}: RDMA data path summary (transport unavailable): {}, bufferPool=disabled", stepId, stats.summary());
			return;
		}
		stopRdma(open);
	}

	/** Package-private for a test of a restart while an old lane still runs. */
	void stopRdma(final OpenRdma open) {
		final boolean available = open.nics().stream().allMatch(nic -> nic.transport.isAvailable());
		open.lanes().forEach(RdmaPreparationLane::cancel);
		final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(laneStopBudgetMillis);
		open.lanes().forEach(lane -> lane.awaitExit(deadline));
		final boolean workersLate = reaperLate || Boolean.FALSE.equals(ioStoppedInTime());
		if (!workersLate) {
			releaseLeakedContexts();
		}
		for (var i = 0; i < open.nics().size(); i++) {
			final var nic = open.nics().get(i);
			final var lane = open.lanes().get(i);
			Loggers.MSG.info("{}: nic={}: RDMA data path summary (transport {}): {}{}", stepId, nic.name,
							nic.transport.isAvailable() ? "available" : "unavailable", nic.stats.summary(), nic.poolSummary());
			if (workersLate) {
				if (lane.exited()) {
					lane.drain();
				}
				Loggers.MSG.warn("{}: NIC {}: an I/O worker or the RDMA reaper did not stop in time and may still use this NIC's RDMA "
								+ "resources; its pool, its context and its unfinished attempts' buffers stay open until the process ends",
								stepId, nic.name);
			} else if (lane.exited()) {
				lane.drain();
				nic.close();
			} else {
				Loggers.MSG.warn("{}: NIC {}: the RDMA preparation lane is still in {} after the {} ms stop budget; it closes this "
								+ "NIC's pool and context when the call returns", stepId, nic.name, lane.blockedCall(), laneStopBudgetMillis);
				lane.closeWhenDone();
			}
		}
		final var pools = pools(open);
		Loggers.MSG.info("{}: RDMA data path summary (transport {}): {}{}", stepId, available ? "available" : "unavailable",
						pathStats().summary(), rdmaConfig().isBufferPoolEnabled() ? ", " + RdmaBufferPool.summary(pools) : ", bufferPool=disabled");
	}

	private static List<RdmaBufferPool> pools(final OpenRdma open) {
		final var pools = new ArrayList<RdmaBufferPool>(open.nics().size());
		for (final var nic : open.nics()) {
			if (nic.pool != null) {
				pools.add(nic.pool);
			}
		}
		return pools;
	}

	/** The counts summed over the NICs; without RDMA, the driver's own. */
	@Override
	RdmaPathStats pathStats() {
		final var open = rdma;
		return open == null
						? super.pathStats()
						: RdmaPathStats.sum(open.nics().stream().map(nic -> nic.stats).toList());
	}

	/** Totals over the NICs, read without a native call, so it is safe while the driver closes. */
	@Override
	public DriverStatsSnapshot driverStats() {
		final var open = rdma;
		if (open == null) {
			return RdmaDriverStats.snapshot(super.pathStats(), List.of(), false);
		}
		return RdmaDriverStats.snapshot(pathStats(), pools(open),
						open.nics().stream().allMatch(nic -> nic.transport.isAvailable()));
	}

	@Override
	public NicPathRows nicPathRows() {
		// The stop's flag first: it was written after start assigned the counters.
		final var ioStopped = ioStoppedInTime();
		final var table = counters;
		return table == null || ioStopped == null ? null : table.snapshot(ioStopped && dispatchStoppedInTime(), this::poolCounts);
	}

	/** NIC {@code index}'s pool counts, from this driver's own pools: all zero without a pool, as nothing was pooled. */
	private NicPathRows.PoolCounts poolCounts(final int index) {
		final var open = rdma;
		final var nic = open == null ? null : open.nics().get(index);
		if (nic == null || nic.pool == null) {
			return new NicPathRows.PoolCounts(0, 0, 0, 0, 0);
		}
		return new NicPathRows.PoolCounts(nic.pool.created.sum(), nic.pool.hits.sum(), nic.pool.exhausted.sum(), nic.pool.unpooled.sum(),
						nic.prewarmedBuffers);
	}

	@Override
	public NicSetResources nicSetResources() {
		return nicSet.resources(ioEventLoopCount());
	}

	/** Runs as the request is sent: the attempt is admitted on its Path. */
	@Override
	protected void bindRequestChannel(final Channel channel, final O op) {
		final var row = counters.path(channel);
		if (row != null) {
			row.attempted();
			channel.attr(ATTR_KEY_ROW).set(row);
		}
		super.bindRequestChannel(channel, op);
	}

	/**
	 * Ends the attempt the channel carries on its row, once its reply is classified, as that settles
	 * its status and a GET's bytes; a repeated completion finds no row.
	 */
	@Override
	public void complete(final Channel channel, final O op) {
		final var row = channel == null ? null : channel.attr(ATTR_KEY_ROW).getAndSet(null);
		if (row == null) {
			super.complete(channel, op);
			return;
		}
		// Read before completion, which may hand the operation on.
		final var lifecycle = op.lifecycle();
		final var sent = sentContext(channel);
		if (!classifyRdmaReply(channel, op)) {
			// The reaper claimed this request and ends it, finding the row taken.
			row.failed();
			return;
		}
		final var succeeded = op.status() == Operation.Status.SUCC;
		final long bytes = succeeded ? payloadBytes(op) : 0;
		final boolean carries = carriesPayload(op);
		final boolean overRdma = succeeded && carries && sent != null && transferred(sent);
		completeTransport(channel, op);
		if (terminal(lifecycle)) {
			if (succeeded) {
				row.succeeded(bytes);
				if (overRdma) {
					row.payloadOverRdma();
				} else if (carries) {
					row.payloadOverHttp();
				}
			} else {
				row.failed();
			}
		}
	}

	/** Whether the server moved this attempt's payload over RDMA: not declined, whatever came back over HTTP. */
	private boolean transferred(final RdmaContext sent) {
		return RdmaReplyContract.classify(sent.opType, sent.reply, sent.bytesTransferred, sent.bodyBytes(), sent.size,
						rdmaConfig().isAllowMissingBytesHeader()).outcome() == RdmaReplyContract.Outcome.TRANSFERRED;
	}

	@Override
	void completeTimedOut(final Channel channel, final O op) {
		final var row = channel == null ? null : channel.attr(ATTR_KEY_ROW).getAndSet(null);
		final var lifecycle = op.lifecycle();
		completeTransport(channel, op);
		if (row != null && terminal(lifecycle)) {
			row.failed();
		}
	}

	/** The request body of a write or the response body of a read; a multipart parent's requests carry none. */
	private static long payloadBytes(final Operation<?> op) {
		return op instanceof DataOperation<?> data && !(op instanceof CompositeOperation) ? data.countBytesDone() : 0;
	}

	/** Whether the tracker committed the circulation; an untracked operation has no drain cut. */
	private static boolean terminal(final OperationLifecycle lifecycle) {
		return !lifecycle.isTracked() || lifecycle.state() == OperationLifecycleState.TERMINAL;
	}

	/** The write, not the accepted submit, starts transport, except on the inherited route. */
	@Override
	protected boolean successfulSubmitStartsTransport(final O op) {
		return usesNoChannel(op);
	}

	/** A free permit and a NIC with room, so the dispatch task parks while every NIC is full, or a lane has died. */
	@Override
	protected boolean hasAvailableDispatchCapacity() {
		return super.hasAvailableDispatchCapacity() && nicPool.admission().hasRoom() && !laneDied;
	}

	@Override
	protected int dispatchAttemptLimit(final int bufferedOperationCount) {
		return Math.min(super.dispatchAttemptLimit(bufferedOperationCount), nicPool.admission().room());
	}

	/** Pool first, so the NIC's room comes with an idle connection, then the NIC and the permit. */
	@Override
	protected void releaseTransport(final Channel channel) {
		final int nic = NicSetConnectionPool.nicIndex(channel);
		nicPool.release(channel);
		if (nic >= 0) {
			nicPool.admission().attemptEnded(nic);
		}
		concurrencyThrottle.release();
		signalDispatchCapacityAvailable();
	}

	@Override
	protected boolean submit(final O op) throws IllegalStateException {
		if (usesNoChannel(op)) {
			return submitInherited(op);
		}
		if (!isStarted()) {
			throw new IllegalStateException();
		}
		// No admission lock here: the operation stays queued until the claim, which checks the
		// token under that lock, so a capture racing a stop is left to queue recovery.
		if (!isAdmissionOpen()) {
			return false;
		}
		final var token = operationLifecycle().queuedDispatchToken(op);
		if (token == null && !continuesDispatchedComposite(op)) {
			return false;
		}
		if (!concurrencyThrottle.tryAcquire()) {
			return false;
		}
		final var admission = nicPool.admission();
		final int nicIndex = admission.take();
		if (nicIndex < 0) {
			concurrencyThrottle.release();
			return false;
		}
		final var open = rdma;
		final var attempt = new Attempt(op, token, nicIndex, open);
		final var selection = selectPath(op, open != null);
		if (selection != PathSelection.RDMA) {
			return attempt.sendHttp(op, httpCounter(selection, statsOf(open, nicIndex)));
		}
		if (exceedsOneBuffer(op)) {
			return attempt.sendHttp(op, statsOf(open, nicIndex).httpOversize);
		}
		if (open.lanes().get(nicIndex).offer(attempt)) {
			return true;
		}
		// The lane has stopped: the operation stays with the dispatch task, for queue recovery.
		admission.cancelled(nicIndex);
		concurrencyThrottle.release();
		return false;
	}

	private RdmaPathStats statsOf(final OpenRdma open, final int nicIndex) {
		return open == null ? super.pathStats() : open.nics().get(nicIndex).stats;
	}

	/** Whether the payload needs more than one RDMA buffer, so it goes over HTTP without the lane. */
	private boolean exceedsOneBuffer(final O op) {
		try {
			final long size = ((DataOperation<?>) op).item().size();
			if (size > Integer.MAX_VALUE) {
				warnOversize(size);
				return true;
			}
		} catch (final IOException e) {
			// The lane's preparation reads the size again and fails the attempt.
		}
		return false;
	}

	/** A composite's finalization, which continues its dispatched attempt and has no queued circulation. */
	private boolean continuesDispatchedComposite(final O op) {
		return op instanceof CompositeOperation
						&& operationLifecycle().stateOf(op) == OperationLifecycleState.DISPATCHED;
	}

	@Override
	protected int submit(final List<O> ops, final int from, final int to) throws IllegalStateException {
		var i = from;
		while (i < to && submit(ops.get(i))) {
			i++;
		}
		return i - from;
	}

	/** NOOP and composite READ passes complete without a channel through the inherited route. */
	private static boolean usesNoChannel(final Operation<?> op) {
		return OpType.NOOP.equals(op.type())
						|| (op instanceof CompositeDataOperation && OpType.READ.equals(op.type()));
	}

	private void completeQuietly(final Channel channel, final O op) {
		try {
			complete(channel, op);
		} catch (final Throwable cause) {
			throwUncheckedIfInterrupted(cause);
			LogUtil.exception(Level.DEBUG, cause, "Load operation result publication failure");
		}
	}

	NicSetConnectionPool connectionPool() {
		return nicPool;
	}

	OpenRdma openRdmaResources() {
		return rdma;
	}

	/**
	 * Replaces the RDMA resources after {@link #stopRdma}. A Netty driver cannot restart after a stop,
	 * whose event-loop groups end for good, so this is the RDMA half of a restart, for its test: each
	 * opening owns only what it made, and an old lane's late callbacks change nothing.
	 */
	void reopenRdma() throws InterruptedException {
		rdma = openRdma();
	}

	/**
	 * One admitted attempt, from its NIC's permit to its request. An attempt the lane prepared owns its
	 * context until the claim; before then, nothing outside it can reach the buffer. It holds a permit
	 * and its NIC's place until the pool reports its connection or its failure, and the pool reports
	 * once; an attempt that never asked the pool gives both back itself.
	 */
	private final class Attempt implements RdmaPreparationLane.Task, NicSetConnectionPool.Acquisition, AttemptRoute<O> {

		private final O op;
		private final DispatchToken<O> token;
		private final int nicIndex;
		/** {@code null} when RDMA is disabled. */
		private final RdmaNic nic;
		private final RdmaPreparationLane lane;
		private RdmaContext context;
		private LongAdder sentCounter;
		// Lane-confined until the attempt asks the pool, which then owns its ending.
		private boolean askedPool;

		Attempt(final O op, final DispatchToken<O> token, final int nicIndex, final OpenRdma open) {
			this.op = op;
			this.token = token;
			this.nicIndex = nicIndex;
			this.nic = open == null ? null : open.nics().get(nicIndex);
			this.lane = open == null ? null : open.lanes().get(nicIndex);
		}

		@Override
		public void prepare() {
			boolean consumed = false;
			try {
				consumed = prepareAndSend(op, nic, this);
			} finally {
				if (!consumed && !askedPool) {
					interrupted();
				}
			}
		}

		/** Ended before asking the pool, by a stop: its operation stays queued for recovery. */
		@Override
		public void interrupted() {
			final var row = counters.path(nicIndex, -1);
			row.attempted();
			row.interrupted();
			nicPool.admission().cancelled(nicIndex);
			concurrencyThrottle.release();
			signalDispatchCapacityAvailable();
		}

		@Override
		public boolean sendRdma(final O ignored, final RdmaContext prepared) {
			prepared.nic = nic;
			if (lane.cancelled()) {
				releaseContext(prepared, true);
				return false;
			}
			context = prepared;
			askPool();
			return true;
		}

		@Override
		public boolean sendHttp(final O ignored, final LongAdder counter) {
			sentCounter = counter;
			askPool();
			return true;
		}

		private void askPool() {
			askedPool = true;
			try {
				// The pool reports exactly once, even when the event loops terminate first (close settles it).
				nicPool.acquire(nicIndex, this);
			} catch (final RuntimeException e) {
				failed(-1, e);
			}
		}

		private boolean claim() {
			return (lane == null || !lane.cancelled()) && claimCapturedDispatch(op, token);
		}

		@Override
		public boolean fail(final O ignored, final boolean settlePart) {
			final var row = counters.path(nicIndex, -1);
			row.attempted();
			final boolean claimed = withDispatchAdmission(this::claim);
			nicPool.admission().cancelled(nicIndex);
			concurrencyThrottle.release();
			signalDispatchCapacityAvailable();
			if (!claimed) {
				row.interrupted();
				return true;
			}
			final var lifecycle = op.lifecycle();
			op.status(Operation.Status.FAIL_IO);
			if (settlePart) {
				markPartSettled(op);
			}
			try {
				handleCompleted(op);
			} catch (final Throwable cause) {
				throwUncheckedIfInterrupted(cause);
				LogUtil.exception(Level.DEBUG, cause, "Load operation result publication failure");
			}
			if (terminal(lifecycle)) {
				row.failed();
			} else {
				row.interrupted();
			}
			return true;
		}

		@Override
		public void complete(final Channel channel, final Throwable failure) {
			if (failure == null) {
				onAcquired(channel);
			} else {
				failed(-1, failure);
			}
		}

		/**
		 * Mirrors a failed default-pool lease: the operation fails with FAIL_IO, if it can still be
		 * marked dispatched. After a stop, or for a stale circulation, queue recovery owns it instead.
		 * Nothing was sent, so its buffer goes back unless its content was invalidated. The attempt
		 * counts on the Path chosen for it, or on its NIC when none was ({@code path} -1).
		 */
		@Override
		public void failed(final int path, final Throwable cause) {
			final var row = counters.path(nicIndex, path);
			row.attempted();
			final boolean claimed = withDispatchAdmission(this::claim);
			nicSet.logSetupFailure(cause, !claimed);
			if (context != null) {
				releaseContext(context, true);
			}
			final var admission = nicPool.admission();
			admission.waitEnded(nicIndex);
			admission.attemptEnded(nicIndex);
			concurrencyThrottle.release();
			signalDispatchCapacityAvailable();
			if (!claimed) {
				row.interrupted();
				return;
			}
			final var lifecycle = op.lifecycle();
			op.status(Operation.Status.FAIL_IO);
			// finishResponse() throws before settling a part whose response never started.
			if (op.respTimeStart() == 0) {
				markPartSettled(op);
			}
			completeQuietly(null, op);
			if (terminal(lifecycle)) {
				row.failed();
			} else {
				row.interrupted();
			}
		}

		/** Runs on the channel's event loop. */
		private void onAcquired(final Channel channel) {
			final var admission = nicPool.admission();
			if (admission.waitEnded(nicIndex)) {
				signalDispatchCapacityAvailable();
			}
			if (!withDispatchAdmission(this::claim)) {
				// Stopped, or the circulation is stale: nothing is written and queue recovery owns the operation.
				final var row = counters.path(channel);
				row.attempted();
				row.interrupted();
				if (context != null) {
					releaseContext(context, true);
				}
				nicPool.release(channel);
				admission.attemptEnded(nicIndex);
				concurrencyThrottle.release();
				signalDispatchCapacityAvailable();
				return;
			}
			if (context != null) {
				nic.stats.rdmaRequestsSent.increment();
			} else if (sentCounter != null) {
				sentCounter.increment();
			}
			try {
				if (context != null) {
					storeContext(op, context);
				}
				channel.attr(ATTR_KEY_RELEASED).set(Boolean.FALSE);
				channel.attr(ATTR_KEY_OPERATION).set(op);
				op.nodeAddr(channel.attr(ATTR_KEY_NODE).get());
				op.startRequest();
				sendRequest(channel, op);
			} catch (final Throwable thrown) {
				throwUncheckedIfInterrupted(thrown);
				nicSet.logSetupFailure(thrown, false);
				if (channel.attr(ATTR_KEY_ROW).get() == null) {
					// Failed before its request was bound: admit the attempt here, so completion ends it.
					final var row = counters.path(channel);
					row.attempted();
					channel.attr(ATTR_KEY_ROW).set(row);
				}
				if (context != null) {
					dropUnboundContext(op, context, true);
				}
				op.status(Operation.Status.FAIL_UNKNOWN);
				completeQuietly(channel, op);
			}
		}
	}
}
