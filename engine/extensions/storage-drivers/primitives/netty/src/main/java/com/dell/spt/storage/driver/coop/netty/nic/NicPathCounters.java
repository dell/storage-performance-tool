package com.dell.spt.storage.driver.coop.netty.nic;

import com.dell.spt.base.nic.NicInventory;
import com.dell.spt.base.nic.NicPlan;
import com.dell.spt.base.nic.NicPathRows;
import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntFunction;

/**
 * A NIC-set driver's per-attempt counters (item 18, D59): one row per Path, one per NIC for
 * attempts that ended before their Path was chosen, and one for attempts that ended before a NIC
 * was. Each attempt adds its admission and then its outcome to one row; unresolved is what has no
 * outcome at the snapshot. The table is fixed at open, so the per-attempt route only indexes and
 * adds. A snapshot is exact only once every update has stopped.
 */
public final class NicPathCounters {

	/** One row's counts; {@link LongAdder}, as one Path's channels run on several of its NIC's event loops. */
	public static final class Row {

		private final NicPathRows.Kind kind;
		private final int nicIndex;
		private final NicPathRows.Nic nic;
		private final NicPathRows.Endpoint endpoint;
		private final LongAdder attempts = new LongAdder();
		private final LongAdder succeeded = new LongAdder();
		private final LongAdder failed = new LongAdder();
		private final LongAdder interrupted = new LongAdder();
		private final LongAdder bytes = new LongAdder();
		private final LongAdder rdmaSucceeded = new LongAdder();
		private final LongAdder httpSucceeded = new LongAdder();

		private Row(final NicPathRows.Kind kind, final int nicIndex, final NicPathRows.Nic nic, final NicPathRows.Endpoint endpoint) {
			this.kind = kind;
			this.nicIndex = nicIndex;
			this.nic = nic;
			this.endpoint = endpoint;
		}

		/** The index of the row's NIC in layout order, or -1 for the row of attempts that ended before a NIC was taken. */
		public int nicIndex() {
			return nicIndex;
		}

		/** An attempt was admitted on this row. */
		public void attempted() {
			attempts.increment();
		}

		/** An admitted attempt succeeded with {@code payloadBytes}. */
		public void succeeded(final long payloadBytes) {
			succeeded.increment();
			if (payloadBytes > 0) {
				bytes.add(payloadBytes);
			}
		}

		/** A succeeded attempt's payload moved over RDMA. */
		public void payloadOverRdma() {
			rdmaSucceeded.increment();
		}

		/** A succeeded attempt's payload went over HTTP. */
		public void payloadOverHttp() {
			httpSucceeded.increment();
		}

		/** An admitted attempt failed: a response failure, or a setup failure that ended its operation. */
		public void failed() {
			failed.increment();
		}

		/** An admitted attempt ended before any request was sent; the tracker settles its operation. */
		public void interrupted() {
			interrupted.increment();
		}

		private NicPathRows.Row snapshot() {
			return new NicPathRows.Row(kind, nic, endpoint, attempts.sum(), succeeded.sum(), failed.sum(),
							interrupted.sum(), bytes.sum());
		}

		private NicPathRows.Row snapshot(final NicPathRows.PoolCounts pool) {
			return new NicPathRows.Row(kind, nic, endpoint, attempts.sum(), succeeded.sum(), failed.sum(),
							interrupted.sum(), bytes.sum(), rdmaSucceeded.sum(), httpSucceeded.sum(), pool);
		}
	}

	/** The one RDMA port a NIC set uses (item 19). */
	private static final Integer RDMA_PORT = 1;

	private final Row[][] paths;
	private final Row[] nics;
	private final Row none = new Row(NicPathRows.Kind.NONE, -1, null, null);

	private NicPathCounters(final Row[][] paths, final Row[] nics) {
		this.paths = paths;
		this.nics = nics;
	}

	/**
	 * The table for {@code layout}, in its NIC and Path order, with each NIC's plan columns from
	 * {@code plan} and {@code inventory}.
	 */
	static NicPathCounters of(final NicSetLayout layout, final NicPlan plan, final NicInventory inventory,
					final int concurrencyLimit) {
		final int count = layout.nics().size();
		final var shares = NicSetSettings.hasShares(concurrencyLimit, count);
		final var paths = new Row[count][];
		final var nics = new Row[count];
		for (var i = 0; i < count; i++) {
			final var nic = layout.nics().get(i);
			final var planned = plan.nics().stream().filter(n -> n.name().equals(nic.name())).findFirst().orElse(null);
			final var port = plan.ports().stream().filter(p -> p.nics().contains(nic.name())).findFirst().orElse(null);
			final var device = plan.rdma() ? inventory.nic(nic.name()).map(NicInventory.Nic::rdmaDevice).orElse(null) : null;
			final var columns = new NicPathRows.Nic(nic.name(), nic.address().getHostAddress(), device,
							device == null ? null : RDMA_PORT,
							planned == null ? null : planned.gidIndex(),
							inventory.nic(nic.name()).map(NicInventory.Nic::numaNode).orElse(null),
							port == null ? null : port.id(),
							shares ? Integer.toString(NicSetSettings.openLimit(concurrencyLimit, count, i)) : "shared");
			nics[i] = new Row(NicPathRows.Kind.NIC, i, columns, null);
			paths[i] = new Row[nic.paths().size()];
			for (var j = 0; j < paths[i].length; j++) {
				final var spec = nic.paths().get(j);
				paths[i][j] = new Row(NicPathRows.Kind.PATH, i, columns, new NicPathRows.Endpoint(spec.endpoint(),
								spec.destination().getAddress().getHostAddress(), spec.destination().getPort()));
			}
		}
		return new NicPathCounters(paths, nics);
	}

	/** The row of the Path that opened {@code channel}, or null for a channel of no Path Pool. */
	public Row path(final Channel channel) {
		final var path = channel.attr(NicPathPool.ATTR_KEY_PATH).get();
		return path == null ? null : paths[path.pool().index()][path.index()];
	}

	/** Path {@code path} of NIC {@code nic}, or the NIC's row for {@code path} -1. */
	public Row path(final int nic, final int path) {
		return path < 0 ? nics[nic] : paths[nic][path];
	}

	/** NIC {@code nic}'s row, or the {@code none} row for {@code nic} -1. */
	public Row nic(final int nic) {
		return nic < 0 ? none : nics[nic];
	}

	/** The rows in file order: each NIC's Paths, then the NICs, then {@code none}. */
	public NicPathRows snapshot(final boolean complete) {
		final var rows = new ArrayList<NicPathRows.Row>();
		for (final var nicPaths : paths) {
			for (final var row : nicPaths) {
				rows.add(row.snapshot());
			}
		}
		for (final var row : nics) {
			rows.add(row.snapshot());
		}
		rows.add(none.snapshot());
		return new NicPathRows(List.copyOf(rows), complete);
	}

	/**
	 * The same rows for a driver with RDMA, with the RDMA columns on every row and each NIC's pool
	 * counts on its row, taken from that context's own driver.
	 *
	 * @param pools the counts of NIC {@code index} in layout order
	 */
	public NicPathRows snapshot(final boolean complete, final IntFunction<NicPathRows.PoolCounts> pools) {
		final var rows = new ArrayList<NicPathRows.Row>();
		for (final var nicPaths : paths) {
			for (final var row : nicPaths) {
				rows.add(row.snapshot(null));
			}
		}
		for (var i = 0; i < nics.length; i++) {
			rows.add(nics[i].snapshot(pools.apply(i)));
		}
		rows.add(none.snapshot(null));
		return new NicPathRows(List.copyOf(rows), complete);
	}
}
