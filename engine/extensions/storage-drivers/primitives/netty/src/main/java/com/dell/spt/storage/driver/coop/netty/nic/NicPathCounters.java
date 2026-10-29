package com.dell.spt.storage.driver.coop.netty.nic;

import com.dell.spt.base.nic.NicInventory;
import com.dell.spt.base.nic.NicPlan;
import com.dell.spt.base.nic.NicPathRows;
import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

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
		private final NicPathRows.Nic nic;
		private final NicPathRows.Endpoint endpoint;
		private final LongAdder attempts = new LongAdder();
		private final LongAdder succeeded = new LongAdder();
		private final LongAdder failed = new LongAdder();
		private final LongAdder interrupted = new LongAdder();
		private final LongAdder bytes = new LongAdder();

		private Row(final NicPathRows.Kind kind, final NicPathRows.Nic nic, final NicPathRows.Endpoint endpoint) {
			this.kind = kind;
			this.nic = nic;
			this.endpoint = endpoint;
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
	}

	private final Row[][] paths;
	private final Row[] nics;
	private final Row none = new Row(NicPathRows.Kind.NONE, null, null);

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
			final var columns = new NicPathRows.Nic(nic.name(), nic.address().getHostAddress(), null, null,
							planned == null ? null : planned.gidIndex(),
							inventory.nic(nic.name()).map(NicInventory.Nic::numaNode).orElse(null),
							port == null ? null : port.id(),
							shares ? Integer.toString(NicSetSettings.openLimit(concurrencyLimit, count, i)) : "shared");
			nics[i] = new Row(NicPathRows.Kind.NIC, columns, null);
			paths[i] = new Row[nic.paths().size()];
			for (var j = 0; j < paths[i].length; j++) {
				final var spec = nic.paths().get(j);
				paths[i][j] = new Row(NicPathRows.Kind.PATH, columns, new NicPathRows.Endpoint(spec.endpoint(),
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
}
