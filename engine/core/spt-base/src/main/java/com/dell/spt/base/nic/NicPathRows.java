package com.dell.spt.base.nic;

import java.util.List;
import java.util.Locale;

/**
 * One NIC-set driver's terminal per-path counters (item 18): its rows in file order, and whether
 * every update had stopped when they were taken. Each Request Attempt counts on one row, admitted
 * and then ended; {@link Row#unresolved()} is what has no outcome.
 *
 * @param complete false when the driver's dispatch task or I/O workers had not stopped in time
 */
public record NicPathRows(List<Row>rows,boolean complete){

public NicPathRows{rows=List.copyOf(rows);}

/** Where an attempt ended: on a Path, on a NIC before its Path was chosen, or before a NIC was. */
public enum Kind {
	PATH, NIC, NONE;

	/** The {@code row} column's value. */
	public String label() {
		return name().toLowerCase(Locale.ROOT);
	}

	}

	/**
	 * A NIC's plan columns; a field is null when it does not apply.
	 *
	 * @param sharedPort the planner's label of the physical port it shares
	 * @param permitShare its share of the concurrency limit, or {@code shared}
	 */
	public record Nic(String iface, String ipv4, String rdmaDevice, Integer rdmaPort, Integer gidIndex,
					Integer numaNode, String sharedPort, String permitShare) {}

	/** A Path's endpoint: the configured string and its frozen destination. */
	public record Endpoint(String endpoint, String ip, int port) {}

	/**
	 * One row's counts.
	 *
	 * @param nic null on the {@link Kind#NONE} row
	 * @param endpoint null except on a {@link Kind#PATH} row
	 * @param bytes the payload bytes of the succeeded attempts
	 */
	public record Row(Kind kind, Nic nic, Endpoint endpoint, long attempts, long succeeded, long failed,
					long interrupted, long bytes) {

	/** Attempts sent with no outcome at the step's end. */
	public long unresolved() {
		return attempts - succeeded - failed - interrupted;
	}
}}
