package com.dell.spt.base.nic;

import java.util.List;

/**
 * The planner's result for one engine: the NIC set, why every other NIC is unused, the endpoints
 * each NIC serves, the pairing groups and their Paths, and the failures and warnings. It is data:
 * the image command prints it, and later the step start acts on it.
 *
 * @param interfaces the setting's entries as given
 * @param auto whether the setting is {@code auto}
 * @param nicSet the chosen NICs: by interface name under {@code auto}, in list order otherwise
 * @param nics every inventory interface, with whether it is used
 */
public record NicPlan(
				List<String> interfaces,
				boolean auto,
				boolean rdma,
				List<String> nicSet,
				List<PlannedNic> nics,
				List<PlannedEndpoint> endpoints,
				List<SharedPort> ports,
				List<PairingGroup> groups,
				List<Finding> failures,
				List<Finding> warnings) {

	public NicPlan {
		interfaces = List.copyOf(interfaces);
		nicSet = List.copyOf(nicSet);
		nics = List.copyOf(nics);
		endpoints = List.copyOf(endpoints);
		ports = List.copyOf(ports);
		groups = List.copyOf(groups);
		failures = List.copyOf(failures);
		warnings = List.copyOf(warnings);
	}

	/** Whether the step may proceed on this plan. */
	public boolean ok() {
		return failures.isEmpty();
	}

	/**
	 * One interface's part in the plan.
	 *
	 * @param address the IPv4 address it binds, when chosen
	 * @param gidIndex its RoCE v2 GID index on port 1, in an RDMA plan
	 * @param serves the endpoints it serves, in configured order
	 * @param reason why it is unused, when it is
	 * @param paths its Paths over every group
	 */
	public record PlannedNic(
					String name,
					boolean used,
					String address,
					Integer gidIndex,
					List<String> serves,
					Reason reason,
					int paths) {

		public PlannedNic {
			serves = List.copyOf(serves);
		}
	}

	/** Why an interface is unused; {@code with} names the NIC it shares a port with. */
	public record Reason(NicCode code, String with) {}

	/**
	 * One configured endpoint.
	 *
	 * @param address its IPv4 address, resolved once (item 7a), or {@code null}
	 * @param candidates the NICs that reach it
	 * @param tierSpeedMbps the speed of the NICs kept for it under {@code auto}; {@code null} for the
	 *     all-unknown tier and in a list
	 * @param kept the NICs that serve it
	 * @param reductions the candidates dropped for it, with the reason
	 */
	public record PlannedEndpoint(
					String endpoint,
					String address,
					List<String> candidates,
					Integer tierSpeedMbps,
					List<String> kept,
					List<Reduction> reductions) {

		public PlannedEndpoint {
			candidates = List.copyOf(candidates);
			kept = List.copyOf(kept);
			reductions = List.copyOf(reductions);
		}
	}

	public record Reduction(String nic, NicCode code, String with) {}

	/** A physical port that backs more than one interface. */
	public record SharedPort(String id, List<String> nics) {

		public SharedPort {
			nics = List.copyOf(nics);
		}
	}

	/** Endpoints served by the same chosen NICs, and the Paths formed over them (item 7). */
	public record PairingGroup(List<String> nics, List<String> endpoints, List<Path> paths) {

		public PairingGroup {
			nics = List.copyOf(nics);
			endpoints = List.copyOf(endpoints);
			paths = List.copyOf(paths);
		}
	}

	/** A NIC paired with one endpoint it serves. */
	public record Path(String nic, String endpoint) {}

	/** A failure or warning, with the NIC and endpoint it concerns and the fix, when there is one. */
	public record Finding(NicCode code, String text, String nic, String endpoint, String fix) {}
}
