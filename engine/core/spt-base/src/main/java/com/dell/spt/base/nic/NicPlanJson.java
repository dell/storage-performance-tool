package com.dell.spt.base.nic;

import com.dell.spt.base.nic.NicInventory.Nic;
import com.dell.spt.base.nic.NicInventory.RdmaDevice;
import com.dell.spt.base.nic.NicInventory.Sysctls;
import com.dell.spt.base.nic.NicPlan.Finding;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;

/**
 * The image command's output, format {@code spt.nic-plan}. Adding fields or codes keeps {@link
 * #VERSION}; removing or renaming a field, or changing what one means, increments it.
 */
public final class NicPlanJson {

	public static final String FORMAT = "spt.nic-plan";
	public static final int VERSION = 1;

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private NicPlanJson() {}

	/** The engine build that produced the output. */
	public record Engine(String version, String revision) {}

	/**
	 * Renders the command's output document.
	 *
	 * @param plan {@code null} without {@code --interfaces}
	 */
	public static String serialize(final Engine engine, final String host, final NicInventory inventory, final NicPlan plan, final boolean pretty) {
		final ObjectNode root = MAPPER.createObjectNode();
		root.put("format", FORMAT);
		root.put("version", VERSION);
		final var engineNode = root.putObject("engine");
		engineNode.put("version", engine.version());
		engineNode.put("revision", engine.revision());
		root.put("host", host);
		root.set("inventory", inventory(inventory));
		if (plan != null) {
			root.set("plan", plan(plan));
		}
		try {
			return pretty ? MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(root) : MAPPER.writeValueAsString(root);
		} catch (final JsonProcessingException e) {
			throw new IllegalStateException("Failed to serialize the NIC plan", e);
		}
	}

	private static ObjectNode inventory(final NicInventory inventory) {
		final var node = MAPPER.createObjectNode();
		final var nics = node.putArray("nics");
		for (final var nic : inventory.nics()) {
			nics.add(nic(nic));
		}
		final var devices = node.putArray("rdmaDevices");
		for (final var device : inventory.rdmaDevices()) {
			devices.add(rdmaDevice(device));
		}
		node.set("sysctls", sysctls(inventory.allSysctls()));
		return node;
	}

	private static ObjectNode nic(final Nic nic) {
		final var node = MAPPER.createObjectNode();
		node.put("name", nic.name());
		node.put("kind", nic.kind().id());
		node.put("up", nic.up());
		strings(node.putArray("addresses"), nic.addresses().stream().map(Ipv4Prefix::toString).toList());
		node.put("speedMbps", nic.speedMbps());
		node.put("mtu", nic.mtu());
		node.put("numaNode", nic.numaNode());
		node.put("port", nic.port());
		strings(node.putArray("lowerDevices"), nic.lowerDevices());
		node.put("bondMaster", nic.bondMaster());
		node.put("bondHashPolicy", nic.bondHashPolicy());
		node.put("rdmaDevice", nic.rdmaDevice());
		final var gids = node.putArray("gids");
		for (final var gid : nic.gids()) {
			final var gidNode = gids.addObject();
			gidNode.put("device", gid.device());
			gidNode.put("port", gid.port());
			gidNode.put("index", gid.index());
			gidNode.put("address", Ipv4.format(gid.address()));
			gidNode.put("roceV2", gid.roceV2());
		}
		node.set("sysctls", sysctls(nic.sysctls()));
		return node;
	}

	private static ObjectNode rdmaDevice(final RdmaDevice device) {
		final var node = MAPPER.createObjectNode();
		node.put("name", device.name());
		node.put("driver", device.driver());
		node.put("dcSupported", device.dcSupported());
		node.put("activeMtu", device.activeMtu());
		final var ports = node.putArray("ports");
		for (final var port : device.ports()) {
			final var portNode = ports.addObject();
			portNode.put("number", port.number());
			portNode.put("state", port.state());
			portNode.put("linkLayer", port.linkLayer());
		}
		return node;
	}

	private static ObjectNode sysctls(final Sysctls sysctls) {
		final var node = MAPPER.createObjectNode();
		node.put("arpIgnore", sysctls.arpIgnore());
		node.put("arpAnnounce", sysctls.arpAnnounce());
		node.put("rpFilter", sysctls.rpFilter());
		return node;
	}

	private static ObjectNode plan(final NicPlan plan) {
		final var node = MAPPER.createObjectNode();
		strings(node.putArray("interfaces"), plan.interfaces());
		node.put("mode", plan.auto() ? "auto" : "list");
		node.put("rdma", plan.rdma());
		node.put("ok", plan.ok());
		strings(node.putArray("nicSet"), plan.nicSet());
		final var nics = node.putArray("nics");
		for (final var nic : plan.nics()) {
			final var nicNode = nics.addObject();
			nicNode.put("name", nic.name());
			nicNode.put("used", nic.used());
			nicNode.put("address", nic.address());
			nicNode.put("gidIndex", nic.gidIndex());
			strings(nicNode.putArray("serves"), nic.serves());
			nicNode.put("paths", nic.paths());
			if (nic.reason() == null) {
				nicNode.putNull("reason");
			} else {
				final var reason = nicNode.putObject("reason");
				reason.put("code", nic.reason().code().id());
				reason.put("text", nic.reason().code().text());
				putIfPresent(reason, "with", nic.reason().with());
			}
		}
		final var endpoints = node.putArray("endpoints");
		for (final var endpoint : plan.endpoints()) {
			final var endpointNode = endpoints.addObject();
			endpointNode.put("endpoint", endpoint.endpoint());
			endpointNode.put("address", endpoint.address());
			strings(endpointNode.putArray("candidates"), endpoint.candidates());
			endpointNode.put("tierSpeedMbps", endpoint.tierSpeedMbps());
			strings(endpointNode.putArray("kept"), endpoint.kept());
			final var reductions = endpointNode.putArray("reductions");
			for (final var reduction : endpoint.reductions()) {
				final var reductionNode = reductions.addObject();
				reductionNode.put("nic", reduction.nic());
				reductionNode.put("code", reduction.code().id());
				putIfPresent(reductionNode, "with", reduction.with());
			}
		}
		final var ports = node.putArray("ports");
		for (final var port : plan.ports()) {
			final var portNode = ports.addObject();
			portNode.put("id", port.id());
			strings(portNode.putArray("nics"), port.nics());
		}
		final var groups = node.putArray("groups");
		for (final var group : plan.groups()) {
			final var groupNode = groups.addObject();
			strings(groupNode.putArray("nics"), group.nics());
			strings(groupNode.putArray("endpoints"), group.endpoints());
			final var paths = groupNode.putArray("paths");
			for (final var path : group.paths()) {
				final var pathNode = paths.addObject();
				pathNode.put("nic", path.nic());
				pathNode.put("endpoint", path.endpoint());
			}
		}
		findings(node.putArray("failures"), plan.failures());
		findings(node.putArray("warnings"), plan.warnings());
		return node;
	}

	private static void findings(final ArrayNode array, final List<Finding> findings) {
		for (final var finding : findings) {
			final var node = array.addObject();
			node.put("code", finding.code().id());
			node.put("text", finding.text());
			putIfPresent(node, "nic", finding.nic());
			putIfPresent(node, "endpoint", finding.endpoint());
			putIfPresent(node, "fix", finding.fix());
		}
	}

	private static void strings(final ArrayNode array, final List<String> values) {
		values.forEach(array::add);
	}

	private static void putIfPresent(final ObjectNode node, final String field, final String value) {
		if (value != null) {
			node.put(field, value);
		}
	}
}
