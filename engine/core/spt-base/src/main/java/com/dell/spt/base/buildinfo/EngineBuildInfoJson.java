package com.dell.spt.base.buildinfo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Canonical schema-1 JSON representation shared by every engine build-information surface. */
public final class EngineBuildInfoJson {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private EngineBuildInfoJson() {}

	public static String serialize(final EngineBuildInfo buildInfo) {
		final ObjectNode root = MAPPER.createObjectNode();
		root.put("schema_version", buildInfo.schemaVersion());
		root.put("product", buildInfo.product());
		root.put("version", buildInfo.version());
		root.put("revision", buildInfo.revision());
		root.put("build_time", buildInfo.buildTime());
		root.put("development", buildInfo.development());
		if (buildInfo.sourceDirty() == null) {
			root.putNull("source_dirty");
		} else {
			root.put("source_dirty", buildInfo.sourceDirty());
		}
		try {
			return MAPPER.writeValueAsString(root);
		} catch (final JsonProcessingException e) {
			throw new IllegalStateException("Failed to serialize Engine Build Information", e);
		}
	}
}
