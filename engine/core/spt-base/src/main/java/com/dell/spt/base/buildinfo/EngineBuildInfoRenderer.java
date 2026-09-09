package com.dell.spt.base.buildinfo;

import static com.dell.spt.base.Constants.APP_NAME;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/** Human-oriented rendering of the immutable Engine Build Identity snapshot. */
public final class EngineBuildInfoRenderer {

	private static final int ABBREVIATED_REVISION_LENGTH = 12;

	private EngineBuildInfoRenderer() {}

	public static String banner(final EngineBuildInfo buildInfo) {
		final var message = " " + APP_NAME + " v " + buildInfo.version() + " ";
		final var padding = StringUtils.repeat("#", (120 - message.length()) / 2);
		return padding + message + padding;
	}

	public static String startupLine(final EngineBuildInfo buildInfo) {
		final List<String> details = new ArrayList<>();
		details.add(abbreviatedRevision(buildInfo.revision()));
		if (buildInfo.development()) {
			details.add("development");
		}
		if (buildInfo.sourceDirty() == null) {
			details.add("dirty state unknown");
		} else if (buildInfo.sourceDirty()) {
			details.add("dirty");
		}
		return "Engine build: " + buildInfo.version() + " (" + String.join(", ", details) + ")";
	}

	public static List<String> versionDetails(final EngineBuildInfo buildInfo) {
		return List.of(
						"Engine build schema: " + buildInfo.schemaVersion(),
						"Engine product: " + buildInfo.product(),
						"Engine version: " + buildInfo.version(),
						"Engine revision: " + buildInfo.revision(),
						"Engine build time: " + buildInfo.buildTime(),
						"Engine development: " + buildInfo.development(),
						"Engine source dirty: "
										+ (buildInfo.sourceDirty() == null ? EngineBuildInfoProvider.UNKNOWN : buildInfo.sourceDirty()));
	}

	private static String abbreviatedRevision(final String revision) {
		return revision.length() <= ABBREVIATED_REVISION_LENGTH
						? revision
						: revision.substring(0, ABBREVIATED_REVISION_LENGTH);
	}
}
