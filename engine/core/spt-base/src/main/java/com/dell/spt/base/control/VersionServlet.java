package com.dell.spt.base.control;

import com.dell.spt.base.buildinfo.EngineBuildInfo;
import com.dell.spt.base.buildinfo.EngineBuildInfoJson;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/** Unauthenticated immutable Engine Build Information endpoint. */
public final class VersionServlet extends HttpServlet {

	private final String json;

	public VersionServlet(final EngineBuildInfo buildInfo) {
		json = EngineBuildInfoJson.serialize(Objects.requireNonNull(buildInfo));
	}

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response)
					throws IOException {
		response.setStatus(HttpServletResponse.SC_OK);
		response.setContentType("application/json");
		response.setCharacterEncoding(StandardCharsets.UTF_8.name());
		response.getWriter().write(json);
	}
}
