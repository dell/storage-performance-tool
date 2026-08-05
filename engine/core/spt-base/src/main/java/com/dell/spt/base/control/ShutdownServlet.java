package com.dell.spt.base.control;

import com.dell.spt.base.svc.Service;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

/**
 * POST /shutdown — gracefully shuts down the node: cancels and joins active work,
 * then closes services. The server stops in {@code Main.runNode()} after the
 * configured linger window.
 */
public final class ShutdownServlet extends HttpServlet {

	private final NodeShutdownCoordinator coordinator;

	public ShutdownServlet(final List<Service> services) {
		this(new NodeShutdownCoordinator(services));
	}

	public ShutdownServlet(final NodeShutdownCoordinator coordinator) {
		this.coordinator = coordinator;
	}

	@Override
	protected void doPost(final HttpServletRequest req, final HttpServletResponse resp) throws IOException {
		// Accept and trigger shutdown asynchronously to avoid blocking the request thread.
		resp.setStatus(HttpServletResponse.SC_ACCEPTED);
		resp.setContentType("application/json");
		resp.getWriter().write("{\"accepted\":true}");
		new Thread(coordinator::shutdown, "shutdown-servlet-thread").start();
	}
}
