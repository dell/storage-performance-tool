package com.dell.spt.base.control;

import static com.dell.spt.base.config.ConfigFormat.JSON;
import static com.dell.spt.base.config.ConfigFormat.YAML;
import static com.dell.spt.base.config.ConfigUtil.writerWithPrettyPrinter;
import static org.eclipse.jetty.http.MimeTypes.Type.APPLICATION_JSON;
import static org.eclipse.jetty.http.MimeTypes.Type.TEXT_JSON;

import com.dell.spt.base.config.ConfigFormat;
import com.dell.spt.base.config.ConfigUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.io.yaml.TypeNames;
import org.eclipse.jetty.http.HttpHeader;
import java.io.IOException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/** @author veronika K. on 26.10.18 */
public class ConfigServlet extends HttpServlet {

	private static final String SCHEMA_PATH = "schema";
	private static final String CONTEXT_SEP = "/";

	private final Config config;

	public ConfigServlet(final Config config) {
		this.config = config;
	}

	@Override
	protected final void doGet(final HttpServletRequest req, final HttpServletResponse resp)
					throws IOException {
		final var contexts = req.getRequestURI().split(CONTEXT_SEP);
		final var acceptHeader = req.getHeader(HttpHeader.ACCEPT.toString());
		final ConfigFormat configFormat;
		if (null == acceptHeader) {
			configFormat = YAML;
		} else {
			if (acceptHeader.startsWith(APPLICATION_JSON.toString())) {
				configFormat = JSON;
			} else if (acceptHeader.startsWith(TEXT_JSON.toString())) {
				configFormat = JSON;
			} else {
				configFormat = YAML;
			}
		}
		if (contexts.length == 2) {
			getConfig(resp, configFormat);
		} else if (contexts[2].equals(SCHEMA_PATH)) {
			getSchema(resp, configFormat);
		} else {
			resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			resp.getWriter().print("<ERROR> Such URI not found : " + req.getRequestURI());
		}
	}

	private void getSchema(final HttpServletResponse resp, final ConfigFormat format) throws IOException {
		final ObjectMapper mapper;
		switch (format) {
		case JSON:
			mapper = new ObjectMapper();
			break;
		case YAML:
			mapper = new YAMLMapper();
			break;
		default:
			throw new AssertionError();
		}
		var schemaStr = writerWithPrettyPrinter(mapper).writeValueAsString(config.schema());
		for (final var k : TypeNames.MAP.keySet()) {
			final var v = TypeNames.MAP.get(k).getTypeName();
			schemaStr = schemaStr.replaceAll(v, k);
		}
		resp.setStatus(HttpServletResponse.SC_OK);
		final var respWriter = resp.getWriter();
		respWriter.print(schemaStr);
	}

	private void getConfig(final HttpServletResponse resp, final ConfigFormat format) throws IOException {
		resp.setStatus(HttpServletResponse.SC_OK);
		final var respWriter = resp.getWriter();
		respWriter.print(ConfigUtil.toString(config, format));
	}
}
