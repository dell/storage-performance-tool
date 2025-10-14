package com.dell.spt.base.control;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.rewrite.handler.Rule;

public final class AddCorsHeadersRule extends Rule {

	public static final String HEADER_NAME_PREFIX_AC = "Access-Control";
	public static final String HEADER_NAME_PREFIX_ACA = HEADER_NAME_PREFIX_AC + "-Allow";
	public static final String HEADER_VALUE_ACA_METHODS = "DELETE,GET,HEAD,POST,PUT,OPTIONS";

	public static final String HEADER_NAME_ACA_ORIGIN = HEADER_NAME_PREFIX_ACA + "-Origin";
	public static final String HEADER_VALUE_ACA_ORIGIN = "*";

	public static final String HEADER_NAME_ACA_HEADERS = HEADER_NAME_PREFIX_ACA + "-Headers";
	public static final String HEADER_NAME_ACA_METHODS = HEADER_NAME_PREFIX_ACA + "-Methods";
	public static final String HEADER_NAME_AC_EXPOSE_HEADERS = HEADER_NAME_PREFIX_AC + "-Expose-Headers";
	public static final String ACA_HEADERS = "Origin, X-Requested-With, Content-Type, Accept, ETag, If-Match";

	@Override
	public final String matchAndApply(
					final String target, final HttpServletRequest request, final HttpServletResponse response) {
		// NOTE: In order to reply to CORS preflight request with the appropriate CORS headers, ...
		// ... Access-Control-Allow-Headers should contain the same headers ...
		// ... as Access-Control-Expose-Headers or more.
		response.setHeader(HEADER_NAME_AC_EXPOSE_HEADERS, ACA_HEADERS);
		response.setHeader(HEADER_NAME_ACA_HEADERS, ACA_HEADERS);
		response.setHeader(HEADER_NAME_ACA_METHODS, HEADER_VALUE_ACA_METHODS);
		response.setHeader(HEADER_NAME_ACA_ORIGIN, HEADER_VALUE_ACA_ORIGIN);
		return null;
	}
}
