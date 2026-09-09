package com.dell.spt.base.buildinfo;

import java.io.IOException;
import java.io.InputStream;

/** Source boundary used to load packaged engine build metadata. */
public interface EngineBuildInfoSource {

	InputStream openBuildInfoResource() throws IOException;

	String implementationVersion();
}
