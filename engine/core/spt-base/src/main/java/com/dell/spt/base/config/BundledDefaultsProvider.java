package com.dell.spt.base.config;

import static com.dell.spt.base.Constants.APP_NAME;
import static com.dell.spt.base.Constants.PATH_DEFAULTS;

import com.github.akurilov.confuse.io.yaml.YamlConfigProviderBase;
import java.io.InputStream;

public class BundledDefaultsProvider extends YamlConfigProviderBase {

	@Override
	protected final InputStream configInputStream() {
		return getClass().getResourceAsStream("/" + PATH_DEFAULTS);
	}

	@Override
	public final String id() {
		return APP_NAME;
	}
}
