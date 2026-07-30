package com.dell.spt.base.integrity;

/** Declared trust boundary for a metadata-mode READ input. */
public enum IntegrityInputProvenance {
	NONE("none"), ENGINE_STEP("engine_step"), CLI_STAGER("cli_stager"), EXTERNAL("external");

	private final String value;

	IntegrityInputProvenance(final String value) {
		this.value = value;
	}

	public String value() {
		return value;
	}
}
