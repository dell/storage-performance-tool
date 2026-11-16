package cmd

import "testing"

func TestInitializeLogger_InvalidLevel(t *testing.T) {
	isolateLoggingGlobals(t)
	debug = false // debug would override the invalid level
	logLevel = "bad-level"
	if err := initializeLogger(); err == nil {
		t.Fatalf("expected error for invalid log level")
	}
}
