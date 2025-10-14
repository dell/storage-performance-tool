package cmd

import "testing"

func TestInitializeLogger_InvalidLevel(t *testing.T) {
	old := logLevel
	defer func() { logLevel = old }()
	logLevel = "bad-level"
	if err := initializeLogger(); err == nil {
		t.Fatalf("expected error for invalid log level")
	}
}
