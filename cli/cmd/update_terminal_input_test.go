/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
	"os"
	"strings"
	"testing"
)

// spt update asks for confirmation only on a terminal: /dev/null is a
// character device, not a terminal, so it gets "requires --yes" instead of a
// prompt that reads end of input.
func TestIsTerminalInputFalseForDevNull(t *testing.T) {
	devNull, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatalf("open %s: %v", os.DevNull, err)
	}
	t.Cleanup(func() { _ = devNull.Close() })
	if isTerminalInput(devNull) {
		t.Errorf("isTerminalInput(%s) = true, want false", os.DevNull)
	}
	if isTerminalInput(strings.NewReader("y\n")) {
		t.Error("isTerminalInput(a strings.Reader) = true, want false")
	}
}
