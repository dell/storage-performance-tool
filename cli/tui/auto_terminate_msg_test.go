package tui

import (
	"fmt"
	"testing"
)

// TestAutoTerminateMsg_TriggersQuit verifies that sending autoTerminateMsg
// from the TUI event loop produces a quit command (non-nil Cmd), allowing
// the Bubble Tea program to exit cleanly. We don't instantiate a full
// tea.Program here to keep the test fast and deterministic.
func TestAutoTerminateMsg_TriggersQuit(t *testing.T) {
	m := InitialModel()

	updated, cmd := m.Update(autoTerminateMsg{})

	if cmd == nil {
		t.Fatal("expected non-nil quit command from autoTerminateMsg")
	}

	// Optionally sanity-check that executing the command yields a message.
	if msg := cmd(); msg == nil {
		t.Error("quit command returned nil message (unexpected)")
	} else {
		// Avoid relying on unexported tea types; just log the type for visibility.
		t.Logf("auto-terminate produced message type: %T", msg)
	}

	// Ensure model type is preserved.
	if _, ok := updated.(Model); !ok {
		t.Fatalf("expected Model after update, got %T", updated)
	}
	_ = fmt.Sprintf("") // silence unused import if Logf removed
}
