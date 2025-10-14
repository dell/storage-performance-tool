package tui

import (
	"bytes"
	"os"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

// helper to capture stdout during a function call
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = old }()

	fn()

	_ = w.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)
	_ = r.Close()
	return buf.String()
}

func TestMultiHostOrchestrator_Notifier_RoutesMessages(t *testing.T) {
	// Build a minimal orchestrator (no hosts required for notifier behavior)
	o := NewMultiHostOrchestratorWithRMI([]*hostparse.HostInfo{}, 0, RMIConfig{})

	// With notifier set: expect callback to receive the message and stdout to remain empty
	var got []string
	o.SetNotifier(func(msg string) { got = append(got, msg) })

	out := captureStdout(t, func() {
		o.notifyf("API Summary: %d/%d APIs ready", 3, 4)
	})

	if out != "" {
		t.Fatalf("expected no stdout when notifier is set; got: %q", out)
	}
	if len(got) != 1 || got[0] != "API Summary: 3/4 APIs ready" {
		t.Fatalf("unexpected notifier messages: %#v", got)
	}
}

func TestMultiHostOrchestrator_Notifier_FallbackToStdout(t *testing.T) {
	// Build a minimal orchestrator with no notifier
	o := NewMultiHostOrchestratorWithRMI([]*hostparse.HostInfo{}, 0, RMIConfig{})

	out := captureStdout(t, func() {
		o.notifyf("Container Summary: %d/%d containers started successfully\n", 2, 3)
	})

	// notifyf trims trailing newlines; printing uses fmt.Println via fallback, expect one line
	if out == "" {
		t.Fatalf("expected stdout output when notifier is nil")
	}
	if want := "Container Summary: 2/3 containers started successfully\n"; out != want {
		t.Fatalf("stdout mismatch: want %q, got %q", want, out)
	}
}
