package tui

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

// lockedBuffer is written by the renderer's CPU monitor while the test reads it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestCalculateDoneMiBWithoutObjectSizeLogsNothing(t *testing.T) {
	var logged lockedBuffer
	previous := logging.GetLogger()
	logging.SetLogger(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { logging.SetLogger(previous) })

	renderer := NewLiveViewRenderer(&scenario.ScenarioParams{
		WorkloadType: scenario.WorkloadTypeRead, ItemsFile: "items.csv",
	})
	defer renderer.Stop()

	if got := renderer.calculateDoneMiB(100); got != 0 {
		t.Fatalf("calculateDoneMiB = %d, want 0", got)
	}
	if text := logged.String(); strings.Contains(text, "Failed to parse object size") {
		t.Fatalf("empty size logged a parse error: %s", text)
	}
}
