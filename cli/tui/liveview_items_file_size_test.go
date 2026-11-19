package tui

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

func TestCalculateDoneMiBWithoutObjectSizeLogsNothing(t *testing.T) {
	var logged bytes.Buffer
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
	if bytes.Contains(logged.Bytes(), []byte("Failed to parse object size")) {
		t.Fatalf("empty size logged a parse error: %s", logged.String())
	}
}
