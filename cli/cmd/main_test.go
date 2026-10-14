package cmd

import (
	"context"
	"os"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/driverstats"
	"github.com/dell/storage-performance-tool/cli/internal/results"
)

// engineCaptureDriverStats and engineObserveDriverStats are what the run command ships with.
// TestMain replaces them so that tests which monitor or finalize a run against a placeholder
// engine address stay off the network; a test of the capture itself installs these or its own.
var (
	engineCaptureDriverStats = captureDriverStatsFunc
	engineObserveDriverStats = observeDriverStatsFunc
)

func TestMain(m *testing.M) {
	captureDriverStatsFunc = func(
		context.Context, string, int64, []string, []string, bool,
	) (map[string]*driverstats.Stats, error) {
		return nil, nil
	}
	observeDriverStatsFunc = func(
		context.Context, string, int64, []string, []string, bool,
	) (*results.DriverStatsObservation, error) {
		return nil, nil
	}
	os.Exit(m.Run())
}
