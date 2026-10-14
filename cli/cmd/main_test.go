package cmd

import (
	"context"
	"os"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/driverstats"
)

// engineCaptureDriverStats is the capture the run command ships with. TestMain replaces it so
// that tests which finalize a run against a placeholder engine address stay off the network;
// a test of the capture itself installs this one or its own.
var engineCaptureDriverStats = captureDriverStatsFunc

func TestMain(m *testing.M) {
	captureDriverStatsFunc = func(
		context.Context, string, int64, []string, []string, bool,
	) (map[string]*driverstats.Stats, error) {
		return nil, nil
	}
	os.Exit(m.Run())
}
