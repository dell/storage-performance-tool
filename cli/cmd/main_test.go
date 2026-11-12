package cmd

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/results"
)

// engineCaptureDriverStats, engineObserveDriverStats and engineObserveDeleteMetrics are what the
// run command ships with. TestMain replaces them so that tests which monitor or finalize a run
// against a placeholder engine address stay off the network; a test of the capture itself
// installs these or its own.
var (
	engineCaptureDriverStats   = captureDriverStatsFunc
	engineObserveDriverStats   = observeDriverStatsFunc
	engineObserveDeleteMetrics = observeDeleteMetricsFunc
)

func TestMain(m *testing.M) {
	captureDriverStatsFunc = func(
		context.Context, string, int64, []string, []string, bool,
	) (*results.DriverStatsCapture, error) {
		return nil, nil
	}
	observeDriverStatsFunc = func(
		context.Context, string, int64, []string, []string, bool,
	) (*results.DriverStatsObservation, error) {
		return nil, nil
	}
	observeDeleteMetricsFunc = func(
		context.Context, string, int64, []string, []string, bool,
	) (*results.DeleteMetricsObservation, error) {
		return nil, nil
	}
	// The shipped request posts /shutdown to every host's API port, which stops any engine
	// listening there, such as one a developer runs on 127.0.0.1:9999 beside the tests. A test
	// that checks the shutdown installs its own recorder; the tests of requestShutdownAll call it
	// directly.
	requestShutdownAllFunc = func(
		context.Context, []*hostparse.HostInfo, string, time.Duration, int64, bool,
	) error {
		return nil
	}
	os.Exit(m.Run())
}
