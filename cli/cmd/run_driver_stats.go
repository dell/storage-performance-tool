package cmd

import (
	"context"
	"maps"
	"slices"
	"sync"

	"github.com/dell/storage-performance-tool/cli/internal/driverstats"
)

// driverStatsCollector keeps the terminal driver statistics of the steps that end while a run
// is monitored. The engine drops the metrics row of a finished step after a retention period,
// so a capture at the end of a run finds only the steps that ended last.
type driverStatsCollector struct {
	mu      sync.Mutex
	steps   map[string]*driverstats.Stats
	refused map[string]error
}

// observe reads the statistics of the steps that have ended so far. For each step, the latest
// response that carries its terminal row decides. A response that cannot be read is skipped:
// it says nothing about any step, and the row of a finished step outlives several observations.
func (c *driverStatsCollector) observe(
	ctx context.Context, baseURL string, expectedRunID int64, stepIDs []string, metadata *runMetadata,
) {
	contributorIDs, err := expectedMetricContributorIDs(metadata, metricCaptureKindDriverStats)
	if err != nil {
		return
	}
	observation, err := observeDriverStatsFunc(
		ctx, baseURL, expectedRunID, stepIDs, contributorIDs, len(contributorIDs) == 1)
	if err != nil || observation == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for stepID, stats := range observation.Steps {
		c.keep(stepID, stats)
	}
	for stepID, refusal := range observation.Refused {
		c.refuse(stepID, refusal)
	}
}

// withFinal returns the statistics of every step: those observed during the run and those of
// the capture at its end, which decide for the steps they hold. A step whose statistics were
// refused refuses them all, because partial statistics are never stored.
func (c *driverStatsCollector) withFinal(
	final map[string]*driverstats.Stats,
) (map[string]*driverstats.Stats, error) {
	if c == nil {
		return final, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for stepID, stats := range final {
		c.keep(stepID, stats)
	}
	if len(c.refused) > 0 {
		return nil, c.refused[slices.Min(slices.Collect(maps.Keys(c.refused)))]
	}
	return maps.Clone(c.steps), nil
}

func (c *driverStatsCollector) keep(stepID string, stats *driverstats.Stats) {
	if c.steps == nil {
		c.steps = make(map[string]*driverstats.Stats)
	}
	c.steps[stepID] = stats
	delete(c.refused, stepID)
}

func (c *driverStatsCollector) refuse(stepID string, refusal error) {
	if c.refused == nil {
		c.refused = make(map[string]error)
	}
	c.refused[stepID] = refusal
	delete(c.steps, stepID)
}
