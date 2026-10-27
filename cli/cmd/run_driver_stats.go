package cmd

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/driverstats"
	"github.com/dell/storage-performance-tool/cli/internal/results"
)

// driverStatsCollector keeps the terminal driver statistics of the steps that end while a run
// is monitored. The engine drops the metrics row of a finished step after a retention period,
// so a capture at the end of a run finds only the steps that ended last.
//
// It also keeps what it needs to tell whether a step was missed: the steps seen with their
// driver at work, the steps seen completing operations, and the longest time the run's metrics
// went unread. A row the engine keeps for its retention period cannot fall between two reads
// that are closer together than that.
type driverStatsCollector struct {
	mu      sync.Mutex
	now     func() time.Time
	steps   map[string]*driverstats.Stats
	refused map[string]error
	active  map[string]struct{}
	worked  map[string]struct{}
	// lastRead is when the latest successful read began; zero before the first one when the
	// collector was not started with newDriverStatsCollector.
	lastRead   time.Time
	longestGap time.Duration
}

// newDriverStatsCollector starts a collector at the beginning of a run. now is its clock.
func newDriverStatsCollector(now func() time.Time) *driverStatsCollector {
	return &driverStatsCollector{now: now, lastRead: now()}
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
	start := c.clock()
	observation, err := observeDriverStatsFunc(
		ctx, baseURL, expectedRunID, stepIDs, contributorIDs, len(contributorIDs) == 1)
	if err != nil || observation == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.read(start)
	for stepID, stats := range observation.Steps {
		c.keep(stepID, stats)
	}
	for stepID, refusal := range observation.Refused {
		c.refuse(stepID, refusal)
	}
	c.active = addSteps(c.active, observation.Active)
	c.worked = addSteps(c.worked, observation.Worked)
}

// readBeforeSteps records a successful read of the run's metrics, begun at start, that found
// no step of the run: nothing could be missed before it.
func (c *driverStatsCollector) readBeforeSteps(start time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.read(start)
}

// withFinal returns the statistics of every step: those observed during the run and those of
// the capture at its end, which decide for the steps they hold and has just been read. A nil
// collector observed nothing. Partial statistics are never stored, so it refuses them all when
// a step's statistics were refused, when the run's metrics went unread for as long as the
// engine keeps a finished step's row, or when a step whose driver was seen at work has none.
// When the run's driver always reports statistics (alwaysReported), it also refuses them when a
// step that completed operations has none, unless no step has any: the caller records that.
func (c *driverStatsCollector) withFinal(
	final *results.DriverStatsCapture, alwaysReported bool,
) (map[string]*driverstats.Stats, error) {
	if c == nil {
		c = &driverStatsCollector{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.read(c.clock())
	if final != nil {
		for stepID, stats := range final.Steps {
			c.keep(stepID, stats)
		}
		c.worked = addSteps(c.worked, final.Worked)
	}
	if len(c.refused) > 0 {
		return nil, c.refused[slices.Min(slices.Collect(maps.Keys(c.refused)))]
	}
	if c.longestGap >= constants.EngineTerminalRowRetention {
		return nil, fmt.Errorf(
			"the run's metrics went unread for %s; the engine keeps the row of a finished step for %s",
			c.longestGap.Round(time.Second), constants.EngineTerminalRowRetention)
	}
	if stepID, missing := c.unread(c.active); missing {
		return nil, fmt.Errorf("no terminal driver statistics were read for step %q, whose driver did work", stepID)
	}
	if alwaysReported && len(c.steps) > 0 {
		if stepID, missing := c.unread(c.worked); missing {
			return nil, fmt.Errorf(
				"no terminal driver statistics were read for step %q, which completed operations with a driver "+
					"that always reports them", stepID)
		}
	}
	return maps.Clone(c.steps), nil
}

// unread returns the first of the given steps whose statistics were not read.
func (c *driverStatsCollector) unread(stepIDs map[string]struct{}) (string, bool) {
	for _, stepID := range slices.Sorted(maps.Keys(stepIDs)) {
		if _, stored := c.steps[stepID]; !stored {
			return stepID, true
		}
	}
	return "", false
}

func (c *driverStatsCollector) clock() time.Time {
	if c == nil || c.now == nil {
		return time.Now()
	}
	return c.now()
}

// read records a successful read that began at start. The gap it closes runs from the start of
// the previous one to the end of this one: the most time that can lie between what they saw.
func (c *driverStatsCollector) read(start time.Time) {
	if !c.lastRead.IsZero() {
		if gap := c.clock().Sub(c.lastRead); gap > c.longestGap {
			c.longestGap = gap
		}
	}
	c.lastRead = start
}

func (c *driverStatsCollector) keep(stepID string, stats *driverstats.Stats) {
	if c.steps == nil {
		c.steps = make(map[string]*driverstats.Stats)
	}
	c.steps[stepID] = stats
	delete(c.refused, stepID)
}

// addSteps adds the steps of from to into and returns into, made when it was nil.
func addSteps(into, from map[string]struct{}) map[string]struct{} {
	if len(from) == 0 {
		return into
	}
	if into == nil {
		into = make(map[string]struct{}, len(from))
	}
	maps.Copy(into, from)
	return into
}

func (c *driverStatsCollector) refuse(stepID string, refusal error) {
	if c.refused == nil {
		c.refused = make(map[string]error)
	}
	c.refused[stepID] = refusal
	delete(c.steps, stepID)
}
