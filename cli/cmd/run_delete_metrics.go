package cmd

import (
	"context"
	"maps"
	"sync"

	"github.com/dell/storage-performance-tool/cli/internal/deletemetrics"
)

// deleteMetricsCollector keeps the terminal DELETE metrics of the DELETE steps that end while a
// run is monitored. The engine drops the metrics row of a finished step after a retention
// period, so a DELETE step that a longer step follows, such as a seeded run's cleanup, has no
// row left when the run ends. The capture at the end still decides for every step it finds.
type deleteMetricsCollector struct {
	mu    sync.Mutex
	steps map[string]*deletemetrics.Metrics
}

// observe reads the DELETE metrics of the run's DELETE steps that have ended so far. For each
// step, the latest response that keeps or refuses it decides. Nothing is read for a run that
// is not a DELETE workload or whose planned DELETE steps have not all started, and a response
// that cannot be read says nothing about any step.
func (c *deleteMetricsCollector) observe(
	ctx context.Context, baseURL string, expectedRunID int64, runtimeStepIDs []string, metadata *runMetadata,
) {
	if metadata == nil || metadata.WorkloadType != WorkloadTypeDelete {
		return
	}
	deleteSteps, err := bindRuntimeDeleteSteps(metadata.ExpectedStepIDs, runtimeStepIDs)
	if err != nil {
		return
	}
	contributorIDs, err := expectedMetricContributorIDs(metadata, metricCaptureKindDelete)
	if err != nil {
		return
	}
	observation, err := observeDeleteMetricsFunc(
		ctx, baseURL, expectedRunID, deleteSteps, contributorIDs, len(contributorIDs) == 1)
	if err != nil || observation == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for stepID := range observation.Refused {
		delete(c.steps, stepID)
	}
	if len(observation.Steps) > 0 && c.steps == nil {
		c.steps = make(map[string]*deletemetrics.Metrics, len(observation.Steps))
	}
	maps.Copy(c.steps, observation.Steps)
}

// observed returns the metrics kept so far, by step. A nil collector observed nothing.
func (c *deleteMetricsCollector) observed() map[string]*deletemetrics.Metrics {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return maps.Clone(c.steps)
}
