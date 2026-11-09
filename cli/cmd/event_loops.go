package cmd

import (
	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/dell/storage-performance-tool/cli/internal/results"
)

// captureEventLoopEngines stores the event-loop reports fetched into root (D88), for every run.
// The pre-summary metadata write keeps them. A report that cannot be read is recorded and never
// fails the run; results from engines without the report store nothing.
func captureEventLoopEngines(root string, stepIDs []string, metadata *runMetadata) {
	if metadata == nil {
		return
	}
	engines, err := results.ReadEventLoopEngines(root, stepIDs, metadata.runNodes())
	metadata.EventLoopEngines = engines
	metadata.EventLoopEnginesError = ""
	if err != nil {
		metadata.EventLoopEnginesError = err.Error()
		logging.LogWarn("auto-results", "event-loop reports not stored", "reason", metadata.EventLoopEnginesError)
	}
}

// runNodes is what the metadata says about the run's slices, as the fetcher reads it back.
func (meta *runMetadata) runNodes() results.RunNodes {
	hosts := make([]results.RunHostRecord, 0, len(meta.Hosts))
	for _, host := range meta.Hosts {
		hosts = append(hosts, results.RunHostRecord{Host: host.Host, Original: host.Original})
	}
	return results.RunNodes{NicNodes: meta.NicNodes, Hosts: results.RunHostNames(hosts)}
}
