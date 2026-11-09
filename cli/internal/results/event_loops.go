package results

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

// eventLoopsSchemaVersion is the event.loops.json schema the CLI reads. Later fields keep it,
// and unknown fields are ignored.
const eventLoopsSchemaVersion = 1

// Event-loop report values: a context's route, and where its count came from.
const (
	EventLoopRouteDefault        = "default"
	EventLoopRouteNicSet         = "nic_set"
	EventLoopSourceDefault       = "default"
	EventLoopSourceDriverThreads = "storage.driver.threads"
)

// EventLoopEngine is one engine's event-loop report for a step (D88), as spt_run_params.json
// eventLoopEngines stores it. EventLoops is the engine's total over Contexts, each driver
// context that owns Netty event loops; a step whose drivers own none has no contexts.
type EventLoopEngine struct {
	Node            int                `json:"node"`
	Host            string             `json:"host,omitempty"`
	HardwareThreads int                `json:"hardwareThreads"`
	EventLoops      int                `json:"eventLoops"`
	Contexts        []EventLoopContext `json:"contexts,omitempty"`
}

// EventLoopContext is one driver context's event loops. A NIC-set context counts every loop it
// owns: its per-NIC groups and its base group. Source is storage.driver.threads when that
// setting is above 0, otherwise default.
type EventLoopContext struct {
	ContextIndex  int    `json:"contextIndex"`
	Route         string `json:"route"`
	DriverThreads int    `json:"driverThreads"`
	Source        string `json:"source"`
	EventLoops    int    `json:"eventLoops"`
}

type eventLoopsReport struct {
	SchemaVersion   int                     `json:"schema_version"`
	StepID          string                  `json:"step_id"`
	HardwareThreads int                     `json:"hardware_threads"`
	EventLoops      int                     `json:"event_loops"`
	Contexts        []eventLoopsReportEntry `json:"contexts"`
}

type eventLoopsReportEntry struct {
	ContextIndex  int    `json:"context_index"`
	Route         string `json:"route"`
	DriverThreads int    `json:"driver_threads"`
	Source        string `json:"source"`
	EventLoops    int    `json:"event_loops"`
}

// RunHostRecord is the part of an spt_run_params.json hosts entry that names the host.
type RunHostRecord struct {
	Host     string `json:"host"`
	Original string `json:"original"`
}

// RunHostNames names each configured host, in order: its host, or as it was given.
func RunHostNames(hosts []RunHostRecord) []string {
	if len(hosts) == 0 {
		return nil
	}
	out := make([]string, 0, len(hosts))
	for _, host := range hosts {
		name := strings.TrimSpace(host.Host)
		if name == "" {
			name = strings.TrimSpace(host.Original)
		}
		out = append(out, name)
	}
	return out
}

// EventLoopsNodeFileName names node's copy of the step's event-loop report on the Entry Node.
func EventLoopsNodeFileName(stepID string, node int) string {
	return fmt.Sprintf("%s.event.loops.node-%03d.json", stepID, node)
}

// sliceHost names the host of slice node of a step with participants: nicNodes' host, which
// follows the run's locked participants, or else the configured host. The Entry Node is always
// the first configured host; a worker is named only when every configured host took part,
// since a worker that did not start moves the later ones up.
func sliceHost(nodes RunNodes, node, participants int) string {
	for _, nicNode := range nodes.NicNodes {
		if nicNode.Node == node {
			return nicNode.Host
		}
	}
	if len(nodes.NicNodes) > 0 || node >= len(nodes.Hosts) {
		return ""
	}
	if node == 0 || len(nodes.Hosts) == participants {
		return nodes.Hosts[node]
	}
	return ""
}

// ExpectedEventLoopsFiles lists the event-loop reports a step should have: one for each of its
// participants. A single participant writes the canonical file; several each write a node-NNN
// copy.
func ExpectedEventLoopsFiles(stepID string, nodes RunNodes, participants int) []NicNodeFile {
	out := make([]NicNodeFile, 0, max(participants, 0))
	for node := range max(participants, 0) {
		name := stepID + "." + constants.ResultsArtifactSuffixEventLoops
		if participants > 1 {
			name = EventLoopsNodeFileName(stepID, node)
		}
		out = append(out, NicNodeFile{Name: name, Node: node, Host: sliceHost(nodes, node, participants)})
	}
	return out
}

// MissingEventLoopsFiles returns the node copies of a distributed step's event-loop reports that
// its index does not list as fetched, once at least one is. A step with one participant, or with
// no copy at all, came from engines that may predate the report, so nothing is missing.
func MissingEventLoopsFiles(sm *StepManifest, nodes RunNodes, participants int) []NicNodeFile {
	if sm == nil || participants < 2 {
		return nil
	}
	expected := ExpectedEventLoopsFiles(sm.StepID, nodes, participants)
	var missing []NicNodeFile
	fetched := 0
	for _, file := range expected {
		if stepFileFetched(sm, file.Name) {
			fetched++
		} else {
			missing = append(missing, file)
		}
	}
	if fetched == 0 {
		return nil
	}
	return missing
}

func stepFileFetched(sm *StepManifest, name string) bool {
	for _, status := range sm.Files {
		if status.Name == name && status.Status == fileStatusOK {
			return true
		}
	}
	return false
}

// recordMissingEventLoopFiles adds a missing entry for each node copy of the step's event-loop
// report that MissingEventLoopsFiles finds. It never makes the step fail.
func (f *Fetcher) recordMissingEventLoopFiles(sm *StepManifest, nodes RunNodes) {
	if !hasSuccessfulGenericTotals(*sm) {
		return
	}
	totals := filepath.Join(f.OutputDir, sm.StepID+"."+constants.ResultsArtifactSuffixMetricsTotal)
	participants, err := metricsTotalsNodeCount(totals)
	if err != nil {
		// recordMissingNicFiles warns for a run with a NIC set; the report is optional evidence.
		return
	}
	for _, expected := range MissingEventLoopsFiles(sm, nodes, participants) {
		reason := fmt.Sprintf("expected from node-%03d; not listed in index.json", expected.Node)
		if expected.Host != "" {
			reason = fmt.Sprintf("expected from node-%03d (%s); not listed in index.json", expected.Node, expected.Host)
		}
		index := -1
		for i := range sm.Files {
			if sm.Files[i].Name == expected.Name {
				index = i
				break
			}
		}
		switch {
		case index < 0:
			sm.Files = append(sm.Files, FileStatus{Name: expected.Name, Status: fileStatusMissing, Error: reason})
		case sm.Files[index].Status == fileStatusMissing && sm.Files[index].Error == "":
			sm.Files[index].Error = reason
		}
	}
}

// ReadEventLoopEngines reads the event-loop reports fetched into dir, per step in node order,
// for each step's participants. A step without metrics totals or a report that is absent adds
// nothing; a report that cannot be read is skipped and named in the returned error. A step whose
// engines all report no contexts is kept, with empty contexts.
func ReadEventLoopEngines(dir string, stepIDs []string, nodes RunNodes) (map[string][]EventLoopEngine, error) {
	var out map[string][]EventLoopEngine
	var errs []error
	for _, stepID := range stepIDs {
		participants, err := metricsTotalsNodeCount(filepath.Join(dir, stepID+"."+constants.ResultsArtifactSuffixMetricsTotal))
		if err != nil {
			continue
		}
		for _, expected := range ExpectedEventLoopsFiles(stepID, nodes, participants) {
			engine, err := readEventLoopsReport(filepath.Join(dir, expected.Name), stepID)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", expected.Name, err))
				continue
			}
			engine.Node = expected.Node
			engine.Host = expected.Host
			if out == nil {
				out = make(map[string][]EventLoopEngine)
			}
			out[stepID] = append(out[stepID], engine)
		}
	}
	return out, errors.Join(errs...)
}

func readEventLoopsReport(path, stepID string) (EventLoopEngine, error) {
	data, err := os.ReadFile(path) // #nosec G304 -- path is under the selected results directory
	if err != nil {
		return EventLoopEngine{}, err
	}
	var report eventLoopsReport
	if err := json.Unmarshal(data, &report); err != nil {
		return EventLoopEngine{}, err
	}
	if report.SchemaVersion != eventLoopsSchemaVersion {
		return EventLoopEngine{}, fmt.Errorf("schema_version %d, want %d", report.SchemaVersion, eventLoopsSchemaVersion)
	}
	if report.StepID != stepID {
		return EventLoopEngine{}, fmt.Errorf("step_id %q, want %q", report.StepID, stepID)
	}
	engine := EventLoopEngine{HardwareThreads: report.HardwareThreads, EventLoops: report.EventLoops}
	for _, entry := range report.Contexts {
		engine.Contexts = append(engine.Contexts, EventLoopContext(entry))
	}
	return engine, nil
}
