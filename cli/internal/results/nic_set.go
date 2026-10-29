package results

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

// nicSetSchemaVersion is the nic.set.json schema the CLI reads. Later fields keep it, and
// unknown fields are ignored.
const nicSetSchemaVersion = 1

// NicEngine is one engine's NIC-set report for a step (D64), as spt_run_params.json
// nicEngines stores it. A field the step's drivers disagree on is absent, and Contexts then
// has each driver's values. Nics is empty for a NIC set that never opened.
type NicEngine struct {
	Node             int          `json:"node"`
	Host             string       `json:"host"`
	Drivers          int          `json:"drivers"`
	DriverThreads    *int         `json:"driverThreads,omitempty"`
	HardwareThreads  int          `json:"hardwareThreads"`
	EventLoopsPerNic *int         `json:"eventLoopsPerNic,omitempty"`
	Nics             []string     `json:"nics,omitzero"`
	EventLoops       int          `json:"eventLoops"`
	Contexts         []NicContext `json:"contexts,omitempty"`
}

// NicContext is one NIC-set driver of a step with several.
type NicContext struct {
	ContextIndex     int      `json:"contextIndex"`
	DriverThreads    int      `json:"driverThreads"`
	EventLoopsPerNic int      `json:"eventLoopsPerNic"`
	Nics             []string `json:"nics"`
	EventLoops       int      `json:"eventLoops"`
}

type nicSetReport struct {
	SchemaVersion    int                `json:"schema_version"`
	StepID           string             `json:"step_id"`
	HardwareThreads  int                `json:"hardware_threads"`
	DriverThreads    *int               `json:"driver_threads"`
	EventLoopsPerNic *int               `json:"event_loops_per_nic"`
	Nics             []string           `json:"nics"`
	EventLoops       int                `json:"event_loops"`
	Contexts         []nicSetReportPart `json:"contexts"`
}

type nicSetReportPart struct {
	ContextIndex     int      `json:"context_index"`
	DriverThreads    int      `json:"driver_threads"`
	EventLoopsPerNic int      `json:"event_loops_per_nic"`
	Nics             []string `json:"nics"`
	EventLoops       int      `json:"event_loops"`
}

// ReadNicEngines reads the NIC-set reports fetched into dir, per step in node order. A step
// reads the reports ExpectedNicSetFiles names for its participants. A step without metrics
// totals or a report that is absent adds nothing, since the fetch records it as missing; a
// report that cannot be read is skipped and named in the returned error.
func ReadNicEngines(dir string, stepIDs []string, nodes []NicNode) (map[string][]NicEngine, error) {
	if len(nodes) == 0 {
		return nil, nil
	}
	var out map[string][]NicEngine
	var errs []error
	for _, stepID := range stepIDs {
		participants, err := metricsTotalsNodeCount(filepath.Join(dir, stepID+"."+constants.ResultsArtifactSuffixMetricsTotal))
		if err != nil {
			continue
		}
		for _, expected := range ExpectedNicSetFiles(stepID, nodes, participants) {
			engine, err := readNicSetReport(filepath.Join(dir, expected.Name), stepID)
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
				out = make(map[string][]NicEngine)
			}
			out[stepID] = append(out[stepID], engine)
		}
	}
	return out, errors.Join(errs...)
}

func readNicSetReport(path, stepID string) (NicEngine, error) {
	data, err := os.ReadFile(path) // #nosec G304 -- path is under the selected results directory
	if err != nil {
		return NicEngine{}, err
	}
	var report nicSetReport
	if err := json.Unmarshal(data, &report); err != nil {
		return NicEngine{}, err
	}
	if report.SchemaVersion != nicSetSchemaVersion {
		return NicEngine{}, fmt.Errorf("schema_version %d, want %d", report.SchemaVersion, nicSetSchemaVersion)
	}
	if report.StepID != stepID {
		return NicEngine{}, fmt.Errorf("step_id %q, want %q", report.StepID, stepID)
	}
	engine := NicEngine{
		Drivers:          max(1, len(report.Contexts)),
		DriverThreads:    report.DriverThreads,
		HardwareThreads:  report.HardwareThreads,
		EventLoopsPerNic: report.EventLoopsPerNic,
		Nics:             report.Nics,
		EventLoops:       report.EventLoops,
	}
	for _, part := range report.Contexts {
		engine.Contexts = append(engine.Contexts, NicContext(part))
	}
	return engine, nil
}
