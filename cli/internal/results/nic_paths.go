package results

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
)

// nicInterfacesNone is the nicNodes value of a node that never had a NIC set.
const nicInterfacesNone = "none"

// NicNode is one spt_run_params.json nicNodes entry, in slice order: the node's
// --interfaces value, or none.
type NicNode struct {
	Node       int    `json:"node"`
	Host       string `json:"host"`
	Interfaces string `json:"interfaces"`
	Source     string `json:"source,omitempty"`
}

// NicNodeFile is a NIC file one node is expected to write for a step: its per-path CSV
// or its NIC-set report.
type NicNodeFile struct {
	Name string
	Node int
	Host string
}

// NicPathsNodeFileName names node's copy of the step's per-path CSV on the Entry Node.
func NicPathsNodeFileName(stepID string, node int) string {
	return fmt.Sprintf("%s.nic.paths.node-%03d.csv", stepID, node)
}

// NicSetNodeFileName names node's copy of the step's NIC-set report on the Entry Node.
func NicSetNodeFileName(stepID string, node int) string {
	return fmt.Sprintf("%s.nic.set.node-%03d.json", stepID, node)
}

// ExpectedNicPathsFiles lists the per-path CSVs a step should have: one for each of
// its first participants nodes whose value is not none. A single participant writes
// the canonical file; several each write a node-NNN copy.
func ExpectedNicPathsFiles(stepID string, nodes []NicNode, participants int) []NicNodeFile {
	return expectedNicFiles(stepID, nodes, participants, constants.ResultsArtifactSuffixNicPaths, NicPathsNodeFileName)
}

// ExpectedNicSetFiles lists the NIC-set reports a step should have, by the rules of
// ExpectedNicPathsFiles.
func ExpectedNicSetFiles(stepID string, nodes []NicNode, participants int) []NicNodeFile {
	return expectedNicFiles(stepID, nodes, participants, constants.ResultsArtifactSuffixNicSet, NicSetNodeFileName)
}

func expectedNicFiles(stepID string, nodes []NicNode, participants int, suffix string,
	nodeFileName func(string, int) string,
) []NicNodeFile {
	var out []NicNodeFile
	for _, node := range nodes {
		if node.Node < 0 || node.Node >= participants || node.Interfaces == "" || node.Interfaces == nicInterfacesNone {
			continue
		}
		name := stepID + "." + suffix
		if participants > 1 {
			name = nodeFileName(stepID, node.Node)
		}
		out = append(out, NicNodeFile{Name: name, Node: node.Node, Host: node.Host})
	}
	return out
}

// readNicNodes returns the run metadata's nicNodes, or nil when the run has none.
func readNicNodes(dir string) ([]NicNode, error) {
	data, err := os.ReadFile(filepath.Join(dir, constants.ResultsMetadataFileName)) // #nosec G304 -- path is under the selected results directory
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var params struct {
		NicNodes []NicNode `json:"nicNodes"`
	}
	if err := json.Unmarshal(data, &params); err != nil {
		return nil, err
	}
	return params.NicNodes, nil
}

// metricsTotalsNodeCount reads the largest NodeCount of a metrics totals CSV, the
// step's participating nodes.
func metricsTotalsNodeCount(path string) (int, error) {
	file, err := os.Open(path) // #nosec G304 -- path is under the selected results directory
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = file.Close()
	}()
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	header, err := reader.Read()
	if err != nil {
		return 0, err
	}
	column := -1
	for i, name := range header {
		if strings.TrimSpace(name) == "NodeCount" {
			column = i
			break
		}
	}
	if column < 0 {
		return 0, errors.New("no NodeCount column")
	}
	count := 0
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return 0, err
		}
		if column >= len(record) {
			continue
		}
		value, err := strconv.Atoi(strings.TrimSpace(record[column]))
		if err != nil {
			return 0, fmt.Errorf("NodeCount %q: %w", record[column], err)
		}
		count = max(count, value)
	}
	return count, nil
}

// recordMissingNicFiles adds a missing entry for each per-path CSV and NIC-set report
// the step was expected to write and did not. It never makes the step fail.
func (f *Fetcher) recordMissingNicFiles(sm *StepManifest, nodes []NicNode) {
	if len(nodes) == 0 || !hasSuccessfulGenericTotals(*sm) {
		return
	}
	totals := filepath.Join(f.OutputDir, sm.StepID+"."+constants.ResultsArtifactSuffixMetricsTotal)
	participants, err := metricsTotalsNodeCount(totals)
	if err != nil {
		logging.LogWarn("results", "cannot tell which NIC files to expect",
			"step", sm.StepID, "error", err)
		return
	}
	expected := append(ExpectedNicPathsFiles(sm.StepID, nodes, participants),
		ExpectedNicSetFiles(sm.StepID, nodes, participants)...)
	for _, expected := range expected {
		reason := fmt.Sprintf("expected from node-%03d (%s); not listed in index.json", expected.Node, expected.Host)
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
