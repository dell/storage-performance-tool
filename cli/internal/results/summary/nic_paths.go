package summary

import (
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/results"
)

var nicPathsNodeCopy = regexp.MustCompile(`^nic\.paths\.node-[0-9]{3}\.csv$`)

// NicPathsEvidence names a step's per-path CSVs. The summary never parses them.
type NicPathsEvidence struct {
	// Files are the fetched files, in node order.
	Files []string
	// Distributed is set when the step's files are per-node copies.
	Distributed bool
	// Nodes counts the files fetched or expected.
	Nodes int
	// Missing are the expected files that were not fetched.
	Missing []results.NicNodeFile
}

// nicPathsEvidence returns the step's per-path CSVs, or nil when it has none and
// expects none. The participants are the first NodeCount nodes of nicNodes.
func nicPathsEvidence(sm *results.StepManifest, nicNodes []results.NicNode, metrics *MetricsTotals) *NicPathsEvidence {
	canonical := sm.StepID + "." + constants.ResultsArtifactSuffixNicPaths
	fetched := make(map[string]bool)
	evidence := &NicPathsEvidence{}
	for _, file := range sm.Files {
		if file.Status != fileStatusOK {
			continue
		}
		copyName, isStepFile := strings.CutPrefix(file.Name, sm.StepID+".")
		switch {
		case isStepFile && nicPathsNodeCopy.MatchString(copyName):
			evidence.Distributed = true
		case file.Name != canonical:
			continue
		}
		fetched[file.Name] = true
		evidence.Files = append(evidence.Files, file.Name)
	}
	participants := nicStepParticipants(metrics)
	expected := results.ExpectedNicPathsFiles(sm.StepID, nicNodes, participants)
	evidence.Missing = missingNicFiles(sm, expected)
	if len(evidence.Files) == 0 && len(evidence.Missing) == 0 {
		return nil
	}
	evidence.Distributed = evidence.Distributed || participants > 1
	if evidence.Distributed {
		// A single engine's canonical name is not one of the node copies.
		evidence.Files = removeString(evidence.Files, canonical)
		delete(fetched, canonical)
	}
	sort.Strings(evidence.Files)
	nodes := make(map[string]bool, len(fetched)+len(expected))
	for name := range fetched {
		nodes[name] = true
	}
	for _, file := range expected {
		nodes[file.Name] = true
	}
	evidence.Nodes = len(nodes)
	return evidence
}

// nicPathsLine is the step's one summary line, or "" when it wrote no file.
func nicPathsLine(stepID string, evidence *NicPathsEvidence) string {
	if evidence == nil || len(evidence.Files) == 0 {
		return ""
	}
	if !evidence.Distributed {
		return "NIC paths: " + evidence.Files[0]
	}
	counts := fmt.Sprintf("%d of %d nodes", len(evidence.Files), evidence.Nodes)
	if len(evidence.Missing) > 0 {
		counts += "; missing " + strings.Join(missingNicNodes(evidence.Missing), "; ")
	}
	return fmt.Sprintf("NIC paths: %s.nic.paths.node-NNN.csv (%s)", stepID, counts)
}

// nicStepParticipants is the step's participating nodes: the largest NodeCount of its metrics
// totals, or none without them.
func nicStepParticipants(metrics *MetricsTotals) int {
	participants := 0
	if metrics != nil {
		for _, row := range metrics.Rows {
			participants = max(participants, int(row.NodeCount))
		}
	}
	return participants
}

// missingNicFiles returns the expected files that the step's index does not list as fetched.
func missingNicFiles(sm *results.StepManifest, expected []results.NicNodeFile) []results.NicNodeFile {
	var missing []results.NicNodeFile
	for _, file := range expected {
		if !slices.ContainsFunc(sm.Files, func(status results.FileStatus) bool {
			return status.Name == file.Name && status.Status == fileStatusOK
		}) {
			missing = append(missing, file)
		}
	}
	return missing
}

// missingNicNodes names each missing file's node and, when it is known, its host, such as
// "node-001, worker-b".
func missingNicNodes(missing []results.NicNodeFile) []string {
	out := make([]string, 0, len(missing))
	for _, file := range missing {
		label := fmt.Sprintf("node-%03d", file.Node)
		if file.Host != "" {
			label += ", " + file.Host
		}
		out = append(out, label)
	}
	return out
}

func (r *Renderer) renderNicPaths(b *strings.Builder, summary *RunSummary, width int) {
	wrote := false
	for i := range summary.Steps {
		line := nicPathsLine(summary.Steps[i].StepID, summary.Steps[i].NicPaths)
		if line == "" {
			continue
		}
		if width > 0 {
			r.writeWrappedLine(b, line, width)
		} else {
			b.WriteString(line)
			b.WriteByte('\n')
		}
		wrote = true
	}
	if wrote {
		b.WriteByte('\n')
	}
}
