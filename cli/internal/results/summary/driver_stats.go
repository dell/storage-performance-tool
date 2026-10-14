package summary

import (
	"fmt"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/driverstats"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

const (
	rdmaDataPathTitle = "RDMA Data Path"
	// rdmaMixedStepScope labels the line of a mixed step: the driver counts every payload
	// operation of the step together, so the line belongs to no single operation.
	rdmaMixedStepScope = "all payload operations of the step"
	rdmaRequestsLabel  = "Requests"
	rdmaProblemLabel   = "Problem"
)

// rdmaDataPathOf returns the step's RDMA data path when the step has one to report: its driver
// was S3-RDMA and it sent payload requests or met a problem.
func rdmaDataPathOf(step *StepSummary) (driverstats.RDMADataPath, bool) {
	path, ok := step.DriverStats.RDMADataPath()
	return path, ok && path.Reported()
}

func rdmaStepLabel(step *StepSummary) string {
	return nonEmpty(step.PhaseLabel, step.StepID)
}

// formatRDMARequests is the one line every reported step gets. It gives counts of requests and
// no share of them: the counts do not say how much data moved over either path.
func formatRDMARequests(path driverstats.RDMADataPath) string {
	return fmt.Sprintf("%d transfers reported over RDMA, %d payload requests sent over HTTP",
		path.Transferred, path.OverHTTP)
}

func formatRDMAProblem(problem driverstats.RDMAProblem) string {
	return fmt.Sprintf("%d %s", problem.Count, problem.What)
}

// rdmaDataPathWarnings returns the warnings about the S3-RDMA data path of a run.
// captureError is the stored reason why the run kept no driver statistics.
func rdmaDataPathWarnings(steps []StepSummary, workload WorkloadSummary, captureError string) []string {
	var warnings []string
	for i := range steps {
		if path, ok := rdmaDataPathOf(&steps[i]); ok && path.Transferred == 0 {
			warnings = append(warnings, fmt.Sprintf(
				"%s: the S3-RDMA driver ran and no payload moved over RDMA", rdmaStepLabel(&steps[i])))
		}
	}
	// Without statistics the driver is known only from the stored selection. A custom scenario
	// and results stored by an older CLI give no reason, or no selection, and no warning.
	captureError = strings.TrimSpace(captureError)
	if captureError != "" && strings.EqualFold(strings.TrimSpace(workload.S3Driver), scenario.S3DriverRdma) {
		warnings = append(warnings, "S3-RDMA driver statistics are not available: "+captureError)
	}
	return warnings
}

// renderRDMADataPath writes the RDMA data path of every step that has one to report. A width
// above zero wraps the lines for a narrow terminal.
func (r *Renderer) renderRDMADataPath(b *strings.Builder, summary *RunSummary, width int) {
	bullet := func(label, value string) {
		if width > 0 {
			r.writeWrappedBullet(b, label, value, width)
			return
		}
		r.writeBullet(b, label, value)
	}
	for i := range summary.Steps {
		step := &summary.Steps[i]
		path, ok := rdmaDataPathOf(step)
		if !ok {
			continue
		}
		heading := fmt.Sprintf("%s — %s", rdmaDataPathTitle, rdmaStepHeading(step))
		if width > 0 {
			r.writeWrappedLine(b, heading, width)
		} else {
			b.WriteString(heading)
			b.WriteByte('\n')
		}
		bullet(rdmaRequestsLabel, formatRDMARequests(path))
		for _, problem := range path.Problems {
			bullet(rdmaProblemLabel, formatRDMAProblem(problem))
		}
		b.WriteByte('\n')
	}
}

func rdmaStepHeading(step *StepSummary) string {
	label := rdmaStepLabel(step)
	if step.IsMixed {
		return fmt.Sprintf("%s (%s)", label, rdmaMixedStepScope)
	}
	return label
}
