package tui

import "fmt"

// formatTable is a legacy single-node table renderer used by tests only.
func (l *LiveViewRenderer) formatTable(metric *PerformanceMetric, _ int) string {
	progressValue := l.calculateDoneMiB(metric.SuccessCount)
	rateValue := metric.MBPerSec
	colSixValue := metric.SuccessCount
	if l.isListWorkload() {
		rateValue = metric.OpsPerSec
		colSixValue = metric.ConcurrencyCurrent
	}

	var lines []string
	labels := l.tableLabels()
	headerLine := fmt.Sprintf("%5s %3s %9s %8s %6s %7s %8s",
		"Rank", "%", labels.progress, labels.rate, labels.ops, labels.colSix, "Errors")
	lines = append(lines, "\033[7m"+headerLine+"\033[0m")

	lines = append(lines, fmt.Sprintf("%5s %3s %9d %8d %6d %7d %8d",
		"Total", l.formatPercentCell(metric), progressValue, rateValue, metric.OpsPerSec, colSixValue, metric.FailedCount))

	lines = append(lines, fmt.Sprintf("%5s %3s %9d %8d %6d %7d %8d",
		"0", l.formatPercentCell(metric), progressValue, rateValue, metric.OpsPerSec, colSixValue, metric.FailedCount))

	return joinLines(lines)
}

func joinLines(lines []string) string {
	if len(lines) == 0 {
		return ""
	}
	out := lines[0]
	for i := 1; i < len(lines); i++ {
		out += "\n" + lines[i]
	}
	return out
}
