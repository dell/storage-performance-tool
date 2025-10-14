package tui

import (
	"regexp"
	"strings"
)

// Test-only regex for Spt data lines.
var sptDataLineRegex = regexp.MustCompile(`^(?:\x1b\[[0-9;]*m)*(\d+\.\d+)\|\s*(\d+)\s*\|`)

// getVisibleLinesWithHighlight returns visible lines with optional highlighting for selected timestamp.
// Test-only helper kept for unit tests.
func (m Model) getVisibleLinesWithHighlight(lines []string, scroll int, height int, selectedTimestamp string) []string {
	maxWidth := m.width - ViewportContentPadding
	if maxWidth <= 0 {
		maxWidth = 80
	}
	if len(lines) == 0 {
		return []string{"(empty)"}
	}

	start := scroll
	end := minInt(start+height, len(lines))
	if start >= len(lines) {
		start = maxInt(0, len(lines)-height)
		end = len(lines)
	}

	visible := make([]string, height)
	for i := 0; i < end-start; i++ {
		line := lines[start+i]
		processed := line
		if selectedTimestamp != "" && isSptDataLine(line, selectedTimestamp) {
			clean := stripANSIEscapeSequences(line)
			processed = "\033[33m" + clean + "\033[0m"
		}
		cleanLen := len(stripANSIEscapeSequences(processed))
		if cleanLen > maxWidth && maxWidth > 3 {
			clean := stripANSIEscapeSequences(processed)
			truncated := clean[:maxWidth-3] + "..."
			if selectedTimestamp != "" && isSptDataLine(line, selectedTimestamp) {
				visible[i] = "\033[33m" + truncated + "\033[0m"
			} else {
				visible[i] = truncated
			}
		} else if cleanLen > maxWidth {
			clean := stripANSIEscapeSequences(processed)
			truncated := clean[:maxWidth]
			if selectedTimestamp != "" && isSptDataLine(line, selectedTimestamp) {
				visible[i] = "\033[33m" + truncated + "\033[0m"
			} else {
				visible[i] = truncated
			}
		} else {
			visible[i] = processed
		}
	}
	for i := end - start; i < height; i++ {
		visible[i] = ""
	}
	return visible
}

// getSelectedTimestamp returns the selected sample timestamp; test-only helper.
func (m Model) getSelectedTimestamp() string {
	if m.historicalIndex >= 0 {
		return ""
	}
	selectedIndex := m.getSelectedSampleIndex()
	if selectedIndex < 0 {
		return ""
	}
	samples := m.metricsCollector.GetSamples()
	if selectedIndex >= len(samples) {
		return ""
	}
	return samples[selectedIndex].SptTimestamp
}

// isSptDataLine checks if a line is a Spt data line and optionally matches a timestamp.
func isSptDataLine(line string, targetTimestamp string) bool {
	if strings.HasPrefix(line, "││") || strings.HasPrefix(line, "│") {
		return false
	}
	matches := sptDataLineRegex.FindStringSubmatch(line)
	if matches == nil {
		return false
	}
	if targetTimestamp == "" {
		return true
	}
	return len(matches) > 2 && matches[2] == targetTimestamp
}
