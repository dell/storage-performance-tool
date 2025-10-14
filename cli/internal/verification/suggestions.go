/*
Copyright © 2025 Dell Technologies
*/

package verification

import (
	"fmt"
	"regexp"
	"strings"
)

var rePID = regexp.MustCompile(`pid=([0-9]+)|\s([0-9]+)/[A-Za-z0-9._-]+$`)

func extractPIDFromLines(lines []string) string {
	for _, l := range lines {
		l = strings.TrimSpace(l)
		m := rePID.FindStringSubmatch(l)
		if len(m) >= 2 {
			if m[1] != "" {
				return m[1]
			}
			if len(m) >= 3 && m[2] != "" {
				return strings.TrimSpace(m[2])
			}
		}
	}
	return ""
}

// formatSuggestedActions produces a human-friendly suggestion block for resolving conflicts.
// It prefers container stop suggestions when a container maps the port; otherwise falls back to PID-based hints.
func formatSuggestedActions(info *VerificationConflictInfo) string {
	if info == nil || len(info.ConflictPorts) == 0 {
		return ""
	}
	var b strings.Builder
	printed := false
	write := func(line string) {
		if !printed {
			b.WriteString("\nSuggested actions:\n")
			printed = true
		}
		b.WriteString(line)
		if !strings.HasSuffix(line, "\n") {
			b.WriteString("\n")
		}
	}
	for _, port := range info.ConflictPorts {
		if suggestContainerStop(info, port, write) {
			continue
		}
		suggestPIDKill(info, port, write)
	}
	return b.String()
}

func suggestContainerStop(info *VerificationConflictInfo, port int, write func(string)) bool {
	for _, c := range info.Containers {
		for _, m := range c.Ports {
			if strings.Contains(m, fmt.Sprintf(":%d->", port)) || strings.Contains(m, fmt.Sprintf(":%d/", port)) {
				id := c.ID
				if len(id) > 12 {
					id = id[:12]
				}
				if info.Host != nil && !info.Host.IsLocal {
					write(fmt.Sprintf("  - Port %d: ssh %s 'docker stop %s'  # %s", port, info.Host.GetSSHTarget(), id, c.Name))
				} else {
					write(fmt.Sprintf("  - Port %d: docker stop %s  # %s", port, id, c.Name))
				}
				return true
			}
		}
	}
	return false
}

func suggestPIDKill(info *VerificationConflictInfo, port int, write func(string)) {
	if lines, ok := info.PortDetails[port]; ok && len(lines) > 0 {
		if pid := extractPIDFromLines(lines); pid != "" {
			if info.Host != nil && !info.Host.IsLocal {
				write(fmt.Sprintf("  - Port %d: ssh %s 'sudo kill -TERM %s'", port, info.Host.GetSSHTarget(), pid))
			} else {
				write(fmt.Sprintf("  - Port %d: sudo kill -TERM %s", port, pid))
			}
		}
	}
}
