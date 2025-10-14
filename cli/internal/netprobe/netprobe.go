/*
Copyright © 2025 Dell Technologies
*/

package netprobe

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

// Listener represents a single listening socket entry parsed from ss/netstat
type Listener struct {
	Address string
	Port    int
	Process string
	PID     int
	Raw     string
}

var (
	// :<port> matcher
	rePort = regexp.MustCompile(`:([0-9]{2,5})\b`)
	// ss users field: users:("proc",pid=123,fd=..)
	reSSUsers = regexp.MustCompile(`users:\([^\)]*?"?([\w.-]+)"?,pid=([0-9]+)`)
)

// SnapshotListeners gets all listening TCP sockets (with pids) using ss or netstat
func SnapshotListeners(ctx context.Context, exec command.CommandExecutor, host *hostparse.HostInfo) ([]Listener, error) {
	cmd := []string{"sh", "-c", constants.PortSnapshotCommand}
	stdout, stderr, err := exec.ExecuteCommand(ctx, host, cmd)
	if err != nil {
		return nil, fmt.Errorf("snapshot failed: %w (stderr: %s)", err, stderr)
	}
	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	out := make([]Listener, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		l := Listener{Raw: line}
		parts := strings.Fields(line)
		l.Port, l.Address = parseAddressAndPort(line, parts)
		l.Process, l.PID = parseProcPID(line, parts)
		if l.Port != 0 {
			out = append(out, l)
		}
	}
	return out, nil
}

// FilterByPort returns listeners matching given port
func FilterByPort(listeners []Listener, port int) []Listener {
	out := []Listener{}
	for _, l := range listeners {
		if l.Port == port {
			out = append(out, l)
		}
	}
	return out
}

// parseProcPID extracts process name and PID from an ss/netstat line.
func parseProcPID(line string, parts []string) (string, int) {
	if m := reSSUsers.FindStringSubmatch(line); len(m) == 3 {
		proc := m[1]
		if pid, err := strconv.Atoi(m[2]); err == nil {
			return proc, pid
		}
		return proc, 0
	}
	// netstat style often ends with "PID/PROC" or includes it in a later column
	for i := len(parts) - 1; i >= 0; i-- {
		if seg := parts[i]; strings.Contains(seg, "/") {
			pp := strings.SplitN(seg, "/", 2)
			if len(pp) == 2 {
				if pid, err := strconv.Atoi(pp[0]); err == nil {
					return pp[1], pid
				}
			}
		}
	}
	return "", 0
}

func parseAddressAndPort(line string, parts []string) (int, string) {
	port := 0
	if m := rePort.FindStringSubmatch(line); len(m) == 2 {
		if p, err := strconv.Atoi(m[1]); err == nil {
			port = p
		}
	}
	addr := ""
	if port != 0 {
		for _, tok := range parts {
			if strings.Contains(tok, ":") && strings.Contains(tok, fmt.Sprintf(":%d", port)) {
				addr = tok
				break
			}
		}
	}
	return port, addr
}
