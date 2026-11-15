/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"context"
	"errors"
	"strings"
	"testing"

	dockerconf "github.com/dell/storage-performance-tool/cli/internal/docker"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

// fixedPortPreflight returns one port-check result for every host.
type fixedPortPreflight struct {
	identityPreflight
	info *dockerconf.ConflictInfo
	err  error
}

func (p fixedPortPreflight) CheckPorts(context.Context, *hostparse.HostInfo, int, int) (*dockerconf.ConflictInfo, error) {
	return p.info, p.err
}

func TestRecheckPortsAfterCleanup(t *testing.T) {
	host := &hostparse.HostInfo{Original: "worker-1"}
	sshErr := errors.New("ssh: connection refused")
	tests := []struct {
		name        string
		checker     fixedPortPreflight
		wantErr     string
		wantSummary string
		wantWrapped error
	}{
		{
			name:        "checker error",
			checker:     fixedPortPreflight{err: sshErr},
			wantErr:     "port re-check failed after cleanup: ssh: connection refused",
			wantSummary: "  ❌ worker-1: Port re-check failed after cleanup: ssh: connection refused",
			wantWrapped: sshErr,
		},
		{
			name:        "no result and no error",
			checker:     fixedPortPreflight{},
			wantErr:     "port re-check failed after cleanup: port checker returned no result",
			wantSummary: "  ❌ worker-1: Port re-check failed after cleanup: port checker returned no result",
			wantWrapped: errNoPortCheckResult,
		},
		{
			name:        "ports still in use",
			checker:     fixedPortPreflight{info: &dockerconf.ConflictInfo{ConflictPorts: []int{1099, 9999}}},
			wantErr:     "ports still in use after cleanup: [1099 9999]",
			wantSummary: "  ❌ worker-1: Ports still in use: [1099 9999]",
		},
		{
			name:    "ports free",
			checker: fixedPortPreflight{info: &dockerconf.ConflictInfo{AvailablePorts: []int{1099, 9999}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			summary, err := recheckPortsAfterCleanup(context.Background(), tt.checker, host, 1099, 9999)
			if tt.wantErr == "" {
				if err != nil || summary != "" {
					t.Fatalf("got (%q, %v), want no error", summary, err)
				}
				return
			}
			if err == nil || err.Error() != tt.wantErr {
				t.Fatalf("error = %v, want %q", err, tt.wantErr)
			}
			if summary != tt.wantSummary {
				t.Fatalf("summary = %q, want %q", summary, tt.wantSummary)
			}
			if tt.wantWrapped != nil && !errors.Is(err, tt.wantWrapped) {
				t.Fatalf("error %v does not wrap %v", err, tt.wantWrapped)
			}
		})
	}
}

func TestPortCheckResult(t *testing.T) {
	info := &dockerconf.ConflictInfo{}
	checkErr := errors.New("boom")
	if got, err := portCheckResult(nil, nil); got != nil || !errors.Is(err, errNoPortCheckResult) {
		t.Fatalf("nil, nil: got (%v, %v), want errNoPortCheckResult", got, err)
	}
	if got, err := portCheckResult(nil, checkErr); got != nil || !errors.Is(err, checkErr) {
		t.Fatalf("nil, err: got (%v, %v), want the checker error", got, err)
	}
	if got, err := portCheckResult(info, nil); got != info || err != nil {
		t.Fatalf("info, nil: got (%v, %v), want the result", got, err)
	}
}

func TestConnectHostsReportsAPortCheckWithNoResult(t *testing.T) {
	o := NewMultiHostOrchestrator([]*hostparse.HostInfo{{Host: "entry.example", Original: "entry.example"}}, 1)
	o.preflight = fixedPortPreflight{}
	o.SetNotifier(func(string) {})
	var cancels []context.CancelFunc
	o.newDockerManagerForHost = func(ctx context.Context, host *hostparse.HostInfo) (*DockerManager, error) {
		managerCtx, cancel := context.WithCancel(ctx)
		cancels = append(cancels, cancel)
		return &DockerManager{ctx: managerCtx, cancel: cancel, hostInfo: host}, nil
	}
	defer func() {
		for _, cancel := range cancels {
			cancel()
		}
	}()

	err := o.ConnectHosts(context.Background())
	if err == nil || !strings.Contains(err.Error(), "port check failed: port checker returned no result") {
		t.Fatalf("ConnectHosts() error = %v, want the failed port check", err)
	}
}
