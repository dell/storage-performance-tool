package poolsizing

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/nicplan"
)

// imageHosts answers each probe as a host's Docker does: --pull never on a missing image fails with
// "No such image" and exit 125; without it, a missing image is pulled once and the command answers.
// Any other command, such as the host's own MemTotal, gets the reply of a host with 1 TiB.
type imageHosts struct {
	command.CommandExecutor
	mu      sync.Mutex
	present map[string]bool
	pulls   map[string]int
	answer  func(cmd []string) string
}

func (h *imageHosts) ExecuteCommand(_ context.Context, host *hostparse.HostInfo, cmd []string) (string, string, error) {
	if len(cmd) < 2 || cmd[1] != "run" {
		return "MemTotal:       1073741824 kB", "", nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.present[host.Host] {
		if strings.Contains(strings.Join(cmd, " "), "--pull never") {
			return "", "docker: Error response from daemon: No such image: " + probedImage(cmd) + "\n",
				errors.New("exit status 125")
		}
		h.pulls[host.Host]++
		h.present[host.Host] = true
	}
	return h.answer(cmd), "", nil
}

// probedImage is the image a probe command names: the first argument after the entrypoint and an
// optional --pull.
func probedImage(cmd []string) string {
	i := slices.Index(cmd, "--entrypoint") + 2
	if i < len(cmd) && cmd[i] == "--pull" {
		i += 2
	}
	if i < len(cmd) {
		return cmd[i]
	}
	return ""
}

func newImageHosts(t *testing.T, present map[string]bool) *imageHosts {
	t.Helper()
	hosts := &imageHosts{present: present, pulls: map[string]int{}}
	hosts.answer = func(cmd []string) string {
		if slices.Contains(cmd, "--capacity") {
			return nicOutput(t, nicplan.Output{Capacity: capacityOf(1024*gib, "MemTotal")})
		}
		return nicOutput(t, nicplan.Output{
			Plan:       &nicplan.Plan{OK: true, NicSet: []string{"ens1"}},
			PoolDemand: &nicplan.PoolDemand{Steps: []nicplan.PoolStep{step("create", 192*gib)}},
		})
	}
	return hosts
}

// A run under the skip gives the probe a non-dev image without NeverPull: a host lacking the image
// pulls it during the probe and is sized, and a host that has it pulls nothing. With NeverPull, as
// for a dev image, the host lacking it keeps the default limit and says why.
func TestAHostMissingTheImageIsSizedUnlessTheProbeNeverPulls(t *testing.T) {
	for _, tc := range []struct {
		name        string
		neverPull   bool
		missingMode string
		missingPull int
	}{
		{"pullable probe", false, ModeSized, 1},
		{"never pull", true, ModeUnprobed, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hosts := newImageHosts(t, map[string]bool{"wrk1": true, "wrk2": false})
			params := rdmaParams("write")
			params.Endpoints = []string{"http://192.0.2.1:9000"}
			image := "ghcr.io/dell/storage-performance-tool:v9.9.9"
			plan, err := BuildPlan(context.Background(), Input{
				Params: params, Hosts: []*hostparse.HostInfo{{Host: "wrk1"}, {Host: "wrk2"}},
				Image: image, NeverPull: tc.neverPull, Executor: hosts,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.Engines) != 2 {
				t.Fatalf("%+v", plan.Engines)
			}
			present, missing := plan.Engines[0], plan.Engines[1]
			if present.Mode != ModeSized || present.Value() != "192g" {
				t.Fatalf("the host with the image: %s %q %q", present.Mode, present.Value(), present.Message)
			}
			if missing.Mode != tc.missingMode {
				t.Fatalf("the host lacking the image: %s, want %s (%q)", missing.Mode, tc.missingMode, missing.Message)
			}
			if tc.missingMode == ModeSized && missing.Value() != "192g" {
				t.Fatalf("the host lacking the image: %q", missing.Value())
			}
			if tc.missingMode == ModeUnprobed && (!missing.Warning ||
				!strings.Contains(missing.Message, "could not size wrk2") || !strings.Contains(missing.Message, "No such image: "+image)) {
				t.Fatalf("the unsized host says why: %v %q", missing.Warning, missing.Message)
			}
			if hosts.pulls["wrk1"] != 0 || hosts.pulls["wrk2"] != tc.missingPull {
				t.Fatalf("pulls = %v, want none on wrk1 and %d on wrk2", hosts.pulls, tc.missingPull)
			}
		})
	}
}
