/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	dockerconf "github.com/dell/storage-performance-tool/cli/internal/docker"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

// recordingImagePreflight records the Docker, image and port checks ConnectHosts runs per host.
// EnsureImage fails for the hosts in missing the way the real checker does when the image is
// absent there: a dev tag gets the build-and-distribute advice, any other tag a failed pull.
type recordingImagePreflight struct {
	identityPreflight
	missing map[string]bool

	mu     sync.Mutex
	docker []string
	images []string
	ports  []string
}

func (p *recordingImagePreflight) CheckDocker(_ context.Context, host *hostparse.HostInfo) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.docker = append(p.docker, host.Original)
	return "test", nil
}

func (p *recordingImagePreflight) EnsureImage(_ context.Context, host *hostparse.HostInfo, image string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.images = append(p.images, host.Original+" "+image)
	if !p.missing[host.Original] {
		return nil
	}
	if constants.IsDevImage(image) {
		return fmt.Errorf("dev image %s not present on %s; build it with `make docker-local` and distribute it with engine/tools/push-worker-image.sh", image, host.Original)
	}
	return fmt.Errorf("failed to pull image %s: manifest unknown", image)
}

func (p *recordingImagePreflight) CheckPorts(
	_ context.Context, host *hostparse.HostInfo, _, _ int,
) (*dockerconf.ConflictInfo, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ports = append(p.ports, host.Original)
	return &dockerconf.ConflictInfo{}, nil
}

func (p *recordingImagePreflight) calls() (docker, images, ports []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	sorted := func(s []string) []string {
		out := append([]string(nil), s...)
		sort.Strings(out)
		return out
	}
	return sorted(p.docker), sorted(p.images), sorted(p.ports)
}

// TestConnectHostsImagePreflightRoute proves which hosts ConnectHosts checks the engine image on:
// only the entry of an attach run, where spt starts nothing on the attached workers, and every
// host of a run that starts its own workers. The Docker and port checks run on every host.
func TestConnectHostsImagePreflightRoute(t *testing.T) {
	devImage := constants.DefaultSptImage + ":" + constants.DevImageTag
	releaseImage := constants.DefaultSptImage + ":v5.17.0"
	allHosts := []string{"entry", "worker-1", "worker-2"}
	tests := []struct {
		name       string
		attach     bool
		image      string
		missing    map[string]bool
		wantImages []string
		wantErr    string
	}{
		{
			name:       "attach checks the image on the entry only",
			attach:     true,
			image:      releaseImage,
			wantImages: []string{"entry " + releaseImage},
		},
		{
			name:       "managed workers check the image on every host",
			image:      releaseImage,
			wantImages: []string{"entry " + releaseImage, "worker-1 " + releaseImage, "worker-2 " + releaseImage},
		},
		{
			name:       "attach connects a worker whose image pull would fail",
			attach:     true,
			image:      releaseImage,
			missing:    map[string]bool{"worker-1": true},
			wantImages: []string{"entry " + releaseImage},
		},
		{
			name:       "attach connects a worker that lacks the dev image",
			attach:     true,
			image:      devImage,
			missing:    map[string]bool{"worker-2": true},
			wantImages: []string{"entry " + devImage},
		},
		{
			name:       "managed workers still refuse a worker that lacks the image",
			image:      releaseImage,
			missing:    map[string]bool{"worker-1": true},
			wantImages: []string{"entry " + releaseImage, "worker-1 " + releaseImage, "worker-2 " + releaseImage},
			wantErr:    "insufficient hosts connected: 2 connected, 3 required",
		},
		{
			name:       "attach still refuses an entry that lacks the image",
			attach:     true,
			image:      releaseImage,
			missing:    map[string]bool{"entry": true},
			wantImages: []string{"entry " + releaseImage},
			wantErr:    "designated entry host entry is unavailable: image preflight failed",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hostInfos := make([]*hostparse.HostInfo, 0, len(allHosts))
			for _, name := range allHosts {
				hostInfos = append(hostInfos, &hostparse.HostInfo{Host: name, Original: name})
			}
			orchestrator := NewMultiHostOrchestrator(hostInfos, len(hostInfos))
			checker := &recordingImagePreflight{missing: test.missing}
			orchestrator.preflight = checker
			orchestrator.SetNotifier(func(string) {})
			orchestrator.SetAttachExistingWorkers(test.attach)
			orchestrator.SetImage(test.image)
			orchestrator.newDockerManagerForHost = func(ctx context.Context, host *hostparse.HostInfo) (*DockerManager, error) {
				managerCtx, cancel := context.WithCancel(ctx)
				return &DockerManager{ctx: managerCtx, cancel: cancel, hostInfo: host}, nil
			}
			t.Cleanup(func() {
				for _, host := range orchestrator.hosts {
					if host.DockerManager != nil {
						host.DockerManager.Close()
					}
				}
			})

			err := orchestrator.ConnectHosts(context.Background())
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("ConnectHosts() error = %v, want every host connected", err)
				}
				if got := len(orchestrator.GetReadyHosts()); got != len(allHosts) {
					t.Fatalf("ready hosts = %d, want %d", got, len(allHosts))
				}
			} else if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("ConnectHosts() error = %v, want it to contain %q", err, test.wantErr)
			}

			docker, images, ports := checker.calls()
			if !slices.Equal(docker, allHosts) {
				t.Errorf("CheckDocker hosts = %v, want %v", docker, allHosts)
			}
			if !slices.Equal(images, test.wantImages) {
				t.Errorf("EnsureImage calls = %v, want %v", images, test.wantImages)
			}
			// The port check runs after the image check, so a host refused there skips it.
			wantPorts := allHosts
			if test.wantErr != "" {
				wantPorts = nil
				for _, name := range allHosts {
					if !test.missing[name] {
						wantPorts = append(wantPorts, name)
					}
				}
			}
			if !slices.Equal(ports, wantPorts) {
				t.Errorf("CheckPorts hosts = %v, want %v", ports, wantPorts)
			}
		})
	}
}
