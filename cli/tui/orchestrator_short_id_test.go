/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

// TestMultiHostOrchestrator_StartContainers_ShortContainerID starts a container whose ID is
// shorter than the display length: it prints whole instead of panicking, and a full 64-character
// ID still prints its first 12 characters.
func TestMultiHostOrchestrator_StartContainers_ShortContainerID(t *testing.T) {
	tests := []struct {
		name   string
		id     string
		wantID string
	}{
		{"short", "abc12", "abc12"},
		{"full", "4f2c9a1b7e3d0c8a5b6f1e2d3c4b5a69788796a5b4c3d2e1f0a9b8c7d6e5f4a3", "4f2c9a1b7e3d"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orchestrator := NewMultiHostOrchestrator([]*hostparse.HostInfo{
				{Host: "host1", IsLocal: false, Original: "host1"},
			}, 1)
			var mu sync.Mutex
			var messages []string
			orchestrator.SetNotifier(func(msg string) {
				mu.Lock()
				defer mu.Unlock()
				messages = append(messages, msg)
			})
			mockDocker := NewMockDockerManager()
			mockDocker.SetContainerID(tt.id)
			orchestrator.hosts[0].DockerManager = mockDocker
			orchestrator.hosts[0].SetStatus(HostStatusReady)

			if err := orchestrator.StartContainers(context.Background(), "test-image", nil); err != nil {
				t.Fatalf("StartContainers() error = %v", err)
			}
			if got := orchestrator.hosts[0].ContainerID; got != tt.id {
				t.Fatalf("stored container ID = %q, want %q", got, tt.id)
			}
			mu.Lock()
			defer mu.Unlock()
			want := "  ✅ Container started on host1 (ID: " + tt.wantID + ")"
			for _, msg := range messages {
				if msg == want {
					return
				}
			}
			t.Fatalf("no %q in notifications:\n%s", want, strings.Join(messages, "\n"))
		})
	}
}
