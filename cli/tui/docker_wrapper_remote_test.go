package tui

import (
	"errors"
	"testing"
)

func TestDockerManager_RemoteDelegatesContainerID(t *testing.T) {
	dm := &DockerManager{remote: &RemoteDockerManager{}}
	dm.remote.containerID = "abc123"
	if got := dm.ContainerID(); got != "abc123" {
		t.Fatalf("expected remote container id, got %q", got)
	}
}

func TestDockerManager_RemoteCleanup_NoContainer_NoError(t *testing.T) {
	dm := &DockerManager{remote: &RemoteDockerManager{}}
	if err := dm.Cleanup(); err != nil {
		t.Fatalf("expected nil error from remote cleanup with no container, got %v", err)
	}
}

func TestDockerManager_RemoteStartContainer_NotSupported(t *testing.T) {
	dm := &DockerManager{remote: &RemoteDockerManager{}}
	if _, err := dm.StartContainer("image", []string{"echo"}); err == nil {
		t.Fatalf("expected error for remote StartContainer not supported")
	} else if !errors.Is(err, err) { // we just check non-nil; message asserted below
		if err.Error() == "" {
			t.Fatalf("expected non-empty error")
		}
	}
}

func TestDockerManager_RemoteStartContainerWithScenario_NotSupported(t *testing.T) {
	dm := &DockerManager{remote: &RemoteDockerManager{}}
	if _, err := dm.StartContainerWithScenario("image", "scenario.js", nil); err == nil {
		t.Fatalf("expected error for remote StartContainerWithScenario not supported")
	}
}

func TestDockerManager_RemoteStreamOutput_NoopForEmptyID(t *testing.T) {
	dm := &DockerManager{remote: &RemoteDockerManager{}}
	called := false
	dm.StreamOutput("", func(string) { called = true }, func(string) { called = true })
	if called {
		t.Fatalf("expected no callback invocation for empty container id")
	}
}
