/*
Copyright © 2025 Dell Technologies
*/

package preflight

import (
	"context"
	"fmt"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

func testHost() *hostparse.HostInfo {
	return &hostparse.HostInfo{User: "root", Host: "remote.test", Original: "root@remote.test", IsLocal: false}
}

func TestPreflight_CheckDocker(t *testing.T) {
	mock := command.NewMockCommandExecutor()
	// Docker version success
	mock.SetCommandSuccess(fmt.Sprintf("%s %s %s %s", constants.DockerCommand, constants.DockerCmdVersion, constants.DockerFlagFormat, constants.DockerVersionFormat), "28.3.3")

	pf := &DefaultChecker{exec: mock}
	ver, err := pf.CheckDocker(context.Background(), testHost())
	if err != nil {
		t.Fatalf("CheckDocker error: %v", err)
	}
	if ver == "" {
		t.Fatal("expected non-empty docker version")
	}
}

func TestPreflight_EnsureImage_PullsWhenMissing(t *testing.T) {
	mock := command.NewMockCommandExecutor()
	image := constants.DefaultSptImage
	// Image missing
	mock.SetCommandSuccess(fmt.Sprintf("%s %s -q %s", constants.DockerCommand, constants.DockerCmdImages, image), "")
	// Pull succeeds
	mock.SetCommandSuccess(fmt.Sprintf("%s %s %s", constants.DockerCommand, constants.DockerCmdPull, image), "pulled")

	pf := &DefaultChecker{exec: mock}
	if err := pf.EnsureImage(context.Background(), testHost(), image); err != nil {
		t.Fatalf("EnsureImage error: %v", err)
	}
}

func TestPreflight_CheckPorts_NoConflicts(t *testing.T) {
	mock := command.NewMockCommandExecutor()
	// Empty listeners snapshot
	mock.SetCommandSuccess("sh -c "+constants.PortSnapshotCommand, "")
	// docker ps call can be empty
	mock.SetCommandSuccess(fmt.Sprintf("%s %s %s %s", constants.DockerCommand, constants.DockerCmdPS, constants.DockerFlagAll, constants.DockerFlagFormat+" "+constants.DockerConflictFormat), "")

	pf := &DefaultChecker{exec: mock}
	info, err := pf.CheckPorts(context.Background(), testHost(), constants.DefaultSptAPIPort, constants.DefaultRMIPortStart, constants.DefaultRMIPortCount)
	if err != nil {
		t.Fatalf("CheckPorts error: %v", err)
	}
	if info == nil || len(info.ConflictPorts) != 0 {
		t.Fatalf("expected no conflicts, got: %+v", info)
	}
}

func TestPreflight_CheckPorts_WithConflictAndContainer(t *testing.T) {
	mock := command.NewMockCommandExecutor()
	// Snapshot shows 9999 in use
	snapshotLine := "tcp LISTEN 0 128 0.0.0.0:9999 0.0.0.0:* users:(\"docker-proxy\",pid=2222,fd=2)"
	mock.SetCommandSuccess("sh -c "+constants.PortSnapshotCommand, snapshotLine)
	// docker ps shows a container mapping 9999
	dockerOut := "abc\tspt-verify\t" + constants.DefaultSptImage + "\trunning\t0.0.0.0:9999->9999/tcp\t2024-01-01 12:00:00 +0000 UTC"
	mock.SetCommandSuccess(fmt.Sprintf("%s %s %s %s", constants.DockerCommand, constants.DockerCmdPS, constants.DockerFlagAll, constants.DockerFlagFormat+" "+constants.DockerConflictFormat), dockerOut)

	pf := &DefaultChecker{exec: mock}
	info, err := pf.CheckPorts(context.Background(), testHost(), constants.DefaultSptAPIPort, constants.DefaultRMIPortStart, constants.DefaultRMIPortCount)
	if err != nil {
		t.Fatalf("CheckPorts error: %v", err)
	}
	if info == nil || len(info.ConflictPorts) == 0 {
		t.Fatalf("expected conflicts, got: %+v", info)
	}
	if len(info.Containers) == 0 {
		t.Fatalf("expected containers mapped to conflict ports")
	}
	if !info.IsSptHost {
		t.Fatalf("expected IsSptHost=true when spt image present")
	}
}
