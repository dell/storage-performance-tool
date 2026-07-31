/*
Copyright © 2025 Dell Technologies
*/

package preflight

import (
	"context"
	"fmt"
	"strings"
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

func TestPreflight_EnsureImage_DevImagePresent(t *testing.T) {
	mock := command.NewMockCommandExecutor()
	image := constants.DefaultSptImage + ":" + constants.DevImageTag
	// Image present
	mock.SetCommandSuccess(fmt.Sprintf("%s %s -q %s", constants.DockerCommand, constants.DockerCmdImages, image), "sha256:abc1234")

	pf := &DefaultChecker{exec: mock}
	if err := pf.EnsureImage(context.Background(), testHost(), image); err != nil {
		t.Fatalf("EnsureImage error: %v", err)
	}
}

func TestPreflight_EnsureImage_DevImageMissing(t *testing.T) {
	mock := command.NewMockCommandExecutor()
	image := constants.DefaultSptImage + ":" + constants.DevImageTag
	// Image missing
	mock.SetCommandSuccess(fmt.Sprintf("%s %s -q %s", constants.DockerCommand, constants.DockerCmdImages, image), "")

	pf := &DefaultChecker{exec: mock}
	err := pf.EnsureImage(context.Background(), testHost(), image)
	if err == nil {
		t.Fatal("expected error when dev image is missing locally")
	}
	if !strings.Contains(err.Error(), "dev image") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestPreflightInspectImageIdentity(t *testing.T) {
	mock := command.NewMockCommandExecutor()
	image := "repo/spt:test"
	id := "sha256:" + strings.Repeat("a", 64)
	cmd := fmt.Sprintf("%s %s %s %s", constants.DockerCommand, constants.DockerCmdImage, constants.DockerCmdInspect, image)
	mock.SetCommandSuccess(cmd, `[{"Id":"`+id+`","RepoDigests":["repo/spt@sha256:bbb","repo/spt@sha256:aaa"]}]`)

	identity, err := NewCheckerWithExecutor(mock).InspectImageIdentity(context.Background(), testHost(), image)
	if err != nil {
		t.Fatal(err)
	}
	if identity.ID != id {
		t.Fatalf("identity ID = %q, want %q", identity.ID, id)
	}
	if got := strings.Join(identity.RepoDigests, ","); got != "repo/spt@sha256:aaa,repo/spt@sha256:bbb" {
		t.Fatalf("repo digests = %q", got)
	}
}

func TestPreflightInspectImageIdentityRejectsUnverifiableID(t *testing.T) {
	mock := command.NewMockCommandExecutor()
	image := "repo/spt:test"
	cmd := fmt.Sprintf("%s %s %s %s", constants.DockerCommand, constants.DockerCmdImage, constants.DockerCmdInspect, image)
	mock.SetCommandSuccess(cmd, `[{"Id":"mutable-tag-only","RepoDigests":[]}]`)

	if _, err := NewCheckerWithExecutor(mock).InspectImageIdentity(context.Background(), testHost(), image); err == nil {
		t.Fatal("expected invalid image identity to fail")
	}
}

func TestPreflightInspectPayloadIdentity(t *testing.T) {
	mock := command.NewMockCommandExecutor()
	host := testHost()
	host.IsLocal = true
	image := "sha256:" + strings.Repeat("a", 64)
	digest := strings.Repeat("b", 64)
	cmd := strings.Join([]string{
		constants.DockerCommand, constants.DockerCmdRun, constants.DockerFlagRemove,
		constants.DockerFlagReadOnly, constants.DockerFlagNetwork, "none",
		constants.DockerFlagEntrypoint, "sh", image, "-c", integrityPayloadHashScript,
	}, " ")
	mock.SetCommandSuccess(cmd, digest+"  -")

	got, err := NewCheckerWithExecutor(mock).InspectPayloadIdentity(context.Background(), host, image)
	if err != nil {
		t.Fatal(err)
	}
	if got != digest {
		t.Fatalf("payload digest = %q, want %q", got, digest)
	}
}

func TestPreflightInspectPayloadIdentityQuotesRemoteScriptAndRejectsBadEvidence(t *testing.T) {
	mock := command.NewMockCommandExecutor()
	host := testHost()
	image := "sha256:" + strings.Repeat("a", 64)
	quotedScript := quoteRemoteShellArg(integrityPayloadHashScript)
	cmd := strings.Join([]string{
		constants.DockerCommand, constants.DockerCmdRun, constants.DockerFlagRemove,
		constants.DockerFlagReadOnly, constants.DockerFlagNetwork, "none",
		constants.DockerFlagEntrypoint, "sh", image, "-c", quotedScript,
	}, " ")
	mock.SetCommandSuccess(cmd, "not-a-digest")

	if _, err := NewCheckerWithExecutor(mock).InspectPayloadIdentity(context.Background(), host, image); err == nil {
		t.Fatal("expected invalid payload identity to fail")
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
