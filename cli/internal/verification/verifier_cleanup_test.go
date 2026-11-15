/*
Copyright © 2025 Dell Technologies
*/

package verification

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
)

// cleanupNow is the client clock in the cleanup tests; docker ps reports creation times in UTC.
var cleanupNow = time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)

func dockerCreatedAt(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05 -0700 MST")
}

func verifyPSLine(id string, created string, labels ...string) string {
	return id + "@" + created + "@" + strings.Join(labels, ",")
}

// captureLogs sends the package logger to a buffer at DEBUG until the test ends.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	prev := logging.GetLogger()
	var buf bytes.Buffer
	logging.SetLogger(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { logging.SetLogger(prev) })
	return &buf
}

// runCleanup runs cleanupVerifyContainers against a fake executor whose docker ps lists psOut, and
// returns the IDs it removed.
func runCleanup(t *testing.T, psOut string) (removed []string, v *Verifier) {
	t.Helper()
	exec := &MockCommandExecutor{Commands: map[string]MockCommandResponse{
		verifyCleanupListCommand(): {Stdout: psOut},
	}}
	for _, line := range strings.Split(psOut, "\n") {
		id, _, _ := strings.Cut(line, "@")
		exec.Commands["docker rm -f "+id] = MockCommandResponse{}
	}
	host := &hostparse.HostInfo{Host: "wrk1", Original: "wrk1"}
	timeProvider := NewMockTimeProvider(cleanupNow)
	timeProvider.TimeAdvance = 0
	v = NewVerifierWithDeps([]*hostparse.HostInfo{host}, Config{}, exec, &MockNetworkChecker{}, timeProvider)
	v.runID = "this-run"

	v.cleanupVerifyContainers(host, command.NewDockerOperations(exec, host))

	for _, c := range exec.ExecutedCommands {
		if len(c.Command) == 4 && c.Command[1] == constants.DockerCmdRM {
			removed = append(removed, c.Command[3])
		}
	}
	return removed, v
}

func assertRemoved(t *testing.T, got []string, want ...string) {
	t.Helper()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("removed %v, want %v", got, want)
	}
}

func runLabel(id string) string { return constants.DockerLabelVerifyRun + "=" + id }

func TestCleanupVerifyContainers_LeavesFreshContainerOfAnotherRun(t *testing.T) {
	logs := captureLogs(t)
	created := dockerCreatedAt(cleanupNow.Add(-20 * time.Second))
	removed, _ := runCleanup(t, verifyPSLine("fresh1", created, "spt.verify=true", runLabel("other-run")))

	assertRemoved(t, removed)
	if !strings.Contains(logs.String(), "level=DEBUG") || !strings.Contains(logs.String(), "container=fresh1") {
		t.Fatalf("want one DEBUG line naming fresh1, got:\n%s", logs.String())
	}
}

func TestCleanupVerifyContainers_RemovesStaleContainerOfAnotherRun(t *testing.T) {
	logs := captureLogs(t)
	created := dockerCreatedAt(cleanupNow.Add(-verifyContainerStaleAge - time.Minute))
	removed, _ := runCleanup(t, verifyPSLine("stale1", created, runLabel("other-run"), "spt.verify=true"))

	assertRemoved(t, removed, "stale1")
	out := logs.String()
	if !strings.Contains(out, "level=INFO") || !strings.Contains(out, "removed lingering verification container") || !strings.Contains(out, "age=16m0s") {
		t.Fatalf("want the INFO removal line with the age, got:\n%s", out)
	}
}

func TestCleanupVerifyContainers_LeavesOwnRunAtAnyAge(t *testing.T) {
	created := dockerCreatedAt(cleanupNow.Add(-2 * verifyContainerStaleAge))
	removed, _ := runCleanup(t, verifyPSLine("own1", created, runLabel("this-run")))

	assertRemoved(t, removed)
}

func TestCleanupVerifyContainers_UnlabelledJudgedByAge(t *testing.T) {
	oldCreated := dockerCreatedAt(cleanupNow.Add(-time.Hour))
	freshCreated := dockerCreatedAt(cleanupNow.Add(-time.Minute))
	removed, _ := runCleanup(t, strings.Join([]string{
		verifyPSLine("oldunlabelled", oldCreated, "spt.verify=true", "spt.verify.host=wrk1"),
		verifyPSLine("freshunlabelled", freshCreated, "spt.verify=true", "spt.verify.host=wrk1"),
	}, "\n"))

	assertRemoved(t, removed, "oldunlabelled")
}

func TestCleanupVerifyContainers_UnlabelledOldRemovedByVerifierWithoutRunID(t *testing.T) {
	exec := &MockCommandExecutor{Commands: map[string]MockCommandResponse{
		verifyCleanupListCommand(): {Stdout: verifyPSLine("old1", dockerCreatedAt(cleanupNow.Add(-time.Hour)), "spt.verify=true")},
		"docker rm -f old1":        {},
	}}
	host := &hostparse.HostInfo{Host: "wrk1"}
	v := &Verifier{cmdExecutor: exec, timeProvider: NewMockTimeProvider(cleanupNow)}

	v.cleanupVerifyContainers(host, command.NewDockerOperations(exec, host))

	if last := strings.Join(exec.ExecutedCommands[len(exec.ExecutedCommands)-1].Command, " "); last != "docker rm -f old1" {
		t.Fatalf("an unlabelled stale container must not match an empty run id; last command %q", last)
	}
}

func TestCleanupVerifyContainers_LeavesUnparseableTimeAlone(t *testing.T) {
	logs := captureLogs(t)
	removed, _ := runCleanup(t, strings.Join([]string{
		verifyPSLine("badtime", "yesterday-ish", runLabel("other-run")),
		"garbage-line-without-fields",
	}, "\n"))

	assertRemoved(t, removed)
	out := logs.String()
	if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "container=badtime") {
		t.Fatalf("want a WARN line naming badtime, got:\n%s", out)
	}
}

func TestCleanupVerifyContainers_ReadsZoneOffset(t *testing.T) {
	// A Docker host in UTC-5 reports local time. 06:58 -0500 is 11:58 UTC, two minutes old for a
	// client clock of 12:00 UTC, so it is fresh; read without the offset it would look five hours old.
	removed, _ := runCleanup(t, verifyPSLine("est1", "2026-11-14 06:58:00 -0500 EST", runLabel("other-run")))

	assertRemoved(t, removed)
}

func TestStartNodeContainer_CarriesRunLabel(t *testing.T) {
	exec := &FlexibleMockCommandExecutor{}
	host := &hostparse.HostInfo{Host: "wrk1", Original: "wrk1"}
	v := NewVerifierWithDeps([]*hostparse.HostInfo{host}, Config{NetworkMode: "host"}, exec, &MockNetworkChecker{}, NewMockTimeProvider(cleanupNow))
	if v.runID == "" {
		t.Fatal("NewVerifierWithDeps left the run id empty")
	}
	other := NewVerifierWithDeps([]*hostparse.HostInfo{host}, Config{}, exec, &MockNetworkChecker{}, NewMockTimeProvider(cleanupNow))
	if other.runID == v.runID {
		t.Fatalf("two invocations share run id %q", v.runID)
	}

	if check := v.startNodeContainer(host, &NodeResult{}); !check.Passed {
		t.Fatalf("startNodeContainer failed: %+v", check)
	}

	var run []string
	for _, c := range exec.ExecutedCommands {
		if len(c.Command) > 1 && c.Command[1] == constants.DockerCmdRun {
			run = c.Command
		}
	}
	joined := strings.Join(run, " ")
	for _, want := range []string{
		"--label " + runLabel(v.runID),
		"--label spt.verify=true",
		"--label spt.verify.host=wrk1",
		"--label spt.managed=true",
		"--label spt.role=verify",
		"--label spt.host=wrk1",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("docker run lacks %q: %s", want, joined)
		}
	}
}
