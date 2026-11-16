/*
Copyright © 2025 Dell Technologies
*/

package verification

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"text/template"
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

// verifyCleanupOwnListCommand is the docker ps command line cleanupVerifyContainers runs to list
// runID's own containers, as the mock executor keys it.
func verifyCleanupOwnListCommand(runID string) string {
	return strings.Join(verifyCleanupOwnListArgs(runID), " ")
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

// fakeContainer is one container on a fakeDockerHost.
type fakeContainer struct {
	id      string
	created time.Time
	labels  map[string]string
}

// fakeDockerHost answers docker ps and docker rm -f as a Docker daemon would for its containers:
// docker ps applies every --filter label=k=v (all must match), prints short IDs for -q and
// otherwise renders --format with text/template, where {{.Labels}} is the raw comma-joined
// "k=v,k=v" docker prints; docker rm -f resolves an ID prefix, as docker does.
type fakeDockerHost struct {
	containers []fakeContainer
	removed    []string
	commands   [][]string
}

func (f *fakeDockerHost) ExecuteCommand(_ context.Context, _ *hostparse.HostInfo, cmd []string) (string, string, error) {
	f.commands = append(f.commands, cmd)
	if len(cmd) == 4 && cmd[0] == constants.DockerCommand && cmd[1] == constants.DockerCmdRM && cmd[2] == constants.DockerFlagForce {
		for i, c := range f.containers {
			if strings.HasPrefix(c.id, cmd[3]) {
				f.removed = append(f.removed, c.id)
				f.containers = append(f.containers[:i], f.containers[i+1:]...)
				return cmd[3], "", nil
			}
		}
		return "", "Error: No such container: " + cmd[3], fmt.Errorf("exit status 1")
	}
	if len(cmd) < 2 || cmd[0] != constants.DockerCommand || cmd[1] != constants.DockerCmdPS {
		return "", "", fmt.Errorf("fake docker: unexpected command %q", cmd)
	}
	var filters [][2]string
	quiet, format := false, "{{.ID}}"
	for i := 2; i < len(cmd); i++ {
		switch cmd[i] {
		case constants.DockerFlagQuiet:
			quiet = true
		case constants.DockerFlagFilter:
			i++
			kv, ok := strings.CutPrefix(cmd[i], "label=")
			if !ok {
				return "", "", fmt.Errorf("fake docker: unsupported filter %q", cmd[i])
			}
			k, val, _ := strings.Cut(kv, "=")
			filters = append(filters, [2]string{k, val})
		case constants.DockerFlagFormat:
			i++
			format = cmd[i]
		default:
			return "", "", fmt.Errorf("fake docker: unsupported ps argument %q", cmd[i])
		}
	}
	tmpl, err := template.New("ps").Parse(format)
	if err != nil {
		return "", err.Error(), err
	}
	var out strings.Builder
	for _, c := range f.containers {
		match := true
		for _, kv := range filters {
			if got, ok := c.labels[kv[0]]; !ok || got != kv[1] {
				match = false
			}
		}
		if !match {
			continue
		}
		if quiet {
			out.WriteString(c.id[:12] + "\n")
			continue
		}
		keys := make([]string, 0, len(c.labels))
		for k := range c.labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		pairs := make([]string, 0, len(keys))
		for _, k := range keys {
			pairs = append(pairs, k+"="+c.labels[k])
		}
		row := struct{ ID, CreatedAt, Labels string }{c.id[:12], dockerCreatedAt(c.created), strings.Join(pairs, ",")}
		if err := tmpl.Execute(&out, row); err != nil {
			return "", err.Error(), err
		}
		out.WriteString("\n")
	}
	return out.String(), "", nil
}

func (f *fakeDockerHost) CopyFile(context.Context, *hostparse.HostInfo, string, string) error {
	return nil
}

func (f *fakeDockerHost) CopyFromHost(context.Context, *hostparse.HostInfo, string, string) error {
	return nil
}

// verifyLabels returns the labels of a verify container of run (none when run is "").
func verifyLabels(run string, extra ...string) map[string]string {
	labels := map[string]string{constants.DockerLabelVerify: stringTrue, constants.DockerLabelVerifyHost: "wrk1"}
	if run != "" {
		labels[constants.DockerLabelVerifyRun] = run
	}
	for _, kv := range extra {
		k, val, _ := strings.Cut(kv, "=")
		labels[k] = val
	}
	return labels
}

// runCleanupOn runs cleanupVerifyContainers with run id "this-run" against a fake Docker host
// holding containers, and returns the IDs it removed.
func runCleanupOn(t *testing.T, containers ...fakeContainer) (removed []string, host *fakeDockerHost) {
	t.Helper()
	host = &fakeDockerHost{containers: containers}
	h := &hostparse.HostInfo{Host: "wrk1", Original: "wrk1"}
	timeProvider := NewMockTimeProvider(cleanupNow)
	timeProvider.TimeAdvance = 0
	v := NewVerifierWithDeps([]*hostparse.HostInfo{h}, Config{}, host, &MockNetworkChecker{}, timeProvider)
	v.runID = "this-run"

	v.cleanupVerifyContainers(h, command.NewDockerOperations(host, h))
	return host.removed, host
}

// runCleanup runs cleanupVerifyContainers against a fake executor whose verify listing prints psOut
// and whose own-run listing prints ownOut, and returns the IDs it removed.
func runCleanup(t *testing.T, psOut, ownOut string) []string {
	t.Helper()
	exec := &MockCommandExecutor{Commands: map[string]MockCommandResponse{
		verifyCleanupListCommand():              {Stdout: psOut},
		verifyCleanupOwnListCommand("this-run"): {Stdout: ownOut},
	}}
	for _, line := range strings.Split(psOut, "\n") {
		id, _, _ := strings.Cut(line, "@")
		exec.Commands["docker rm -f "+id] = MockCommandResponse{}
	}
	host := &hostparse.HostInfo{Host: "wrk1", Original: "wrk1"}
	timeProvider := NewMockTimeProvider(cleanupNow)
	timeProvider.TimeAdvance = 0
	v := NewVerifierWithDeps([]*hostparse.HostInfo{host}, Config{}, exec, &MockNetworkChecker{}, timeProvider)
	v.runID = "this-run"

	v.cleanupVerifyContainers(host, command.NewDockerOperations(exec, host))

	var removed []string
	for _, c := range exec.ExecutedCommands {
		if len(c.Command) == 4 && c.Command[1] == constants.DockerCmdRM {
			removed = append(removed, c.Command[3])
		}
	}
	return removed
}

func assertRemoved(t *testing.T, got []string, want ...string) {
	t.Helper()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("removed %v, want %v", got, want)
	}
}

func runLabel(id string) string { return constants.DockerLabelVerifyRun + "=" + id }

func TestCleanupVerifyContainers_ListingWordsAreShellSafe(t *testing.T) {
	// The remote executor appends each word to the ssh arguments unquoted, so the host's shell
	// splits and interprets them. A word with whitespace or a metacharacter would change the command.
	const unsafe = " \t\n\r()\"'|&;<>$`\\"
	for _, cmd := range [][]string{verifyCleanupListArgs(), verifyCleanupOwnListArgs(newVerifyRunID())} {
		for _, word := range cmd {
			if word == "" || strings.ContainsAny(word, unsafe) {
				t.Errorf("cleanup listing %q has a word unsafe for a remote shell: %q", cmd, word)
			}
		}
	}
	for _, cmd := range [][]string{verifyCleanupListArgs(), verifyCleanupOwnListArgs(newVerifyRunID())} {
		if strings.Contains(strings.Join(cmd, " "), "Label") {
			t.Errorf("cleanup listing %q prints label text", cmd)
		}
	}
}

func TestCleanupVerifyContainers_ForgedLabelRowRemovesNothing(t *testing.T) {
	// A fresh verify container whose label value holds a newline and a row-shaped text naming an
	// old container of another run. With the labels printed, that text read as a row and its ID
	// went to docker rm -f, which resolves a prefix to any container on the host.
	old := dockerCreatedAt(cleanupNow.Add(-time.Hour))
	forged := "x\nbbbbbbbbbbbb@" + old + "@spt.verify.run=other"
	removed, host := runCleanupOn(t,
		fakeContainer{id: "f0f0f0f0f0f0", created: cleanupNow.Add(-time.Minute), labels: verifyLabels("other-run", "note="+forged)},
		fakeContainer{id: "bbbbbbbbbbbb0000", created: cleanupNow.Add(-2 * time.Hour), labels: map[string]string{"app": "victim"}},
		fakeContainer{id: "bbbbbbbbbbbbffff", created: cleanupNow.Add(-2 * time.Hour), labels: map[string]string{}},
	)

	assertRemoved(t, removed)
	if len(host.containers) != 3 {
		t.Fatalf("want all three containers left, got %d", len(host.containers))
	}
}

func TestCleanupVerifyContainers_ForgedLabelOnStaleContainerRemovesOnlyIt(t *testing.T) {
	// A stale verify container of another run with a forged multi-line label and an embedded
	// ",spt.verify.run=this-run": it is removed for its own age, and nothing else is touched.
	old := dockerCreatedAt(cleanupNow.Add(-time.Hour))
	forged := "x,spt.verify.run=this-run\ncccccccccccc@" + old + "@spt.verify.run=other"
	removed, _ := runCleanupOn(t,
		fakeContainer{id: "5a1e5a1e5a1e", created: cleanupNow.Add(-time.Hour), labels: verifyLabels("other-run", "note="+forged)},
		fakeContainer{id: "cccccccccccc", created: cleanupNow.Add(-2 * time.Hour), labels: map[string]string{"app": "victim"}},
	)

	assertRemoved(t, removed, "5a1e5a1e5a1e")
}

func TestCleanupVerifyContainers_LeavesFreshContainerOfAnotherRun(t *testing.T) {
	logs := captureLogs(t)
	removed, host := runCleanupOn(t,
		fakeContainer{id: "0f0f0f0f0f01", created: cleanupNow.Add(-20 * time.Second), labels: verifyLabels("other-run")})

	assertRemoved(t, removed)
	if !strings.Contains(logs.String(), "level=DEBUG") || !strings.Contains(logs.String(), "container=0f0f0f0f0f01") {
		t.Fatalf("want one DEBUG line naming 0f0f0f0f0f01, got:\n%s", logs.String())
	}
	if len(host.commands) != 1 {
		t.Fatalf("with no stale container, want only the verify listing, ran %q", host.commands)
	}
}

func TestCleanupVerifyContainers_RemovesStaleContainerOfAnotherRun(t *testing.T) {
	logs := captureLogs(t)
	removed, _ := runCleanupOn(t,
		fakeContainer{id: "05a1e0000001", created: cleanupNow.Add(-verifyContainerStaleAge - time.Minute), labels: verifyLabels("other-run")})

	assertRemoved(t, removed, "05a1e0000001")
	out := logs.String()
	if !strings.Contains(out, "level=INFO") || !strings.Contains(out, "removed lingering verification container") || !strings.Contains(out, "age=16m0s") {
		t.Fatalf("want the INFO removal line with the age, got:\n%s", out)
	}
}

func TestCleanupVerifyContainers_LeavesOwnRunAtAnyAge(t *testing.T) {
	removed, _ := runCleanupOn(t,
		fakeContainer{id: "0000000000a1", created: cleanupNow.Add(-2 * verifyContainerStaleAge), labels: verifyLabels("this-run")},
		fakeContainer{id: "0000000000a2", created: cleanupNow.Add(-2 * verifyContainerStaleAge), labels: verifyLabels("other-run")})

	assertRemoved(t, removed, "0000000000a2")
}

func TestCleanupVerifyContainers_UnlabelledJudgedByAge(t *testing.T) {
	removed, _ := runCleanupOn(t,
		fakeContainer{id: "01d000000001", created: cleanupNow.Add(-time.Hour), labels: verifyLabels("")},
		fakeContainer{id: "0f0000000002", created: cleanupNow.Add(-time.Minute), labels: verifyLabels("")},
		fakeContainer{id: "0e0000000003", created: cleanupNow.Add(-time.Hour), labels: map[string]string{"app": "other"}})

	assertRemoved(t, removed, "01d000000001")
}

func TestCleanupVerifyContainers_UnlabelledOldRemovedByVerifierWithoutRunID(t *testing.T) {
	exec := &MockCommandExecutor{Commands: map[string]MockCommandResponse{
		verifyCleanupListCommand():  {Stdout: "01d000000002@" + dockerCreatedAt(cleanupNow.Add(-time.Hour))},
		"docker rm -f 01d000000002": {},
	}}
	host := &hostparse.HostInfo{Host: "wrk1"}
	v := &Verifier{cmdExecutor: exec, timeProvider: NewMockTimeProvider(cleanupNow)}

	v.cleanupVerifyContainers(host, command.NewDockerOperations(exec, host))

	if last := strings.Join(exec.ExecutedCommands[len(exec.ExecutedCommands)-1].Command, " "); last != "docker rm -f 01d000000002" {
		t.Fatalf("an unlabelled stale container must not match an empty run id; last command %q", last)
	}
	for _, c := range exec.ExecutedCommands {
		if c.Command[1] == constants.DockerCmdPS && strings.Contains(strings.Join(c.Command, " "), constants.DockerLabelVerifyRun) {
			t.Fatalf("a verifier without a run id must not list by an empty run label: %q", c.Command)
		}
	}
}

func TestCleanupVerifyContainers_OwnListingFailureRemovesNothing(t *testing.T) {
	logs := captureLogs(t)
	exec := &MockCommandExecutor{Commands: map[string]MockCommandResponse{
		verifyCleanupListCommand():              {Stdout: "01d000000003@" + dockerCreatedAt(cleanupNow.Add(-time.Hour))},
		verifyCleanupOwnListCommand("this-run"): {Stderr: "boom", Error: fmt.Errorf("exit status 1")},
		"docker rm -f 01d000000003":             {},
	}}
	host := &hostparse.HostInfo{Host: "wrk1"}
	v := &Verifier{cmdExecutor: exec, timeProvider: NewMockTimeProvider(cleanupNow), runID: "this-run"}

	v.cleanupVerifyContainers(host, command.NewDockerOperations(exec, host))

	for _, c := range exec.ExecutedCommands {
		if c.Command[1] == constants.DockerCmdRM {
			t.Fatalf("removed %q although this run's containers could not be listed", c.Command)
		}
	}
	if !strings.Contains(logs.String(), "failed to list this run's containers") {
		t.Fatalf("want a WARN line for the failed own listing, got:\n%s", logs.String())
	}
}

func TestCleanupVerifyContainers_LeavesUnparseableTimeAlone(t *testing.T) {
	logs := captureLogs(t)
	removed := runCleanup(t, strings.Join([]string{
		"0bad00000001@yesterday-ish",
		"garbage-line-without-fields",
	}, "\n"), "")

	assertRemoved(t, removed)
	out := logs.String()
	if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "container=0bad00000001") {
		t.Fatalf("want a WARN line naming 0bad00000001, got:\n%s", out)
	}
}

func TestCleanupVerifyContainers_ReadsZoneOffset(t *testing.T) {
	// A Docker host in UTC-5 reports local time. 06:58 -0500 is 11:58 UTC, two minutes old for a
	// client clock of 12:00 UTC, so it is fresh; read without the offset it would look five hours old.
	removed := runCleanup(t, "0e5700000001@2026-11-14 06:58:00 -0500 EST", "")

	assertRemoved(t, removed)
}

func TestCleanupVerifyContainers_StaleAgeBoundary(t *testing.T) {
	removed, _ := runCleanupOn(t,
		fakeContainer{id: "0a6e00000001", created: cleanupNow.Add(-verifyContainerStaleAge), labels: verifyLabels("other-run")},
		fakeContainer{id: "0a6e00000002", created: cleanupNow.Add(-verifyContainerStaleAge + time.Second), labels: verifyLabels("other-run")})

	assertRemoved(t, removed, "0a6e00000001")
}

func TestCleanupVerifyContainers_LeavesLineWithoutContainerIDAlone(t *testing.T) {
	// Only a hex ID may reach docker rm, whatever a docker ps line holds.
	logs := captureLogs(t)
	old := dockerCreatedAt(cleanupNow.Add(-time.Hour))
	removed := runCleanup(t, strings.Join([]string{
		"$(touch${IFS}x)@" + old,
		"0123456789AB@" + old,
		"0123456789a@" + old,
		strings.Repeat("a", 65) + "@" + old,
		strings.Repeat("a", 64) + "@" + old,
	}, "\n"), "")

	assertRemoved(t, removed, strings.Repeat("a", 64))
	if got := strings.Count(logs.String(), "unexpected docker ps line"); got != 4 {
		t.Fatalf("want 4 WARN lines for the lines without a container ID, got %d:\n%s", got, logs.String())
	}
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
