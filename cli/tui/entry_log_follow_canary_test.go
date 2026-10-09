//go:build docker_log_follow_canary

package tui

import (
	"context"
	"errors"
	"os"
	osexec "os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

// TestRemoteLogFetcherDockerCanary runs the real fetcher, Docker operations and
// command executor against a local Docker daemon (the executor's local path, so
// no SSH). It pins the docker logs behaviour the relay relies on: --follow
// exits cleanly when the container stops, only stdout is relayed, and --since
// is inclusive.
//
//	SPT_TEST_DOCKER_IMAGE=<local image with sh> go test -tags docker_log_follow_canary ./tui -run DockerCanary
func TestRemoteLogFetcherDockerCanary(t *testing.T) {
	image := os.Getenv("SPT_TEST_DOCKER_IMAGE")
	if image == "" {
		t.Fatal("docker log follow canary requires SPT_TEST_DOCKER_IMAGE (a local image that provides sh)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	host := &hostparse.HostInfo{Host: "localhost", IsLocal: true}
	exec := command.NewCommandExecutor()

	out, _, err := exec.ExecuteCommand(ctx, host, []string{"docker", "run", "-d", "--entrypoint", "sh", image,
		"-c", "echo out-1; echo err-1 >&2; sleep 2; echo out-2"})
	if err != nil {
		t.Fatalf("docker run: %v", err)
	}
	id := strings.TrimSpace(out)
	t.Cleanup(func() {
		_, _, _ = exec.ExecuteCommand(context.Background(), host, []string{"docker", "rm", "-f", id})
	})

	// Follow while the container runs; Stream must return on its own when it stops.
	ops := command.NewDockerOperations(exec, host)
	var relayed []string
	started := time.Now()
	if err := newRemoteLogFetcher(ops, id).Stream(ctx, func(s string) { relayed = append(relayed, s) }); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if elapsed := time.Since(started); elapsed < time.Second {
		t.Fatalf("Stream returned after %v, before the container finished", elapsed)
	}
	if strings.Join(relayed, "|") != "out-1|out-2" {
		t.Fatalf("relayed lines = %q, want stdout only", relayed)
	}

	// Resume semantics against the stopped container.
	var stamped []string
	if err := ops.FollowContainerLogs(ctx, id, time.Time{}, func(l string) { stamped = append(stamped, l) }, func(string) {}); err != nil {
		t.Fatalf("FollowContainerLogs: %v", err)
	}
	if len(stamped) != 2 {
		t.Fatalf("stdout lines = %q", stamped)
	}
	_, firstTS, ok := splitDockerTimestamp(stamped[0])
	if !ok {
		t.Fatalf("stdout line has no docker timestamp: %q", stamped[0])
	}
	from := func(since time.Time) []string {
		var lines []string
		if err := ops.FollowContainerLogs(ctx, id, since, func(l string) {
			text, _, _ := splitDockerTimestamp(l)
			lines = append(lines, text)
		}, func(string) {}); err != nil {
			t.Fatalf("FollowContainerLogs since %v: %v", since, err)
		}
		return lines
	}
	if got := from(firstTS); strings.Join(got, "|") != "out-1|out-2" {
		t.Fatalf("--since at the line's timestamp should include it, got %q", got)
	}
	if got := from(firstTS.Add(dockerSinceInclusiveSkew)); strings.Join(got, "|") != "out-2" {
		t.Fatalf("--since just past the line's timestamp should exclude it, got %q", got)
	}
}

// TestRemoteLogFetcherSSHCanary runs the same real stack against a remote
// Docker host over SSH. It proves that the stream still ends when the
// container stops although the session's stdin is held open, and that
// cancelling the relay of a quiet container leaves no follower on the host.
//
//	SPT_TEST_SSH_HOST=<user@host> SPT_TEST_DOCKER_IMAGE=<image with sh on that host> \
//	  go test -tags docker_log_follow_canary ./tui -run SSHCanary
func TestRemoteLogFetcherSSHCanary(t *testing.T) {
	target, image := os.Getenv("SPT_TEST_SSH_HOST"), os.Getenv("SPT_TEST_DOCKER_IMAGE")
	if target == "" || image == "" {
		t.Fatal("ssh log follow canary requires SPT_TEST_SSH_HOST (user@host with key-based ssh) and SPT_TEST_DOCKER_IMAGE (an image with sh on that host)")
	}
	host, err := hostparse.ParseSingleHost(target)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	exec := command.NewCommandExecutor()
	ops := command.NewDockerOperations(exec, host)
	run := func(script string) string {
		// The remote login shell parses the command line, so quote the script.
		out, stderr, err := exec.ExecuteCommand(ctx, host, []string{"docker", "run", "-d", "--entrypoint", "sh", image, "-c", "'" + script + "'"})
		if err != nil {
			t.Fatalf("docker run: %v: %s", err, stderr)
		}
		id := strings.TrimSpace(out)
		t.Cleanup(func() {
			_, _, _ = exec.ExecuteCommand(context.Background(), host, []string{"docker", "rm", "-f", id})
		})
		return id
	}
	followers := func(id string) int {
		// The bracket keeps the pattern from matching the shell that runs pgrep.
		out, stderr, err := exec.ExecuteCommand(ctx, host, []string{"pgrep", "-fc", "'[d]ocker logs --follow .*" + id + "'"})
		var exitErr *osexec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			return 0
		}
		if err != nil {
			t.Fatalf("pgrep: %v: %s", err, stderr)
		}
		n, err := strconv.Atoi(strings.TrimSpace(out))
		if err != nil {
			t.Fatalf("pgrep output %q", out)
		}
		return n
	}

	// The session ends by itself when the container stops.
	id := run("echo out-1; echo err-1 >&2; sleep 2; echo out-2")
	var relayed []string
	started := time.Now()
	if err := newRemoteLogFetcher(ops, id).Stream(ctx, func(s string) { relayed = append(relayed, s) }); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if elapsed := time.Since(started); elapsed < time.Second {
		t.Fatalf("Stream returned after %v, before the container finished", elapsed)
	}
	if strings.Join(relayed, "|") != "out-1|out-2" {
		t.Fatalf("relayed lines = %q, want stdout only", relayed)
	}

	// Cancelling the relay of a quiet container must not leave its follower running.
	quiet := run("echo ready; exec sleep 600")
	streamCtx, stopStream := context.WithCancel(ctx)
	defer stopStream()
	ready := make(chan struct{})
	var once sync.Once
	done := make(chan error, 1)
	go func() {
		done <- newRemoteLogFetcher(ops, quiet).Stream(streamCtx, func(s string) {
			if s == "ready" {
				once.Do(func() { close(ready) })
			}
		})
	}()
	select {
	case <-ready:
	case <-time.After(30 * time.Second):
		t.Fatal("no line relayed from the quiet container")
	}
	if followers(quiet) == 0 {
		t.Fatal("pgrep found no follower while streaming; the cleanup check below would prove nothing")
	}
	stopStream()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Stream: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Stream did not return after cancellation")
	}
	deadline := time.Now().Add(10 * time.Second)
	for followers(quiet) > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("docker logs follower for %s still running on %s after the relay stopped", quiet, host.Host)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
