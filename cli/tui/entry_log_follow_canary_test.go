//go:build docker_log_follow_canary

package tui

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

// TestRemoteLogFetcherDockerCanary runs the real fetcher, Docker operations and
// command executor against a local Docker daemon (the executor's local path, so
// no SSH). It pins the docker logs behaviour the relay relies on: --follow
// exits cleanly when the container stops, container stderr carries timestamps,
// and --since is inclusive.
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
	if len(relayed) != 3 || relayed[2] != "out-2" || !containsAll(relayed, "out-1", "err-1") {
		t.Fatalf("relayed lines = %q", relayed)
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

func containsAll(lines []string, want ...string) bool {
	seen := make(map[string]bool, len(lines))
	for _, l := range lines {
		seen[l] = true
	}
	for _, w := range want {
		if !seen[w] {
			return false
		}
	}
	return true
}
