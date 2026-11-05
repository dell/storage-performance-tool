package tui

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/nicplan"
	"github.com/dell/storage-performance-tool/cli/internal/poolsizing"
)

const testGiB = int64(1) << 30

func sizedEngine(host string, limit int64) poolsizing.Engine {
	return poolsizing.Decide(poolsizing.DecideInput{
		Host: host,
		Output: &nicplan.Output{
			Plan:     &nicplan.Plan{OK: true, NicSet: []string{"ens1"}},
			Capacity: &nicplan.Capacity{Bytes: 2048 * testGiB, Source: "MemTotal"},
			PoolDemand: &nicplan.PoolDemand{Steps: []nicplan.PoolStep{
				{ID: "mt-001-create", LimitBytes: limit, LimitForPoolsBytes: limit, LimitForProcessBytes: limit, Known: true},
			}},
		},
	})
}

func setSized(t *testing.T, local bool, host string, limit int64) {
	t.Helper()
	t.Cleanup(poolsizing.ClearLaunchOptions)
	poolsizing.SetLaunchOptions(poolsizing.Plan{Engines: []poolsizing.Engine{sizedEngine(host, limit)}},
		[]*hostparse.HostInfo{{Host: host, IsLocal: local}})
}

func TestTheLocalNodeContainerGetsTheValueInJavaOpts(t *testing.T) {
	setSized(t, true, "127.0.0.1", 192*testGiB)
	f := &fakeDockerClient{}
	dm := &DockerManager{client: f, ctx: context.Background()}
	if _, err := dm.StartContainerInNodeMode("test-image", "10080", constants.BridgeNetworkMode, nil); err != nil {
		t.Fatal(err)
	}
	env := f.createContainerConfig.Env
	if !containsString(env, "JAVA_OPTS=-XX:MaxDirectMemorySize=192g") {
		t.Fatalf("env = %v", env)
	}
	if !containsString(env, poolsizing.EstimateEnv+"=mt-001-create="+strconv.FormatInt(192*testGiB, 10)) {
		t.Fatalf("env = %v", env)
	}
}

func TestALocalContainerKeepsItsOwnJavaOptsBesideTheValue(t *testing.T) {
	poolsizing.ClearLaunchOptions()
	got := withEngineOptions([]string{"JAVA_OPTS=-Djava.rmi.server.hostname=127.0.0.1"})
	if len(got) != 1 {
		t.Fatalf("an unsized engine gets nothing: %v", got)
	}
	setSized(t, true, "127.0.0.1", 192*testGiB)
	got = withEngineOptions([]string{"JAVA_OPTS=-Djava.rmi.server.hostname=127.0.0.1"})
	if got[0] != "JAVA_OPTS=-Djava.rmi.server.hostname=127.0.0.1 -XX:MaxDirectMemorySize=192g" {
		t.Fatalf("%v", got)
	}
}

func TestWithoutSizingTheLocalContainersJvmOptionsAreUnchanged(t *testing.T) {
	poolsizing.ClearLaunchOptions()
	f := &fakeDockerClient{}
	dm := &DockerManager{client: f, ctx: context.Background()}
	if _, err := dm.StartContainerInNodeMode("test-image", "10080", constants.BridgeNetworkMode, nil); err != nil {
		t.Fatal(err)
	}
	for _, entry := range f.createContainerConfig.Env {
		if strings.HasPrefix(entry, "JAVA_OPTS=") || strings.HasPrefix(entry, poolsizing.EstimateEnv) {
			t.Fatalf("env = %v", f.createContainerConfig.Env)
		}
	}
}

func TestARemoteWorkerGetsItsOwnValueInJavaOpts(t *testing.T) {
	mgr, mock, host := newTestRemoteManager(t)
	setSized(t, false, host.Host, 96*testGiB)
	if _, err := mgr.StartWorkerNodeContainer(constants.DefaultSptImage, "10.0.0.10", 40000, 3, nil); err != nil {
		t.Fatal(err)
	}
	executed := mock.GetExecutedCommandsMatching("docker run")
	command := strings.Join(executed[len(executed)-1].Command, " ")
	if !strings.Contains(command, "-Djava.rmi.server.hostname=10.0.0.10 -XX:MaxDirectMemorySize=96g") {
		t.Fatalf("the worker keeps its RMI hostname and gains the value: %s", command)
	}
	if !strings.Contains(command, poolsizing.EstimateEnv+"=mt-001-create=") {
		t.Fatalf("%s", command)
	}
}

func TestARemoteEntryNodeGetsTheValueAndAnUnsizedOneDoesNot(t *testing.T) {
	mgr, mock, host := newTestRemoteManager(t)
	setSized(t, false, host.Host, 96*testGiB)
	if _, err := mgr.StartEntryNodeContainer(constants.DefaultSptImage, nil, nil, constants.HostNetworkMode); err != nil {
		t.Fatal(err)
	}
	executed := mock.GetExecutedCommandsMatching("docker run")
	if command := strings.Join(executed[len(executed)-1].Command, " "); !strings.Contains(command, "-XX:MaxDirectMemorySize=96g") {
		t.Fatalf("%s", command)
	}

	poolsizing.ClearLaunchOptions()
	other, otherMock, _ := newTestRemoteManager(t)
	if _, err := other.StartEntryNodeContainer(constants.DefaultSptImage, nil, nil, constants.HostNetworkMode); err != nil {
		t.Fatal(err)
	}
	executed = otherMock.GetExecutedCommandsMatching("docker run")
	if command := strings.Join(executed[len(executed)-1].Command, " "); strings.Contains(command, "MaxDirectMemorySize") || strings.Contains(command, poolsizing.EstimateEnv) {
		t.Fatalf("without sizing the JVM options are unchanged: %s", command)
	}
}

// Through the real entrypoint: the last -XX:MaxDirectMemorySize wins, and SPT_JAVA_OPTS comes last.
func TestTheEntrypointPlacesTheValueBeforeSptJavaOpts(t *testing.T) {
	bin := t.TempDir()
	stub := filepath.Join(bin, "java")
	if err := os.WriteFile(stub, []byte("#!/bin/sh\nfor arg in \"$@\"; do echo \"$arg\"; done\n"), 0o700); err != nil { // #nosec G306 -- a test's executable stub.
		t.Fatal(err)
	}
	entrypoint, err := filepath.Abs("../../engine/bundle/docker/entrypoint.sh")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(entrypoint); err != nil {
		t.Fatal(err)
	}
	run := func(extra ...string) string {
		t.Helper()
		cmd := exec.Command("sh", entrypoint, "--version") // #nosec G204 -- the repository's own entrypoint.
		cmd.Env = append([]string{"PATH=" + bin + ":" + os.Getenv("PATH")}, extra...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("entrypoint: %v: %s", err, out)
		}
		return string(out)
	}
	last := func(out string) string {
		value := ""
		for _, line := range strings.Split(out, "\n") {
			if strings.HasPrefix(line, "-XX:MaxDirectMemorySize=") {
				value = line
			}
		}
		return value
	}

	if got := last(run()); got != "-XX:MaxDirectMemorySize=64g" {
		t.Fatalf("the shipped default: %q", got)
	}

	t.Cleanup(poolsizing.ClearLaunchOptions)
	setSized(t, true, "127.0.0.1", 192*testGiB)
	if got := last(run(withEngineOptions(nil)...)); got != "-XX:MaxDirectMemorySize=192g" {
		t.Fatalf("the local launch path: %q", got)
	}
	remote := remoteEngineEnvironment(remoteNodeEnvironment("10.0.0.10"), poolsizing.LocalHostKey)
	if got := last(run(constants.JavaOptsEnvVar + "=" + remote[constants.JavaOptsEnvVar])); got != "-XX:MaxDirectMemorySize=192g" {
		t.Fatalf("the remote launch path: %q", got)
	}
	userWins := run(append(withEngineOptions(nil), constants.EnvSptJavaOpts+"=-XX:MaxDirectMemorySize=128g")...)
	if got := last(userWins); got != "-XX:MaxDirectMemorySize=128g" {
		t.Fatalf("a user's value wins: %q", got)
	}
}
