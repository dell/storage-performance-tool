package tui

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/docker/docker/api/types/container"
)

// fakeNumaNodes gives the tests a host with node0 (CPUs 0,2,4) and node1 (CPUs 1,3,5).
func fakeNumaNodes(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	for node, cpus := range map[string]string{"node0": "0,2,4\n", "node1": "1,3,5\n"} {
		if err := os.MkdirAll(filepath.Join(root, node), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, node, "cpulist"), []byte(cpus), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	previous := numaNodeRoot
	numaNodeRoot = root
	t.Cleanup(func() { numaNodeRoot = previous })
}

func TestWithoutANumaNodeTheContainerIsNotPlaced(t *testing.T) {
	fakeNumaNodes(t)
	t.Setenv(constants.EnvSptEngineNumaNode, "")
	hostConfig := &container.HostConfig{}
	if err := applyEnginePlacement(hostConfig); err != nil {
		t.Fatal(err)
	}
	if hostConfig.CpusetCpus != "" || hostConfig.CpusetMems != "" {
		t.Fatalf("placed without the setting: %q %q", hostConfig.CpusetCpus, hostConfig.CpusetMems)
	}
}

func TestANumaNodeHoldsTheContainerToItsCpusAndMemory(t *testing.T) {
	fakeNumaNodes(t)
	t.Setenv(constants.EnvSptEngineNumaNode, " 1 ")
	hostConfig := &container.HostConfig{}
	if err := applyEnginePlacement(hostConfig); err != nil {
		t.Fatal(err)
	}
	if hostConfig.CpusetCpus != "1,3,5" || hostConfig.CpusetMems != "1" {
		t.Fatalf("got %q %q", hostConfig.CpusetCpus, hostConfig.CpusetMems)
	}
}

func TestABadNumaNodeIsRefused(t *testing.T) {
	fakeNumaNodes(t)
	for _, value := range []string{"x", "-1", "0,1", "2"} {
		t.Setenv(constants.EnvSptEngineNumaNode, value)
		hostConfig := &container.HostConfig{}
		err := applyEnginePlacement(hostConfig)
		if err == nil || !strings.Contains(err.Error(), constants.EnvSptEngineNumaNode) {
			t.Fatalf("%q: err = %v", value, err)
		}
		if hostConfig.CpusetCpus != "" || hostConfig.CpusetMems != "" {
			t.Fatalf("%q: placed anyway", value)
		}
	}
}

func TestTheLocalNodeContainerIsCreatedOnTheNumaNode(t *testing.T) {
	fakeNumaNodes(t)
	t.Setenv(constants.EnvSptEngineNumaNode, "0")
	f := &fakeDockerClient{}
	dm := &DockerManager{client: f, ctx: context.Background()}
	if _, err := dm.StartContainerInNodeMode("test-image", "10080", constants.HostNetworkMode, nil); err != nil {
		t.Fatal(err)
	}
	if f.createHostConfig.CpusetCpus != "0,2,4" || f.createHostConfig.CpusetMems != "0" {
		t.Fatalf("got %q %q", f.createHostConfig.CpusetCpus, f.createHostConfig.CpusetMems)
	}
}

func TestABadNumaNodeCreatesNoContainer(t *testing.T) {
	fakeNumaNodes(t)
	t.Setenv(constants.EnvSptEngineNumaNode, "7")
	f := &fakeDockerClient{}
	dm := &DockerManager{client: f, ctx: context.Background()}
	if _, err := dm.StartContainerInNodeMode("test-image", "10080", constants.HostNetworkMode, nil); err == nil {
		t.Fatal("started on a node the host does not have")
	}
	if f.createHostConfig != nil {
		t.Fatalf("created a container: %+v", f.createHostConfig)
	}
}
