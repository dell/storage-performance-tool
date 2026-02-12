package verification

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"time"
)

func TestPrintNodeResult_RdmaChecksShown(t *testing.T) {
	report := &Report{
		Results: map[string]*NodeResult{
			"rdma-host": {
				Host:            "rdma-host",
				SSHConnectivity: Check{Passed: true, Message: "SSH OK"},
				DockerAvailable: Check{Passed: true, Message: "Docker OK"},
				DockerImagePull: Check{Passed: true, Message: "Image pulled"},
				ContainerStart:  Check{Passed: true, Message: "Container started"},
				RdmaDevice:      Check{Passed: true, Message: "RDMA devices found: uverbs0"},
				RdmaDriver:      Check{Passed: true, Message: "RDMA runtime libraries present in container"},
				PortsAccessible: Check{Passed: true, Message: "Ports OK"},
				MetricsEndpoint: Check{Passed: true, Message: "Metrics OK"},
				ControlEndpoint: Check{Passed: true, Message: "Control OK"},
				ContainerCleanup: Check{Passed: true, Message: "Cleaned up"},
				Overall:         Check{Passed: true, Message: "READY"},
			},
		},
		Duration: 5 * time.Second,
		Config:   Config{MinHosts: 1},
	}
	report.assess()

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	report.Print()

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	buf.ReadFrom(r)
	output := buf.String()

	if !strings.Contains(output, "RDMA Device") {
		t.Error("Expected 'RDMA Device' in report output")
	}
	if !strings.Contains(output, "RDMA Driver") {
		t.Error("Expected 'RDMA Driver' in report output")
	}
	if !strings.Contains(output, "uverbs0") {
		t.Error("Expected 'uverbs0' in report output")
	}
}

func TestPrintNodeResult_RdmaChecksHiddenWhenEmpty(t *testing.T) {
	report := &Report{
		Results: map[string]*NodeResult{
			"non-rdma-host": {
				Host:            "non-rdma-host",
				SSHConnectivity: Check{Passed: true, Message: "SSH OK"},
				DockerAvailable: Check{Passed: true, Message: "Docker OK"},
				DockerImagePull: Check{Passed: true, Message: "Image pulled"},
				ContainerStart:  Check{Passed: true, Message: "Container started"},
				// RdmaDevice and RdmaDriver have zero-value (empty Message)
				PortsAccessible: Check{Passed: true, Message: "Ports OK"},
				MetricsEndpoint: Check{Passed: true, Message: "Metrics OK"},
				ControlEndpoint: Check{Passed: true, Message: "Control OK"},
				ContainerCleanup: Check{Passed: true, Message: "Cleaned up"},
				Overall:         Check{Passed: true, Message: "READY"},
			},
		},
		Duration: 3 * time.Second,
		Config:   Config{MinHosts: 1},
	}
	report.assess()

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	report.Print()

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	buf.ReadFrom(r)
	output := buf.String()

	if strings.Contains(output, "RDMA Device") {
		t.Error("Expected 'RDMA Device' to NOT appear when check was not performed")
	}
	if strings.Contains(output, "RDMA Driver") {
		t.Error("Expected 'RDMA Driver' to NOT appear when check was not performed")
	}
}

func TestPrintNodeResult_RdmaDeviceFailed(t *testing.T) {
	report := &Report{
		Results: map[string]*NodeResult{
			"no-rdma-host": {
				Host:            "no-rdma-host",
				SSHConnectivity: Check{Passed: true, Message: "SSH OK"},
				DockerAvailable: Check{Passed: true, Message: "Docker OK"},
				DockerImagePull: Check{Passed: true, Message: "Image pulled"},
				ContainerStart:  Check{Passed: true, Message: "Container started"},
				RdmaDevice:      Check{Passed: false, Message: "RDMA device directory /dev/infiniband/ not found"},
				PortsAccessible: Check{Passed: true, Message: "Ports OK"},
				ContainerCleanup: Check{Passed: true, Message: "Cleaned up"},
				Overall:         Check{Passed: false, Message: "RDMA device not available"},
			},
		},
		Duration: 2 * time.Second,
		Config:   Config{MinHosts: 1},
	}
	report.assess()

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	report.Print()

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	buf.ReadFrom(r)
	output := buf.String()

	if !strings.Contains(output, "RDMA Device") {
		t.Error("Expected 'RDMA Device' check to appear in failed state")
	}
	if !strings.Contains(output, "FAIL") {
		t.Error("Expected 'FAIL' in report output for failed RDMA device check")
	}
	if !strings.Contains(output, "NOT READY") {
		t.Error("Expected 'NOT READY' summary when RDMA check fails")
	}
}

func TestReportAssess_RdmaFailureAffectsOverall(t *testing.T) {
	report := &Report{
		Results: map[string]*NodeResult{
			"host1": {
				Host:    "host1",
				Overall: Check{Passed: false, Message: "RDMA device not available"},
			},
		},
		Config: Config{MinHosts: 1},
	}
	report.assess()

	if report.Ready {
		t.Error("Expected report to be NOT READY when the only node fails RDMA check")
	}
	if !strings.Contains(report.Summary, "host1") {
		t.Error("Expected summary to mention the failed host")
	}
}
