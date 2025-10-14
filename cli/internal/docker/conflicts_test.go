/*
Copyright © 2025 Dell Technologies
*/

package docker

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
)

func TestPortChecker_CheckForConflicts(t *testing.T) {
	tests := []struct {
		name               string
		ports              []int
		mockResponses      map[string]command.MockResponse
		wantConflictPorts  []int
		wantAvailablePorts []int
		wantContainers     int
		wantSptHost        bool
		wantErr            bool
	}{
		{
			name:  "no conflicts - all ports available",
			ports: []int{8080, 9090},
			mockResponses: map[string]command.MockResponse{
				"sh -c " + constants.PortSnapshotCommand: {
					Stdout: "", Stderr: "", Error: nil,
				},
			},
			wantConflictPorts:  []int{},
			wantAvailablePorts: []int{8080, 9090},
			wantContainers:     0,
			wantSptHost:        false,
			wantErr:            false,
		},
		{
			name:  "some conflicts - mixed port availability",
			ports: []int{8080, 9090, 3000},
			mockResponses: map[string]command.MockResponse{
				"sh -c " + constants.PortSnapshotCommand: {
					Stdout: "tcp LISTEN 0 128 0.0.0.0:8080 0.0.0.0:*\n tcp LISTEN 0 128 127.0.0.1:3000 0.0.0.0:*\n",
					Stderr: "", Error: nil,
				},
				getUnfilteredDockerPSCommand(): {
					Stdout: "abc123def456\ttest-container\t" + constants.DefaultSptImage + "\trunning\t0.0.0.0:8080->8080/tcp,0.0.0.0:3000->3000/tcp\t2024-01-01 12:00:00 +0000 UTC",
					Stderr: "",
					Error:  nil,
				},
			},
			wantConflictPorts:  []int{8080, 3000},
			wantAvailablePorts: []int{9090},
			wantContainers:     1,
			wantSptHost:        true,
			wantErr:            false,
		},
		{
			name:  "all conflicts - no available ports",
			ports: []int{1099, 9999},
			mockResponses: map[string]command.MockResponse{
				"sh -c " + constants.PortSnapshotCommand: {
					Stdout: "tcp LISTEN 0 128 0.0.0.0:1099 0.0.0.0:*\n tcp LISTEN 0 128 0.0.0.0:9999 0.0.0.0:*\n",
					Stderr: "", Error: nil,
				},
				getUnfilteredDockerPSCommand(): {
					Stdout: "def456abc123\tspt-verify-test\t" + constants.DefaultSptImage + "\trunning\t0.0.0.0:1099->1099/tcp,0.0.0.0:9999->9999/tcp\t2024-01-01 12:00:00 +0000 UTC",
					Stderr: "",
					Error:  nil,
				},
			},
			wantConflictPorts:  []int{1099, 9999},
			wantAvailablePorts: []int{},
			wantContainers:     1,
			wantSptHost:        true,
			wantErr:            false,
		},
		{
			name:  "conflicts with non-containerized services",
			ports: []int{80, 443},
			mockResponses: map[string]command.MockResponse{
				"sh -c " + constants.PortSnapshotCommand: {
					Stdout: "tcp LISTEN 0 128 0.0.0.0:80 0.0.0.0:*\n tcp LISTEN 0 128 0.0.0.0:443 0.0.0.0:*\n",
					Stderr: "", Error: nil,
				},
				getUnfilteredDockerPSCommand(): {
					Stdout: "",
					Stderr: "",
					Error:  nil,
				},
			},
			wantConflictPorts:  []int{80, 443},
			wantAvailablePorts: []int{},
			wantContainers:     0,
			wantSptHost:        false,
			wantErr:            false,
		},
		{
			name:  "netstat command failure",
			ports: []int{8080},
			mockResponses: map[string]command.MockResponse{
				"sh -c " + constants.PortSnapshotCommand: {
					Stdout: "", Stderr: "snapshot error", Error: fmt.Errorf("failed"),
				},
			},
			wantConflictPorts:  []int{8080}, // Should assume port in use on error
			wantAvailablePorts: []int{},
			wantContainers:     0,
			wantSptHost:        false,
			wantErr:            false,
		},
		{
			name:  "docker not available",
			ports: []int{8080},
			mockResponses: map[string]command.MockResponse{
				"sh -c " + constants.PortSnapshotCommand: {
					Stdout: "tcp LISTEN 0 128 0.0.0.0:8080 0.0.0.0:*\n",
					Stderr: "", Error: nil,
				},
				getUnfilteredDockerPSCommand(): {
					Stdout: "",
					Stderr: "docker: command not found",
					Error:  fmt.Errorf("command not found"),
				},
			},
			wantConflictPorts:  []int{8080},
			wantAvailablePorts: []int{},
			wantContainers:     0,
			wantSptHost:        false,
			wantErr:            false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockExecutor := command.NewMockCommandExecutor()
			for cmd, response := range tt.mockResponses {
				mockExecutor.SetCommandResponse(cmd, response)
			}

			host := command.CreateLocalHost()
			checker := NewPortChecker(host, mockExecutor, tt.ports)
			ctx := context.Background()

			result, err := checker.CheckForConflicts(ctx)

			if (err != nil) != tt.wantErr {
				t.Errorf("CheckForConflicts() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !equalIntSlices(result.ConflictPorts, tt.wantConflictPorts) {
				t.Errorf("CheckForConflicts() ConflictPorts = %v, want %v", result.ConflictPorts, tt.wantConflictPorts)
			}

			if !equalIntSlices(result.AvailablePorts, tt.wantAvailablePorts) {
				t.Errorf("CheckForConflicts() AvailablePorts = %v, want %v", result.AvailablePorts, tt.wantAvailablePorts)
			}

			if len(result.Containers) != tt.wantContainers {
				t.Errorf("CheckForConflicts() found %d containers, want %d", len(result.Containers), tt.wantContainers)
			}

			if result.IsSptHost != tt.wantSptHost {
				t.Errorf("CheckForConflicts() IsSptHost = %v, want %v", result.IsSptHost, tt.wantSptHost)
			}

			// Verify host information
			if result.Host != host {
				t.Error("CheckForConflicts() result should contain the same host")
			}
		})
	}
}

func TestPortChecker_isPortAvailable(t *testing.T) {
	tests := []struct {
		name          string
		port          int
		mockResponse  command.MockResponse
		wantAvailable bool
		wantErr       bool
	}{
		{
			name: "port available - grep finds nothing",
			port: 8080,
			mockResponse: command.MockResponse{
				Stdout: "",
				Stderr: "exit status 1",
				Error:  fmt.Errorf("grep exit 1"),
			},
			wantAvailable: true,
			wantErr:       false,
		},
		{
			name: "port in use - grep finds match",
			port: 9999,
			mockResponse: command.MockResponse{
				Stdout: "tcp 0 0 0.0.0.0:9999 0.0.0.0:* LISTEN",
				Stderr: "",
				Error:  nil,
			},
			wantAvailable: false,
			wantErr:       false,
		},
		{
			name: "port tools missing treated as available",
			port: 8080,
			mockResponse: command.MockResponse{
				Stdout: "",
				Stderr: "netstat: command not found",
				Error:  fmt.Errorf("command not found"),
			},
			wantAvailable: true,
			wantErr:       false,
		},
		{
			name: "empty output from netstat",
			port: 3000,
			mockResponse: command.MockResponse{
				Stdout: "",
				Stderr: "",
				Error:  nil,
			},
			wantAvailable: true,
			wantErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockExecutor := command.NewMockCommandExecutor()
			expectedCmd := fmt.Sprintf("sh -c %s", constants.PortCheckCommand)
			_ = expectedCmd // silence linter; value used via pattern matching elsewhere
			// Fill both %d placeholders to avoid fmt errors in PortCheckCommand expectation
			expectedCmd = fmt.Sprintf("sh -c "+constants.PortCheckCommand, tt.port, tt.port)
			mockExecutor.SetCommandResponse(expectedCmd, tt.mockResponse)

			host := command.CreateLocalHost()
			checker := NewPortChecker(host, mockExecutor, []int{tt.port})
			ctx := context.Background()

			available, err := checker.isPortAvailable(ctx, tt.port)

			if (err != nil) != tt.wantErr {
				t.Errorf("isPortAvailable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if available != tt.wantAvailable {
				t.Errorf("isPortAvailable() available = %v, want %v", available, tt.wantAvailable)
			}
		})
	}
}

func TestPortChecker_findContainersUsingPorts(t *testing.T) {
	tests := []struct {
		name           string
		ports          []int
		mockResponse   command.MockResponse
		wantContainers int
		wantErr        bool
	}{
		{
			name:  "single container using port",
			ports: []int{8080},
			mockResponse: command.MockResponse{
				Stdout: "abc123def456\ttest-container\tnginx:latest\trunning\t0.0.0.0:8080->8080/tcp\t2024-01-01 12:00:00 +0000 UTC",
				Stderr: "",
				Error:  nil,
			},
			wantContainers: 1,
			wantErr:        false,
		},
		{
			name:  "multiple containers",
			ports: []int{8080, 9090},
			mockResponse: command.MockResponse{
				Stdout: "abc123def456\ttest-1\tnginx:latest\trunning\t0.0.0.0:8080->8080/tcp\t2024-01-01 12:00:00 +0000 UTC\ndef456abc123\ttest-2\tspt:latest\texited\t0.0.0.0:9090->9090/tcp\t2024-01-01 11:00:00 +0000 UTC",
				Stderr: "",
				Error:  nil,
			},
			wantContainers: 2,
			wantErr:        false,
		},
		{
			name:  "no containers using ports",
			ports: []int{8080, 9090},
			mockResponse: command.MockResponse{
				Stdout: "abc123def456\tother-container\tnginx:latest\trunning\t0.0.0.0:3000->3000/tcp\t2024-01-01 12:00:00 +0000 UTC",
				Stderr: "",
				Error:  nil,
			},
			wantContainers: 0,
			wantErr:        false,
		},
		{
			name:  "no containers at all",
			ports: []int{8080},
			mockResponse: command.MockResponse{
				Stdout: "",
				Stderr: "",
				Error:  nil,
			},
			wantContainers: 0,
			wantErr:        false,
		},
		{
			name:  "docker ps command fails",
			ports: []int{8080},
			mockResponse: command.MockResponse{
				Stdout: "",
				Stderr: "Cannot connect to the Docker daemon",
				Error:  fmt.Errorf("exit status 1"),
			},
			wantContainers: 0,
			wantErr:        true,
		},
		{
			name:  "malformed container line",
			ports: []int{8080},
			mockResponse: command.MockResponse{
				Stdout: "incomplete-line\twithout-enough-fields",
				Stderr: "",
				Error:  nil,
			},
			wantContainers: 0,
			wantErr:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockExecutor := command.NewMockCommandExecutor()
			mockExecutor.SetCommandResponse(getUnfilteredDockerPSCommand(), tt.mockResponse)

			host := command.CreateLocalHost()
			checker := NewPortChecker(host, mockExecutor, tt.ports)
			ctx := context.Background()

			containers, err := checker.findContainersUsingPorts(ctx, tt.ports)

			if (err != nil) != tt.wantErr {
				t.Errorf("findContainersUsingPorts() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(containers) != tt.wantContainers {
				t.Errorf("findContainersUsingPorts() found %d containers, want %d", len(containers), tt.wantContainers)
			}
		})
	}
}

func TestPortChecker_containerUsesAnyPort(t *testing.T) {
	tests := []struct {
		name      string
		container *ContainerInfo
		ports     []int
		wantUsing bool
	}{
		{
			name: "container uses single port",
			container: &ContainerInfo{
				ID:    "abc123",
				Name:  "test",
				Ports: []string{"0.0.0.0:8080->8080/tcp"},
			},
			ports:     []int{8080, 9090},
			wantUsing: true,
		},
		{
			name: "container uses multiple ports",
			container: &ContainerInfo{
				ID:    "def456",
				Name:  "multi-port",
				Ports: []string{"0.0.0.0:8080->8080/tcp,0.0.0.0:9090->9090/tcp"},
			},
			ports:     []int{9090},
			wantUsing: true,
		},
		{
			name: "container doesn't use target ports",
			container: &ContainerInfo{
				ID:    "ghi789",
				Name:  "other",
				Ports: []string{"0.0.0.0:3000->3000/tcp"},
			},
			ports:     []int{8080, 9090},
			wantUsing: false,
		},
		{
			name: "container with no port mappings",
			container: &ContainerInfo{
				ID:    "jkl012",
				Name:  "no-ports",
				Ports: []string{""},
			},
			ports:     []int{8080},
			wantUsing: false,
		},
		{
			name: "container with internal ports only",
			container: &ContainerInfo{
				ID:    "mno345",
				Name:  "internal",
				Ports: []string{"3000/tcp"},
			},
			ports:     []int{3000},
			wantUsing: false, // No host port mapping
		},
		{
			name: "container with UDP port mapping",
			container: &ContainerInfo{
				ID:    "pqr678",
				Name:  "udp-container",
				Ports: []string{"0.0.0.0:5353->5353/udp"},
			},
			ports:     []int{5353},
			wantUsing: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host := command.CreateLocalHost()
			checker := NewPortChecker(host, nil, nil)

			using := checker.containerUsesAnyPort(tt.container, tt.ports)

			if using != tt.wantUsing {
				t.Errorf("containerUsesAnyPort() = %v, want %v", using, tt.wantUsing)
			}
		})
	}
}

func TestPortChecker_parseContainerLine(t *testing.T) {
	tests := []struct {
		name          string
		line          string
		wantContainer *ContainerInfo
		wantErr       bool
	}{
		{
			name: "valid container line",
			line: "abc123def456\ttest-container\tnginx:latest\trunning\t0.0.0.0:8080->8080/tcp\t2024-01-01 12:00:00 +0000 UTC",
			wantContainer: &ContainerInfo{
				ID:    "abc123def456",
				Name:  "test-container",
				Image: "nginx:latest",
				State: "running",
				Ports: []string{"0.0.0.0:8080->8080/tcp"},
			},
			wantErr: false,
		},
		{
			name: "container with multiple ports",
			line: "def456abc123\tmulti-port\tspt:latest\texited\t0.0.0.0:8080->8080/tcp,0.0.0.0:9090->9090/tcp\t2024-01-01 11:00:00 +0000 UTC",
			wantContainer: &ContainerInfo{
				ID:    "def456abc123",
				Name:  "multi-port",
				Image: "spt:latest",
				State: "exited",
				Ports: []string{"0.0.0.0:8080->8080/tcp", "0.0.0.0:9090->9090/tcp"},
			},
			wantErr: false,
		},
		{
			name: "container with no ports",
			line: "ghi789abc012\tno-ports\tubuntu:latest\trunning\t\t2024-01-01 10:00:00 +0000 UTC",
			wantContainer: &ContainerInfo{
				ID:    "ghi789abc012",
				Name:  "no-ports",
				Image: "ubuntu:latest",
				State: "running",
				Ports: []string{""},
			},
			wantErr: false,
		},
		{
			name:          "malformed line - too few fields",
			line:          "abc123\ttest-container\tnginx:latest",
			wantContainer: nil,
			wantErr:       true,
		},
		{
			name:          "empty line",
			line:          "",
			wantContainer: nil,
			wantErr:       true,
		},
		{
			name: "line with extra whitespace",
			line: "  abc123def456  \t  test-container  \t  nginx:latest  \t  running  \t  0.0.0.0:8080->8080/tcp  \t  2024-01-01 12:00:00 +0000 UTC  ",
			wantContainer: &ContainerInfo{
				ID:    "abc123def456",
				Name:  "test-container",
				Image: "nginx:latest",
				State: "running",
				Ports: []string{"0.0.0.0:8080->8080/tcp"},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host := command.CreateLocalHost()
			checker := NewPortChecker(host, nil, nil)

			container, err := checker.parseContainerLine(tt.line)

			if (err != nil) != tt.wantErr {
				t.Errorf("parseContainerLine() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr {
				return
			}

			if container.ID != tt.wantContainer.ID {
				t.Errorf("parseContainerLine() ID = %s, want %s", container.ID, tt.wantContainer.ID)
			}
			if container.Name != tt.wantContainer.Name {
				t.Errorf("parseContainerLine() Name = %s, want %s", container.Name, tt.wantContainer.Name)
			}
			if container.Image != tt.wantContainer.Image {
				t.Errorf("parseContainerLine() Image = %s, want %s", container.Image, tt.wantContainer.Image)
			}
			if container.State != tt.wantContainer.State {
				t.Errorf("parseContainerLine() State = %s, want %s", container.State, tt.wantContainer.State)
			}
			if !equalStringSlices(container.Ports, tt.wantContainer.Ports) {
				t.Errorf("parseContainerLine() Ports = %v, want %v", container.Ports, tt.wantContainer.Ports)
			}
		})
	}
}

func TestPortChecker_containsSptContainers(t *testing.T) {
	tests := []struct {
		name       string
		containers []*ContainerInfo
		wantResult bool
	}{
		{
			name: "contains spt container by image",
			containers: []*ContainerInfo{
				{
					ID:    "abc123",
					Name:  "test-container",
					Image: "dcr.octo.dell.com/spt/spt",
					State: "running",
				},
			},
			wantResult: true,
		},
		{
			name: "contains spt container by name prefix",
			containers: []*ContainerInfo{
				{
					ID:    "def456",
					Name:  "spt-verify-test",
					Image: "nginx:latest",
					State: "running",
				},
			},
			wantResult: true,
		},
		{
			name: "contains both spt image and name patterns",
			containers: []*ContainerInfo{
				{
					ID:    "abc123",
					Name:  "regular-container",
					Image: "ubuntu:latest",
					State: "running",
				},
				{
					ID:    "def456",
					Name:  "spt-verify-worker",
					Image: "custom-image",
					State: "exited",
				},
				{
					ID:    "ghi789",
					Name:  "mongo-test",
					Image: "spt:latest",
					State: "running",
				},
			},
			wantResult: true,
		},
		{
			name: "no spt containers",
			containers: []*ContainerInfo{
				{
					ID:    "abc123",
					Name:  "nginx-container",
					Image: "nginx:latest",
					State: "running",
				},
				{
					ID:    "def456",
					Name:  "redis-container",
					Image: "redis:alpine",
					State: "running",
				},
			},
			wantResult: false,
		},
		{
			name:       "empty container list",
			containers: []*ContainerInfo{},
			wantResult: false,
		},
		{
			name:       "nil container list",
			containers: nil,
			wantResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host := command.CreateLocalHost()
			checker := NewPortChecker(host, nil, nil)

			result := checker.containsSptContainers(tt.containers)

			if result != tt.wantResult {
				t.Errorf("containsSptContainers() = %v, want %v", result, tt.wantResult)
			}
		})
	}
}

func TestConflictInfo_HasConflicts(t *testing.T) {
	tests := []struct {
		name   string
		info   *ConflictInfo
		expect bool
	}{
		{
			name: "has conflicts",
			info: &ConflictInfo{
				ConflictPorts: []int{8080, 9090},
			},
			expect: true,
		},
		{
			name: "no conflicts",
			info: &ConflictInfo{
				ConflictPorts: []int{},
			},
			expect: false,
		},
		{
			name: "nil conflict ports",
			info: &ConflictInfo{
				ConflictPorts: nil,
			},
			expect: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.info.HasConflicts()
			if result != tt.expect {
				t.Errorf("HasConflicts() = %v, want %v", result, tt.expect)
			}
		})
	}
}

func TestConflictInfo_GetSptContainers(t *testing.T) {
	allContainers := []*ContainerInfo{
		{
			ID:    "abc123",
			Name:  "nginx-container",
			Image: "nginx:latest",
			State: "running",
		},
		{
			ID:    "def456",
			Name:  "spt-verify-test",
			Image: "custom-image",
			State: "running",
		},
		{
			ID:    "ghi789",
			Name:  "regular-container",
			Image: "dcr.octo.dell.com/spt/spt",
			State: "exited",
		},
		{
			ID:    "jkl012",
			Name:  "redis-container",
			Image: "redis:alpine",
			State: "running",
		},
	}

	info := &ConflictInfo{
		Containers: allContainers,
	}

	sptContainers := info.GetSptContainers()

	if len(sptContainers) != 2 {
		t.Errorf("GetSptContainers() found %d containers, want 2", len(sptContainers))
	}

	// Should contain the spt-verify container and the spt image container
	foundVerify := false
	foundSpt := false

	for _, container := range sptContainers {
		if container.Name == "spt-verify-test" {
			foundVerify = true
		}
		if strings.Contains(container.Image, "spt") {
			foundSpt = true
		}
	}

	if !foundVerify {
		t.Error("GetSptContainers() should include spt-verify container")
	}
	if !foundSpt {
		t.Error("GetSptContainers() should include spt image container")
	}
}

func TestConflictInfo_GenerateSSHStopCommands(t *testing.T) {
	tests := []struct {
		name         string
		info         *ConflictInfo
		wantCommands []string
	}{
		{
			name: "local host with containers",
			info: &ConflictInfo{
				Host: command.CreateLocalHost(),
				Containers: []*ContainerInfo{
					{
						ID:   "abc123def456789012345678901234567890",
						Name: "test-container",
					},
					{
						ID:   "def456",
						Name: "short-id",
					},
				},
			},
			wantCommands: []string{
				"docker stop abc123def456",
				"docker stop def456",
			},
		},
		{
			name: "remote host with containers",
			info: &ConflictInfo{
				Host: command.CreateRemoteHost("remote.example.com"),
				Containers: []*ContainerInfo{
					{
						ID:   "abc123def456",
						Name: "remote-container",
					},
				},
			},
			wantCommands: []string{
				"ssh testuser@remote.example.com 'docker stop abc123def456'",
			},
		},
		{
			name: "no containers",
			info: &ConflictInfo{
				Host:       command.CreateLocalHost(),
				Containers: []*ContainerInfo{},
			},
			wantCommands: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			commands := tt.info.GenerateSSHStopCommands()

			if len(commands) != len(tt.wantCommands) {
				t.Errorf("GenerateSSHStopCommands() returned %d commands, want %d", len(commands), len(tt.wantCommands))
				return
			}

			for i, got := range commands {
				if got != tt.wantCommands[i] {
					t.Errorf("GenerateSSHStopCommands()[%d] = %q, want %q", i, got, tt.wantCommands[i])
				}
			}
		})
	}
}

func TestNewPortChecker(t *testing.T) {
	host := command.CreateLocalHost()
	mockExecutor := command.NewMockCommandExecutor()
	ports := []int{8080, 9090, 1099}

	checker := NewPortChecker(host, mockExecutor, ports)

	if checker == nil {
		t.Fatal("NewPortChecker() returned nil")
	}

	if checker.host != host {
		t.Error("NewPortChecker() host not set correctly")
	}

	if checker.cmdExecutor != mockExecutor {
		t.Error("NewPortChecker() executor not set correctly")
	}

	if !equalIntSlices(checker.ports, ports) {
		t.Errorf("NewPortChecker() ports = %v, want %v", checker.ports, ports)
	}
}

func TestPortChecker_ContextCancellation(t *testing.T) {
	mockExecutor := command.NewMockCommandExecutor()
	mockExecutor.FailureMode = "context_cancelled"

	host := command.CreateLocalHost()
	checker := NewPortChecker(host, mockExecutor, []int{8080})
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	result, err := checker.CheckForConflicts(ctx)

	// CheckForConflicts handles errors gracefully and doesn't fail
	if err != nil {
		t.Errorf("CheckForConflicts() should not fail, but should handle errors gracefully: %v", err)
	}

	// With context cancellation, the port should be assumed to be in conflict (safe assumption)
	if len(result.ConflictPorts) == 0 {
		t.Error("CheckForConflicts() should assume port is in conflict when context is cancelled")
	}
}

// Helper functions

func getUnfilteredDockerPSCommand() string {
	return fmt.Sprintf("%s %s %s %s %s",
		constants.DockerCommand,
		constants.DockerCmdPS,
		constants.DockerFlagAll,
		constants.DockerFlagFormat,
		constants.DockerConflictFormat)
}

func equalIntSlices(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}
