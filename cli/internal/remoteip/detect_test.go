package remoteip

import (
	"context"
	"fmt"
	"os"
	"testing"

	cmdpkg "github.com/dell/storage-performance-tool/cli/internal/docker/command"
)

func TestDetectAdvertisedIP_PrimaryRoute(t *testing.T) {
	mock := cmdpkg.NewMockCommandExecutor()
	host := cmdpkg.CreateRemoteHost("worker1.example.com")

	// ip route get 1.1.1.1 -> src 10.10.1.2
	mock.SetCommandSuccess("ip -o route get 1.1.1.1", "1.1.1.1 via 10.0.0.1 dev eth0 src 10.10.1.2 cache\n")

	ip, err := DetectAdvertisedIP(context.Background(), mock, host)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ip != "10.10.1.2" {
		t.Fatalf("expected 10.10.1.2, got %q", ip)
	}

	// Ensure we used ip directly
	if !mock.HasExecutedCommand("ip -o route get 1.1.1.1") {
		t.Fatalf("expected ip route get to be executed")
	}
}

func TestDetectAdvertisedIP_FallbackInterfaceScan(t *testing.T) {
	mock := cmdpkg.NewMockCommandExecutor()
	host := cmdpkg.CreateRemoteHost("worker2.example.com")

	// Primary route yields no src -> force iface scan
	mock.SetCommandSuccess("ip -o route get 1.1.1.1", "1.1.1.1 via 10.0.0.1 dev eth0 cache\n")
	mock.SetCommandSuccess("ip -br -4 addr show scope global", "eth0    UP   192.168.50.20/24\nlo      UNKNOWN 127.0.0.1/8\n")

	ip, err := DetectAdvertisedIP(context.Background(), mock, host)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ip != "192.168.50.20" {
		t.Fatalf("expected 192.168.50.20, got %q", ip)
	}
}

func TestDetectAdvertisedIP_FallbackHostnameMethods(t *testing.T) {
	mock := cmdpkg.NewMockCommandExecutor()
	host := cmdpkg.CreateRemoteHost("worker3.example.com")

	mock.SetCommandSuccess("ip -o route get 1.1.1.1", "")
	mock.SetCommandSuccess("ip -br -4 addr show scope global", "")
	mock.SetCommandSuccess("hostname -I", "127.0.0.1 10.0.0.5   10.0.0.6\n")

	ip, err := DetectAdvertisedIP(context.Background(), mock, host)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ip != "10.0.0.5" {
		t.Fatalf("expected first hostname IP 10.0.0.5, got %q", ip)
	}
}

func TestDetectAdvertisedIP_AllFail(t *testing.T) {
	mock := cmdpkg.NewMockCommandExecutor()
	host := cmdpkg.CreateRemoteHost("worker4.example.com")

	mock.SetCommandSuccess("ip -o route get 1.1.1.1", " ")
	mock.SetCommandSuccess("ip -br -4 addr show scope global", " ")
	mock.SetCommandSuccess("hostname -I", " ")
	mock.SetCommandSuccess("hostname -f", "node.example.com\n")
	mock.SetCommandSuccess("getent hosts node.example.com", " ")
	// Fallback pipelines also fail
	mock.SetCommandSuccess("sh -lc "+routeCmd, " ")
	mock.SetCommandSuccess("sh -lc "+ifaceCmd, " ")
	mock.SetCommandSuccess("sh -lc "+hostIPCmd, " ")
	mock.SetCommandSuccess("sh -lc "+getentCmd, " ")

	if _, err := DetectAdvertisedIP(context.Background(), mock, host); err == nil {
		t.Fatalf("expected error when all detection methods fail")
	}
}

func TestFirstRoutableIPv4_FiltersLoopbackAndCIDR(t *testing.T) {
	cases := map[string]string{
		"192.168.0.10/24":      "192.168.0.10",
		"10.1.2.3\n":           "10.1.2.3",
		"127.0.0.1 10.1.2.3":   "10.1.2.3",
		"169.254.1.2 10.2.3.4": "10.2.3.4",
		"":                     "",
	}
	for in, want := range cases {
		if got := firstRoutableIPv4(in); got != want {
			t.Fatalf("input %q: want %q, got %q", in, want, got)
		}
	}
}

func TestDetectAdvertisedIP_OverrideFromEnv(t *testing.T) {
	mock := cmdpkg.NewMockCommandExecutor()
	host := cmdpkg.CreateRemoteHost("root@worker5.example.com")
	// hostparse normalizes Host field; Original keeps user@. Emulate that here.
	host.Original = "root@worker5.example.com"
	host.Host = "worker5.example.com"

	old := os.Getenv("ADVERTISED_IPS")
	_ = os.Setenv("ADVERTISED_IPS", "worker5.example.com=10.9.9.9,other=10.1.1.1")
	t.Cleanup(func() { _ = os.Setenv("ADVERTISED_IPS", old) })

	ip, err := DetectAdvertisedIP(context.Background(), mock, host)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ip != "10.9.9.9" {
		t.Fatalf("expected override 10.9.9.9, got %q", ip)
	}
}

func TestDetectAdvertisedIP_TriesAbsoluteIpPaths(t *testing.T) {
	mock := cmdpkg.NewMockCommandExecutor()
	host := cmdpkg.CreateRemoteHost("worker6.example.com")
	// Simulate plain "ip" not found, /sbin/ip works
	mock.SetCommandFailure("ip -o route get 1.1.1.1", "ip: command not found", fmt.Errorf("not found"))
	mock.SetCommandSuccess("/sbin/ip -o route get 1.1.1.1", "1.1.1.1 via 10.0.0.1 dev eth0 src 10.0.0.7 cache\n")

	ip, err := DetectAdvertisedIP(context.Background(), mock, host)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ip != "10.0.0.7" {
		t.Fatalf("expected 10.0.0.7, got %q", ip)
	}
	if !mock.HasExecutedCommand("/sbin/ip -o route get 1.1.1.1") {
		t.Fatalf("expected /sbin/ip to be attempted")
	}
}
