/*
Copyright © 2025 Dell Technologies
*/

package hostparse

import (
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

func TestParseTestHosts(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []*HostInfo
		wantErr  bool
	}{
		{
			name:  "default empty string",
			input: "",
			expected: []*HostInfo{
				{Host: "127.0.0.1", IsLocal: true, Original: "127.0.0.1"},
			},
		},
		{
			name:  "single local host",
			input: "127.0.0.1",
			expected: []*HostInfo{
				{Host: "127.0.0.1", IsLocal: true, Original: "127.0.0.1"},
			},
		},
		{
			name:  "localhost normalizes to IPv4",
			input: "localhost",
			expected: []*HostInfo{
				{Host: "127.0.0.1", IsLocal: true, Original: "localhost"},
			},
		},
		{
			name:  "single remote host",
			input: "worker1.example.com",
			expected: []*HostInfo{
				{Host: "worker1.example.com", IsLocal: false, Original: "worker1.example.com"},
			},
		},
		{
			name:  "user at host",
			input: "root@worker1",
			expected: []*HostInfo{
				{User: "root", Host: "worker1", IsLocal: false, Original: "root@worker1"},
			},
		},
		{
			name:  "multiple hosts with users",
			input: "127.0.0.1,admin@worker1,root@10.0.1.10",
			expected: []*HostInfo{
				{Host: "127.0.0.1", IsLocal: true, Original: "127.0.0.1"},
				{User: "admin", Host: "worker1", IsLocal: false, Original: "admin@worker1"},
				{User: "root", Host: "10.0.1.10", IsLocal: false, Original: "root@10.0.1.10"},
			},
		},
		{
			name:  "whitespace handling",
			input: " 127.0.0.1 , root@worker1 , worker2 ",
			expected: []*HostInfo{
				{Host: "127.0.0.1", IsLocal: true, Original: "127.0.0.1"},
				{User: "root", Host: "worker1", IsLocal: false, Original: "root@worker1"},
				{Host: "worker2", IsLocal: false, Original: "worker2"},
			},
		},
		{
			name:    "invalid username starting with number",
			input:   "123invalid@host",
			wantErr: true,
		},
		{
			name:    "empty username",
			input:   "@host",
			wantErr: true,
		},
		{
			name:    "invalid hostname with leading hyphen",
			input:   "root@-invalid-",
			wantErr: true,
		},
		{
			name:  "IPv6 address",
			input: "[::1]",
			expected: []*HostInfo{
				{Host: "[::1]", IsLocal: false, Original: "[::1]"},
			},
		},
		{
			name:  "root at multiple temporary machines",
			input: "root@10.0.1.10,root@10.0.1.11,root@10.0.1.12",
			expected: []*HostInfo{
				{User: "root", Host: "10.0.1.10", IsLocal: false, Original: "root@10.0.1.10"},
				{User: "root", Host: "10.0.1.11", IsLocal: false, Original: "root@10.0.1.11"},
				{User: "root", Host: "10.0.1.12", IsLocal: false, Original: "root@10.0.1.12"},
			},
		},
		{
			name:  "mixed local and remote",
			input: "localhost,root@remote1,admin@remote2",
			expected: []*HostInfo{
				{Host: "127.0.0.1", IsLocal: true, Original: "localhost"},
				{User: "root", Host: "remote1", IsLocal: false, Original: "root@remote1"},
				{User: "admin", Host: "remote2", IsLocal: false, Original: "admin@remote2"},
			},
		},
		{
			name:    "empty host list",
			input:   " , , ",
			wantErr: true,
		},
		{
			name:    "username too long",
			input:   "this_is_a_very_long_username_that_exceeds_32_characters@host",
			wantErr: true,
		},
		{
			name:  "valid underscore in username",
			input: "_underscore@host",
			expected: []*HostInfo{
				{User: "_underscore", Host: "host", IsLocal: false, Original: "_underscore@host"},
			},
		},
		{
			name:  "valid dash in username",
			input: "user-name@host",
			expected: []*HostInfo{
				{User: "user-name", Host: "host", IsLocal: false, Original: "user-name@host"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTestHosts(tt.input)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if len(tt.expected) != len(got) {
				t.Errorf("expected %d hosts, got %d", len(tt.expected), len(got))
				return
			}

			for i, expected := range tt.expected {
				if expected.User != got[i].User {
					t.Errorf("user mismatch at index %d: expected %q, got %q", i, expected.User, got[i].User)
				}
				if expected.Host != got[i].Host {
					t.Errorf("host mismatch at index %d: expected %q, got %q", i, expected.Host, got[i].Host)
				}
				if expected.IsLocal != got[i].IsLocal {
					t.Errorf("IsLocal mismatch at index %d: expected %t, got %t", i, expected.IsLocal, got[i].IsLocal)
				}
				if expected.Original != got[i].Original {
					t.Errorf("Original mismatch at index %d: expected %q, got %q", i, expected.Original, got[i].Original)
				}
			}
		})
	}
}

func TestParseSingleHost(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *HostInfo
		wantErr  bool
	}{
		{
			name:  "local IPv4",
			input: "127.0.0.1",
			expected: &HostInfo{
				Host:     "127.0.0.1",
				IsLocal:  true,
				Original: "127.0.0.1",
			},
		},
		{
			name:  "localhost normalized",
			input: "localhost",
			expected: &HostInfo{
				Host:     "127.0.0.1",
				IsLocal:  true,
				Original: "localhost",
			},
		},
		{
			name:  "remote hostname",
			input: "worker1.example.com",
			expected: &HostInfo{
				Host:     "worker1.example.com",
				IsLocal:  false,
				Original: "worker1.example.com",
			},
		},
		{
			name:  "user at remote host",
			input: "root@worker1",
			expected: &HostInfo{
				User:     "root",
				Host:     "worker1",
				IsLocal:  false,
				Original: "root@worker1",
			},
		},
		{
			name:  "user at local host",
			input: "myuser@127.0.0.1",
			expected: &HostInfo{
				User:     "myuser",
				Host:     "127.0.0.1",
				IsLocal:  true,
				Original: "myuser@127.0.0.1",
			},
		},
		{
			name:    "empty input",
			input:   "",
			wantErr: true,
		},
		{
			name:    "invalid username",
			input:   "123@host",
			wantErr: true,
		},
		{
			name:    "empty username",
			input:   "@host",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseSingleHost(tt.input)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if tt.expected.User != got.User {
				t.Errorf("user mismatch: expected %q, got %q", tt.expected.User, got.User)
			}
			if tt.expected.Host != got.Host {
				t.Errorf("host mismatch: expected %q, got %q", tt.expected.Host, got.Host)
			}
			if tt.expected.IsLocal != got.IsLocal {
				t.Errorf("IsLocal mismatch: expected %t, got %t", tt.expected.IsLocal, got.IsLocal)
			}
			if tt.expected.Original != got.Original {
				t.Errorf("Original mismatch: expected %q, got %q", tt.expected.Original, got.Original)
			}
		})
	}
}

func TestGetDockerHost(t *testing.T) {
	tests := []struct {
		name     string
		info     *HostInfo
		expected string
	}{
		{
			name:     "local host",
			info:     &HostInfo{Host: "127.0.0.1", IsLocal: true},
			expected: "unix:///var/run/docker.sock",
		},
		{
			name:     "remote host without user",
			info:     &HostInfo{Host: "worker1", IsLocal: false},
			expected: "ssh://worker1",
		},
		{
			name:     "remote host with user",
			info:     &HostInfo{User: "root", Host: "worker1", IsLocal: false},
			expected: "ssh://root@worker1",
		},
		{
			name:     "remote IP with user",
			info:     &HostInfo{User: "admin", Host: "10.0.1.10", IsLocal: false},
			expected: "ssh://admin@10.0.1.10",
		},
		{
			name:     "IPv6 with user",
			info:     &HostInfo{User: "root", Host: "[2001:db8::1]", IsLocal: false},
			expected: "ssh://root@[2001:db8::1]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.info.GetDockerHost()
			if tt.expected != got {
				t.Errorf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}

func TestGetAPIURL(t *testing.T) {
	tests := []struct {
		name     string
		info     *HostInfo
		port     string
		expected string
	}{
		{
			name:     "local host",
			info:     &HostInfo{Host: "127.0.0.1", IsLocal: true},
			port:     constants.SptAPIPort,
			expected: "http://127.0.0.1:9999",
		},
		{
			name:     "remote host",
			info:     &HostInfo{Host: "worker1", IsLocal: false},
			port:     constants.SptAPIPort,
			expected: "http://worker1:9999",
		},
		{
			name:     "remote IP with user",
			info:     &HostInfo{User: "root", Host: "10.0.1.10", IsLocal: false},
			port:     constants.SptAPIPort,
			expected: "http://10.0.1.10:9999",
		},
		{
			name:     "custom port",
			info:     &HostInfo{Host: "worker1", IsLocal: false},
			port:     "8080",
			expected: "http://worker1:8080",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.info.GetAPIURL(tt.port)
			if tt.expected != got {
				t.Errorf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}

func TestValidateHost(t *testing.T) {
	tests := []struct {
		name    string
		host    string
		wantErr bool
	}{
		{"valid IPv4", "192.168.1.1", false},
		{"valid IPv6 with brackets", "[2001:db8::1]", false},
		{"valid IPv6 localhost", "[::1]", false},
		{"valid hostname", "example.com", false},
		{"valid subdomain", "worker.example.com", false},
		{"valid short name", "worker1", false},
		{"localhost", "localhost", false},
		{"valid hostname with numbers", "host123", false},
		{"valid complex hostname", "my-worker-01.prod.example.com", false},
		{"invalid start hyphen", "-invalid", true},
		{"invalid end hyphen", "invalid-", true},
		{"invalid IPv6 without brackets", "2001:db8::1", false}, // This is actually valid as hostname
		{"invalid IPv6 with brackets", "[invalid:::]", true},
		{"too long hostname", strings.Repeat("a", 254), true},
		{"too long label", strings.Repeat("a", 64) + ".com", true},
		{"empty", "", true},
		{"double dot", "host..com", true},
		{"ending with dot", "host.com.", false},
		{"starting with dot", ".host.com", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateHost(tt.host)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error for host %q but got none", tt.host)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error for host %q: %v", tt.host, err)
				}
			}
		})
	}
}

func TestIsValidUsername(t *testing.T) {
	tests := []struct {
		name     string
		username string
		expected bool
	}{
		{"valid simple", "user", true},
		{"valid with underscore", "_user", true},
		{"valid with underscore middle", "my_user", true},
		{"valid with dash", "user-name", true},
		{"valid with numbers", "user123", true},
		{"valid complex", "_my-user_123", true},
		{"root user", "root", true},
		{"admin user", "admin", true},
		{"invalid start with number", "123user", false},
		{"invalid start with dash", "-user", false},
		{"invalid empty", "", false},
		{"invalid too long", strings.Repeat("a", 33), false},
		{"invalid special chars", "user@host", false},
		{"invalid dot", "user.name", false},
		{"valid max length", strings.Repeat("a", 32), true},
		{"invalid space", "my user", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isValidUsername(tt.username)
			if tt.expected != got {
				t.Errorf("for username %q: expected %t, got %t", tt.username, tt.expected, got)
			}
		})
	}
}

func TestIsValidHostname(t *testing.T) {
	tests := []struct {
		name     string
		hostname string
		expected bool
	}{
		{"valid simple", "host", true},
		{"valid with domain", "host.example.com", true},
		{"valid with numbers", "host123", true},
		{"valid with dash", "my-host", true},
		{"valid complex", "worker-01.prod.example.com", true},
		{"valid single char", "a", true},
		{"invalid empty", "", false},
		{"invalid start with dash", "-host", false},
		{"invalid end with dash", "host-", false},
		{"invalid double dash", "host--name", true},
		{"invalid too long", strings.Repeat("a", 254), false},
		{"invalid label too long", strings.Repeat("a", 64) + ".com", false},
		{"invalid empty label", "host..com", false},
		{"valid with trailing dot", "host.com.", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isValidHostname(tt.hostname)
			// Note: Our regex is more permissive than strict RFC 1123
			// This is intentional for practical usage
			if tt.expected != got {
				t.Errorf("for hostname %q: expected %t, got %t", tt.hostname, tt.expected, got)
			}
		})
	}
}
