/*
Copyright © 2025 Dell Technologies
*/

package hostparse

import (
	"fmt"
	"net"
	"regexp"
	"strings"
)

const loopbackIPv4 = "127.0.0.1"

// HostInfo represents a parsed host specification
type HostInfo struct {
	User     string // SSH user (empty for local or current user)
	Host     string // Hostname or IP address
	IsLocal  bool   // True if this is a local connection
	Original string // Original string for error messages
}

// ParseTestHosts parses and validates a comma-separated host list
func ParseTestHosts(hostString string) ([]*HostInfo, error) {
	if hostString == "" {
		return []*HostInfo{{
			Host:     loopbackIPv4,
			IsLocal:  true,
			Original: loopbackIPv4,
		}}, nil
	}

	hosts := strings.Split(hostString, ",")
	parsed := make([]*HostInfo, 0, len(hosts))

	for _, hostSpec := range hosts {
		hostSpec = strings.TrimSpace(hostSpec)
		if hostSpec == "" {
			continue
		}

		info, err := ParseSingleHost(hostSpec)
		if err != nil {
			return nil, fmt.Errorf("invalid host specification %q: %w", hostSpec, err)
		}

		parsed = append(parsed, info)
	}

	if len(parsed) == 0 {
		return nil, fmt.Errorf("no valid hosts provided")
	}

	return parsed, nil
}

// ParseSingleHost parses a single host specification
func ParseSingleHost(hostSpec string) (*HostInfo, error) {
	info := &HostInfo{
		Original: hostSpec,
	}

	// Check for user@host format
	if idx := strings.LastIndex(hostSpec, "@"); idx != -1 {
		info.User = hostSpec[:idx]
		info.Host = hostSpec[idx+1:]

		// Validate user
		if info.User == "" {
			return nil, fmt.Errorf("empty user in user@host format")
		}
		if !isValidUsername(info.User) {
			return nil, fmt.Errorf("invalid username: %s", info.User)
		}
	} else {
		info.Host = hostSpec
	}

	// Check if local
	if info.Host == loopbackIPv4 || info.Host == "localhost" {
		info.IsLocal = true
		info.Host = loopbackIPv4 // Normalize to IPv4
	}

	// Validate host
	if err := ValidateHost(info.Host); err != nil {
		return nil, err
	}

	return info, nil
}

// ValidateHost checks if a host is a valid IP or hostname
func ValidateHost(host string) error {
	// Check for IPv6 with brackets
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		ipv6 := host[1 : len(host)-1]
		if net.ParseIP(ipv6) == nil {
			return fmt.Errorf("invalid IPv6 address")
		}
		return nil
	}

	// Check for IPv4
	if net.ParseIP(host) != nil {
		return nil
	}

	// Validate as hostname (RFC 1123)
	if !isValidHostname(host) {
		return fmt.Errorf("invalid hostname format")
	}

	return nil
}

// isValidUsername checks if a username is valid
func isValidUsername(user string) bool {
	// Unix username rules: alphanumeric, underscore, dash
	// First character must be letter or underscore
	match, _ := regexp.MatchString(`^[a-zA-Z_][a-zA-Z0-9_-]*$`, user)
	return match && len(user) <= 32
}

// isValidHostname checks if a hostname is valid per RFC 1123
func isValidHostname(host string) bool {
	if len(host) == 0 || len(host) > 253 {
		return false
	}

	// Strip single trailing dot (FQDN format)
	host = strings.TrimSuffix(host, ".")

	// Check each label
	labels := strings.Split(host, ".")
	for _, label := range labels {
		if len(label) == 0 || len(label) > 63 {
			return false
		}
		// Must start and end with alphanumeric
		if !regexp.MustCompile(`^[a-zA-Z0-9].*[a-zA-Z0-9]$|^[a-zA-Z0-9]$`).MatchString(label) {
			return false
		}
		// Can contain alphanumeric and hyphens
		if !regexp.MustCompile(`^[a-zA-Z0-9-]+$`).MatchString(label) {
			return false
		}
	}

	return true
}

// GetDockerHost returns the Docker connection string for a host
func (info *HostInfo) GetDockerHost() string {
	if info.IsLocal {
		return "unix:///var/run/docker.sock"
	}

	if info.User != "" {
		return fmt.Sprintf("ssh://%s@%s", info.User, info.Host)
	}

	return fmt.Sprintf("ssh://%s", info.Host)
}

// GetSSHTarget returns the SSH target for running remote commands
func (info *HostInfo) GetSSHTarget() string {
	if info.User != "" {
		return fmt.Sprintf("%s@%s", info.User, info.Host)
	}
	return info.Host
}

// GetAPIURL returns the Spt API URL for a host
func (info *HostInfo) GetAPIURL(port string) string {
	// Always use the host address for API connections
	// SSH tunnel will handle remote connections
	return fmt.Sprintf("http://%s:%s", info.Host, port)
}
