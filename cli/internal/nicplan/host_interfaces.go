package nicplan

import (
	"fmt"
	"slices"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

// None is the --host-interfaces value that turns a host's NICs off. It is only valid there, alone.
const None = "none"

// HostInterfaces is one host's --host-interfaces value.
type HostInterfaces struct {
	// Host is the host part of a --test-hosts entry, without the user.
	Host string
	// Value is the value as given, without surrounding space: None, auto or a list.
	Value string
	// Entries are the value's entries; none for None.
	Entries []string
}

// ParseHostInterfaces parses --host-interfaces <host>=<value> values against the --test-hosts hosts.
func ParseHostInterfaces(values, hosts []string) ([]HostInterfaces, error) {
	parsed := make([]HostInterfaces, 0, len(values))
	seen := make([]string, 0, len(values))
	for _, raw := range values {
		given, value, found := strings.Cut(strings.TrimSpace(raw), "=")
		given, value = strings.TrimSpace(given), strings.TrimSpace(value)
		if !found || given == "" {
			return nil, fmt.Errorf("--host-interfaces %q is not <host>=<value>", raw)
		}
		if value == "" {
			return nil, fmt.Errorf("--host-interfaces %s= has an empty value", given)
		}
		info, err := hostparse.ParseSingleHost(given)
		if err != nil {
			return nil, fmt.Errorf("--host-interfaces %s=%s: %w", given, value, err)
		}
		if !slices.Contains(hosts, info.Host) {
			return nil, fmt.Errorf("--host-interfaces %s=%s: %s is not in --test-hosts", given, value, given)
		}
		if slices.Contains(seen, info.Host) {
			return nil, fmt.Errorf("--host-interfaces names %s twice", given)
		}
		seen = append(seen, info.Host)
		entry := HostInterfaces{Host: info.Host, Value: value}
		if value != None {
			entry.Entries, err = ParseInterfaces(value)
			if err != nil {
				return nil, fmt.Errorf("--host-interfaces %s=%s: %w", given, value, err)
			}
		}
		parsed = append(parsed, entry)
	}
	return parsed, nil
}

// ForHost returns the host's value, if it has one.
func ForHost(overrides []HostInterfaces, host string) (HostInterfaces, bool) {
	for _, override := range overrides {
		if override.Host == host {
			return override, true
		}
	}
	return HostInterfaces{}, false
}

// AnyNicSet returns the entries of the first value that asks for NICs, none when every value is None.
func AnyNicSet(overrides []HostInterfaces) []string {
	for _, override := range overrides {
		if len(override.Entries) > 0 {
			return override.Entries
		}
	}
	return nil
}
