package scenario

import (
	"fmt"
	"math"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/workload"
)

// Endpoint-selection modes accepted by --endpoint-selection and storage.net.endpoint.selection.
const (
	EndpointSelectionDefault       = "default"
	EndpointSelectionRoundRobin    = "round-robin"
	EndpointSelectionPerRequestDNS = "per-request-dns"

	maxDNSLabelLength = 63
	maxDNSNameLength  = 253
	maxPort           = 65535
)

// EndpointSelection holds the opt-in endpoint-selection settings. The zero value is the default
// mode. Durations are whole milliseconds; zero means "use the engine default".
type EndpointSelection struct {
	Mode                 string
	Hostname             string
	DNSServer            string
	DNSTimeoutMillis     int
	ConnectTimeoutMillis int
}

// Active reports whether a non-default mode is selected.
func (e EndpointSelection) Active() bool {
	return e.Mode != "" && e.Mode != EndpointSelectionDefault
}

// EndpointSelectionTarget is the rest of the run configuration that endpoint selection is
// validated against. RawEndpoints are the endpoint URLs as given, before deduplication.
type EndpointSelectionTarget struct {
	RawEndpoints   []string
	S3Driver       string
	SliceEndpoints bool
	RangeRead      bool
	WorkloadType   string
}

// DurationMillis converts an explicit duration flag to whole milliseconds within the engine's
// int range, rejecting zero, negative, fractional-millisecond and overflowing values.
func DurationMillis(flag string, d time.Duration) (int, error) {
	if d <= 0 {
		return 0, fmt.Errorf("--%s must be positive, got %s", flag, d)
	}
	if d%time.Millisecond != 0 {
		return 0, fmt.Errorf("--%s must be a whole number of milliseconds, got %s", flag, d)
	}
	millis := d / time.Millisecond
	if millis > math.MaxInt32 {
		return 0, fmt.Errorf("--%s must be at most %d ms, got %s", flag, math.MaxInt32, d)
	}
	return int(millis), nil
}

// ValidateEndpointSelection checks the settings against the endpoints and workload. Errors name
// the CLI flags. The engine repeats equivalent checks for direct engine configuration.
func ValidateEndpointSelection(sel EndpointSelection, target EndpointSelectionTarget) error {
	switch sel.Mode {
	case "", EndpointSelectionDefault:
		if sel.Hostname != "" || sel.DNSServer != "" || sel.DNSTimeoutMillis != 0 || sel.ConnectTimeoutMillis != 0 {
			return fmt.Errorf("--endpoint-hostname, --dns-server, --dns-timeout and --endpoint-connect-timeout require --endpoint-selection %s or %s",
				EndpointSelectionRoundRobin, EndpointSelectionPerRequestDNS)
		}
		return nil
	case EndpointSelectionRoundRobin, EndpointSelectionPerRequestDNS:
	default:
		return fmt.Errorf("--endpoint-selection must be %s, %s or %s, got %q",
			EndpointSelectionDefault, EndpointSelectionRoundRobin, EndpointSelectionPerRequestDNS, sel.Mode)
	}
	if target.WorkloadType == workload.Tables {
		return fmt.Errorf("--endpoint-selection %s does not support the tables workload", sel.Mode)
	}
	if driver := resolveStorageDriverType(target.S3Driver); driver != storageDriverTypeS3 {
		return fmt.Errorf("--endpoint-selection %s requires the default Netty S3 driver, not --s3-driver %s", sel.Mode, target.S3Driver)
	}
	if target.SliceEndpoints {
		return fmt.Errorf("--endpoint-selection %s cannot be combined with --slice-endpoints", sel.Mode)
	}
	if target.RangeRead {
		return fmt.Errorf("--endpoint-selection %s does not support partial-object reads yet", sel.Mode)
	}
	endpoints, err := parseSelectionEndpoints(target.RawEndpoints)
	if err != nil {
		return err
	}
	if sel.Mode == EndpointSelectionRoundRobin {
		return validateRoundRobin(sel, endpoints)
	}
	return validatePerRequestDNS(sel, endpoints)
}

type selectionEndpoint struct {
	raw  string
	host string
	port int
}

func parseSelectionEndpoints(raw []string) ([]selectionEndpoint, error) {
	endpoints := make([]selectionEndpoint, 0, len(raw))
	for _, value := range raw {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		u, err := url.Parse(value)
		if err != nil || u.Hostname() == "" {
			return nil, fmt.Errorf("invalid endpoint URL %q", value)
		}
		portStr := u.Port()
		if portStr == "" {
			portStr = constants.DefaultHTTPPort
			if u.Scheme == schemeHTTPS {
				portStr = constants.DefaultHTTPSPort
			}
		}
		port, err := strconv.Atoi(portStr)
		if err != nil || port < 1 || port > maxPort {
			return nil, fmt.Errorf("invalid port in endpoint %q", value)
		}
		endpoints = append(endpoints, selectionEndpoint{raw: value, host: strings.ToLower(u.Hostname()), port: port})
	}
	return endpoints, nil
}

func validateRoundRobin(sel EndpointSelection, endpoints []selectionEndpoint) error {
	if sel.DNSServer != "" || sel.DNSTimeoutMillis != 0 {
		return fmt.Errorf("--dns-server and --dns-timeout require --endpoint-selection %s", EndpointSelectionPerRequestDNS)
	}
	if len(endpoints) == 0 {
		return fmt.Errorf("--endpoint-selection %s requires at least one endpoint", EndpointSelectionRoundRobin)
	}
	seen := make(map[string]string, len(endpoints))
	for _, ep := range endpoints {
		if !isIPv4Literal(ep.host) {
			return fmt.Errorf("--endpoint-selection %s requires IPv4 endpoint addresses; use --endpoint-hostname for the hostname, got %q",
				EndpointSelectionRoundRobin, ep.raw)
		}
		key := net.JoinHostPort(ep.host, strconv.Itoa(ep.port))
		if first, dup := seen[key]; dup {
			return fmt.Errorf("duplicate round-robin endpoint %q (same address as %q)", ep.raw, first)
		}
		seen[key] = ep.raw
	}
	if sel.Hostname != "" {
		return validateDNSName("--endpoint-hostname", sel.Hostname)
	}
	return nil
}

func validatePerRequestDNS(sel EndpointSelection, endpoints []selectionEndpoint) error {
	if sel.Hostname != "" {
		return fmt.Errorf("--endpoint-hostname applies only to --endpoint-selection %s; per-request DNS uses the endpoint hostname",
			EndpointSelectionRoundRobin)
	}
	if len(endpoints) != 1 {
		return fmt.Errorf("--endpoint-selection %s requires exactly one endpoint hostname, got %d",
			EndpointSelectionPerRequestDNS, len(endpoints))
	}
	if err := validateDNSName("the per-request DNS endpoint", endpoints[0].host); err != nil {
		return err
	}
	if sel.DNSServer != "" {
		return validateDNSServer(sel.DNSServer)
	}
	return nil
}

func validateDNSServer(server string) error {
	host, port, hasPort := strings.Cut(server, ":")
	if !isIPv4Literal(host) {
		return fmt.Errorf("--dns-server must be an IPv4 address with an optional port, got %q", server)
	}
	if hasPort {
		if p, err := strconv.Atoi(port); err != nil || p < 1 || p > maxPort {
			return fmt.Errorf("--dns-server has an invalid port: %q", server)
		}
	}
	return nil
}

func validateDNSName(what, name string) error {
	trimmed := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(name)), ".")
	if net.ParseIP(trimmed) != nil {
		return fmt.Errorf("%s must be a hostname, not an IP address: %q", what, name)
	}
	if trimmed == "" || len(trimmed) > maxDNSNameLength {
		return fmt.Errorf("%s is not a valid hostname: %q", what, name)
	}
	for _, label := range strings.Split(trimmed, ".") {
		if !isDNSLabel(label) {
			return fmt.Errorf("%s is not a valid hostname: %q", what, name)
		}
	}
	return nil
}

func isDNSLabel(label string) bool {
	if label == "" || len(label) > maxDNSLabelLength || strings.HasPrefix(label, "-") || strings.HasSuffix(label, "-") {
		return false
	}
	for _, c := range label {
		isLower := c >= 'a' && c <= 'z'
		isDigit := c >= '0' && c <= '9'
		if !isLower && !isDigit && c != '-' {
			return false
		}
	}
	return true
}

func isIPv4Literal(host string) bool {
	ip := net.ParseIP(host)
	return ip != nil && ip.To4() != nil && !strings.Contains(host, ":")
}

// endpointConfig builds the storage.net.endpoint block, or nil in the default mode so the
// generated defaults stay unchanged.
func endpointConfig(sel EndpointSelection) *EndpointConfig {
	if !sel.Active() {
		return nil
	}
	cfg := &EndpointConfig{Selection: sel.Mode, Hostname: strings.TrimSpace(sel.Hostname)}
	if sel.DNSServer != "" || sel.DNSTimeoutMillis != 0 {
		cfg.DNS = &EndpointDNSConfig{Server: strings.TrimSpace(sel.DNSServer), TimeoutMilliSec: sel.DNSTimeoutMillis}
	}
	if sel.ConnectTimeoutMillis != 0 {
		cfg.Connect = &EndpointConnectConfig{TimeoutMilliSec: sel.ConnectTimeoutMillis}
	}
	return cfg
}
