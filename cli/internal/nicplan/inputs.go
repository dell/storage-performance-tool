package nicplan

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

// Auto is the --interfaces value that lets each engine choose its eligible NICs.
const Auto = "auto"

// Arguments reach a remote host through its shell, so entries and endpoints are limited to
// the characters of interface names, IPv4 and IPv6 addresses, RDMA devices and host names.
var (
	entryPattern    = regexp.MustCompile(`^[A-Za-z0-9_.:-]+$`)
	endpointPattern = regexp.MustCompile(`^[A-Za-z0-9_.:\[\]-]+$`)
)

// ParseInterfaces splits an --interfaces value into its entries; an empty value gives none, and
// auto must be the only entry.
// Resolving the entries, auto included, is the engine planner's work on each host.
func ParseInterfaces(value string) ([]string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	parts := strings.Split(value, ",")
	entries := make([]string, 0, len(parts))
	for _, part := range parts {
		entry := strings.TrimSpace(part)
		if entry == "" {
			return nil, fmt.Errorf("--interfaces %q has an empty entry", value)
		}
		if !entryPattern.MatchString(entry) {
			return nil, fmt.Errorf("--interfaces entry %q is not an interface name, IPv4 address or RDMA device", entry)
		}
		entries = append(entries, entry)
	}
	if len(entries) > 1 && slices.Contains(entries, Auto) {
		return nil, fmt.Errorf("--interfaces %q: %s must be the only entry", value, Auto)
	}
	return entries, nil
}

// ValidateEndpoints rejects an endpoint that cannot be passed to the command unquoted.
func ValidateEndpoints(endpoints []string) error {
	for _, endpoint := range endpoints {
		if !endpointPattern.MatchString(endpoint) {
			return fmt.Errorf("endpoint %q has characters verify cannot pass to the NIC planner", endpoint)
		}
	}
	return nil
}

// Args is the docker exec command line for the image command. Without interfaces it lists the
// inventory only; rdma adds the RDMA checks, DC support and the RoCE active MTU.
func Args(interfaces, endpoints []string, rdma bool) []string {
	args := []string{constants.SptNicPlanCommand}
	if rdma {
		args = append(args, "--rdma")
	}
	if len(interfaces) > 0 {
		args = append(args, "--interfaces", strings.Join(interfaces, ","))
		for _, endpoint := range endpoints {
			args = append(args, "--endpoint", endpoint)
		}
	}
	return args
}

// SliceEndpoints returns the endpoints of one slice, as the engine's
// ConfigSliceUtil.sliceStorageNodeAddrs assigns them under --slice-endpoints. With g the
// greatest common divisor of the slice and endpoint counts, endpoint group i of E/g goes to
// slices i·S/g through (i+1)·S/g − 1. Slice 0 is the entry node; one endpoint is never sliced.
func SliceEndpoints(endpoints []string, sliceCount, slice int) []string {
	if len(endpoints) <= 1 || sliceCount <= 1 || slice < 0 || slice >= sliceCount {
		return endpoints
	}
	g := gcd(sliceCount, len(endpoints))
	slicesPerGroup := sliceCount / g
	endpointsPerGroup := len(endpoints) / g
	group := slice / slicesPerGroup
	return endpoints[endpointsPerGroup*group : endpointsPerGroup*(group+1)]
}

func gcd(a, b int) int {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

// Permits is how a driver's concurrency limit P is divided among N NICs (item 8b).
type Permits struct {
	// Limit is P; zero or less is unlimited.
	Limit int
	// Shared means one limit for every NIC, each submit taking the next NIC in rotation:
	// with 0 < P < N, or unlimited.
	Shared bool
	// PerNic holds each NIC's permits in NIC-set order when the limit is divided.
	PerNic []int
}

// PermitShares divides limit among nics NICs: floor(P/N) each, the remainder to the first.
func PermitShares(limit, nics int) Permits {
	if nics <= 0 {
		return Permits{Limit: limit}
	}
	if limit <= 0 || limit < nics {
		return Permits{Limit: limit, Shared: true}
	}
	perNic := make([]int, nics)
	for i := range perNic {
		perNic[i] = limit / nics
		if i < limit%nics {
			perNic[i]++
		}
	}
	return Permits{Limit: limit, PerNic: perNic}
}

// PolicyRulesCommand lists the host's IPv4 routing policy rules.
var PolicyRulesCommand = []string{"ip", "-4", "rule", "show"}

// defaultPolicyRules are the kernel's rules for an IPv4 namespace.
var defaultPolicyRules = map[string]bool{
	"0: from all lookup local":       true,
	"32766: from all lookup main":    true,
	"32767: from all lookup default": true,
}

// NonDefaultPolicyRules returns the rules of PolicyRulesCommand's output other than the
// kernel's defaults, with their whitespace normalized. NIC choice follows connected subnets
// and the main table, so policy routing is outside the supported model (item 6b).
func NonDefaultPolicyRules(output string) []string {
	var rules []string
	for _, line := range strings.Split(output, "\n") {
		rule := strings.Join(strings.Fields(line), " ")
		if rule == "" || defaultPolicyRules[rule] {
			continue
		}
		rules = append(rules, rule)
	}
	return rules
}
