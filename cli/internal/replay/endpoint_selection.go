package replay

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

var (
	jsNetObjectRe      = regexp.MustCompile(`"net"\s*:\s*\{`)
	jsEndpointObjectRe = regexp.MustCompile(`"endpoint"\s*:\s*\{`)
)

// ArchivedEndpointSelection lists the distinct endpoint-selection declarations found in an
// archive. A declaration counts only where a config states the mode explicitly, "default"
// included. Its DNS timeout counts only for per-request DNS and its connect timeout only for a
// non-default mode, matching what the engine applies. Archived settings are never written into
// replayed steps: the effective settings go into the generated defaults only, after merging with
// the replay flags, so the validated settings are the ones that run.
type ArchivedEndpointSelection struct {
	Declarations []scenario.EndpointSelection
}

func (a *ArchivedEndpointSelection) add(mode string, dnsTimeoutMillis, connectTimeoutMillis int) {
	mode = strings.TrimSpace(mode)
	if mode == "" {
		return
	}
	declaration := scenario.EndpointSelection{Mode: mode}
	if mode == scenario.EndpointSelectionPerRequestDNS {
		declaration.DNSTimeoutMillis = dnsTimeoutMillis
	}
	if mode != scenario.EndpointSelectionDefault {
		declaration.ConnectTimeoutMillis = connectTimeoutMillis
	}
	for _, existing := range a.Declarations {
		if existing == declaration {
			return
		}
	}
	a.Declarations = append(a.Declarations, declaration)
}

func (a ArchivedEndpointSelection) modes() []string {
	seen := map[string]struct{}{}
	var modes []string
	for _, d := range a.Declarations {
		if _, dup := seen[d.Mode]; !dup {
			seen[d.Mode] = struct{}{}
			modes = append(modes, d.Mode)
		}
	}
	sort.Strings(modes)
	return modes
}

// timeouts returns the distinct nonzero values of one timeout among declarations of mode.
func (a ArchivedEndpointSelection) timeouts(mode string, value func(scenario.EndpointSelection) int) []int {
	seen := map[int]struct{}{}
	var values []int
	for _, d := range a.Declarations {
		v := value(d)
		if _, dup := seen[v]; d.Mode == mode && v != 0 && !dup {
			seen[v] = struct{}{}
			values = append(values, v)
		}
	}
	sort.Ints(values)
	return values
}

// archivedEndpointSelectionFromConfig records storage.net.endpoint from a merged JSON step config.
// Hostname and DNS server are environment-specific and are never read.
func archivedEndpointSelectionFromConfig(config map[string]any, vars map[string]string, archived *ArchivedEndpointSelection) {
	endpoint := []string{legacyKeyStorage, legacyKeyNet, legacyKeyEndpoint}
	archived.add(
		resolveString(getPath(config, append(endpoint, "selection")...), vars),
		intValue(getPath(config, append(endpoint, "dns", "timeoutMilliSec")...), vars),
		intValue(getPath(config, append(endpoint, "connect", "timeoutMilliSec")...), vars))
}

// extractJSEndpointSelections records every storage.net.endpoint object in a JavaScript scenario,
// whether in a parent config or an inline step config, and removes it so that the generated
// defaults alone carry endpoint selection.
func extractJSEndpointSelections(source string, vars map[string]string, archived *ArchivedEndpointSelection) (string, []Diagnostic) {
	var diagnostics []Diagnostic
	var replacements []jsReplacement
	for _, netMatch := range jsNetObjectRe.FindAllStringIndex(source, -1) {
		netOpen := netMatch[1] - 1
		netClose := findMatchingJSBrace(source, netOpen)
		if netClose < 0 {
			continue
		}
		keyMatch := jsEndpointObjectRe.FindStringIndex(source[netOpen : netClose+1])
		if keyMatch == nil {
			continue
		}
		endpointOpen := netOpen + keyMatch[1] - 1
		endpointClose := findMatchingJSBrace(source, endpointOpen)
		if endpointClose < 0 || endpointClose > netClose {
			continue
		}
		endpointText := source[endpointOpen : endpointClose+1]
		dnsText := jsObjectForKey(endpointText, "dns")
		connectText := jsObjectForKey(endpointText, "connect")
		archived.add(
			resolveString(jsFieldValue(endpointText, "selection", vars), vars),
			intValue(jsFieldValue(dnsText, "timeoutMilliSec", vars), vars),
			intValue(jsFieldValue(connectText, "timeoutMilliSec", vars), vars))
		var environmentSpecific []string
		if resolveString(jsFieldValue(endpointText, "hostname", vars), vars) != "" {
			environmentSpecific = append(environmentSpecific, "storage.net.endpoint.hostname")
		}
		if resolveString(jsFieldValue(dnsText, "server", vars), vars) != "" {
			environmentSpecific = append(environmentSpecific, "storage.net.endpoint.dns.server")
		}
		if len(environmentSpecific) > 0 {
			diagnostics = append(diagnostics, Diagnostic{Severity: severityWarning, Message: fmt.Sprintf(
				"archived scenario contains environment-specific endpoint selection setting(s) %s; replay uses --endpoint-hostname and --dns-server instead",
				strings.Join(environmentSpecific, ", "))})
		}
		start, end := widenToAdjacentComma(source, netOpen+keyMatch[0], endpointClose+1, netOpen, netClose)
		replacements = append(replacements, jsReplacement{start: start, end: end, text: ""})
	}
	return applyJSReplacements(source, replacements), diagnostics
}

// widenToAdjacentComma extends [start, end) over the comma that separates the entry from its
// neighbor, so removing the range leaves a valid object.
func widenToAdjacentComma(source string, start, end, objectOpen, objectClose int) (int, int) {
	after := skipJSWhitespace(source, end)
	if after < objectClose && source[after] == ',' {
		return start, after + 1
	}
	before := start - 1
	for before > objectOpen && isJSWhitespace(source[before]) {
		before--
	}
	if before > objectOpen && source[before] == ',' {
		return before, end
	}
	return start, end
}

// mergeEndpointSelection applies the replay flags over archived declarations, field by field.
// Explicit flags always win. Otherwise the archived mode applies, and archived timeouts apply only
// from declarations of the effective mode. A field whose archived values disagree and that the
// flags leave unset is an error naming the flag that resolves it. Hostname and DNS server come
// only from the flags.
func mergeEndpointSelection(flags scenario.EndpointSelection, archived ArchivedEndpointSelection) (scenario.EndpointSelection, []Diagnostic, error) {
	merged := flags
	var diagnostics []Diagnostic
	modes := archived.modes()
	switch {
	case len(modes) == 0:
	case merged.Mode != "":
		if len(modes) != 1 || modes[0] != merged.Mode {
			diagnostics = append(diagnostics, Diagnostic{Severity: severityWarning, Message: fmt.Sprintf(
				"--endpoint-selection %s overrides the archived endpoint selection %s", merged.Mode, strings.Join(modes, ", "))})
		}
	case len(modes) == 1:
		merged.Mode = modes[0]
		diagnostics = append(diagnostics, Diagnostic{Severity: severityWarning,
			Message: fmt.Sprintf("replay uses the archived endpoint selection %s", merged.Mode)})
	default:
		return merged, diagnostics, fmt.Errorf(
			"archived steps use different endpoint selection modes (%s); choose one with --endpoint-selection", strings.Join(modes, ", "))
	}
	if merged.Mode == scenario.EndpointSelectionPerRequestDNS && merged.DNSTimeoutMillis == 0 {
		value, err := archivedTimeout(archived.timeouts(merged.Mode, func(d scenario.EndpointSelection) int { return d.DNSTimeoutMillis }),
			"DNS timeouts", "--dns-timeout")
		if err != nil {
			return merged, diagnostics, err
		}
		merged.DNSTimeoutMillis = value
	}
	if merged.Active() && merged.ConnectTimeoutMillis == 0 {
		value, err := archivedTimeout(archived.timeouts(merged.Mode, func(d scenario.EndpointSelection) int { return d.ConnectTimeoutMillis }),
			"connect timeouts", "--endpoint-connect-timeout")
		if err != nil {
			return merged, diagnostics, err
		}
		merged.ConnectTimeoutMillis = value
	}
	return merged, diagnostics, nil
}

func archivedTimeout(values []int, what, flag string) (int, error) {
	switch len(values) {
	case 0:
		return 0, nil
	case 1:
		return values[0], nil
	default:
		return 0, fmt.Errorf("archived steps use different %s (%s ms); set %s", what, strings.Trim(fmt.Sprint(values), "[]"), flag)
	}
}
