package replay

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

var (
	jsNetObjectRe      = regexp.MustCompile(`"net"\s*:\s*\{`)
	jsEndpointObjectRe = regexp.MustCompile(`"endpoint"\s*:\s*\{`)
	jsUnquotedKeyRe    = regexp.MustCompile(`[{,]\s*[A-Za-z_$][A-Za-z0-9_$]*\s*:`)
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

// extractJSEndpointSelections records every storage.net.endpoint declaration in a JavaScript
// scenario, whether in a parent config or an inline step config, and removes it so that the
// generated defaults alone carry endpoint selection. Comments are ignored. Replay only evaluates
// literal declarations: an endpoint object directly inside a literal "net" object, with an explicit
// mode and literal or exported-variable values. Any other endpoint declaration, and any partial
// one that would inherit settings from another config, is rejected rather than left to override
// the validated defaults.
func extractJSEndpointSelections(source string, vars map[string]string, archived *ArchivedEndpointSelection) (string, []Diagnostic) {
	code := jsCodeMask(source)
	var diagnostics []Diagnostic
	var replacements []jsReplacement
	handled := map[int]struct{}{}
	for _, netMatch := range jsNetObjectRe.FindAllStringIndex(code, -1) {
		netOpen := netMatch[1] - 1
		netClose := findMatchingJSBrace(code, netOpen)
		if netClose < 0 {
			continue
		}
		keyMatch := jsEndpointObjectRe.FindStringIndex(code[netOpen : netClose+1])
		if keyMatch == nil {
			continue
		}
		keyStart := netOpen + keyMatch[0]
		endpointOpen := netOpen + keyMatch[1] - 1
		endpointClose := findMatchingJSBrace(code, endpointOpen)
		if endpointClose < 0 || endpointClose > netClose {
			continue
		}
		handled[keyStart] = struct{}{}
		declaration, err := parseJSEndpointObject(code[endpointOpen:endpointClose+1], vars)
		if err != nil {
			diagnostics = append(diagnostics, endpointSelectionError("archived scenario "+err.Error()))
			continue
		}
		archived.add(declaration.mode, declaration.dnsTimeoutMillis, declaration.connectTimeoutMillis)
		if len(declaration.environmentSpecific) > 0 {
			diagnostics = append(diagnostics, Diagnostic{Severity: severityWarning, Message: fmt.Sprintf(
				"archived scenario contains environment-specific endpoint selection setting(s) %s; replay uses --endpoint-hostname and --dns-server instead",
				strings.Join(declaration.environmentSpecific, ", "))})
		}
		start, end := widenToAdjacentComma(code, keyStart, endpointClose+1, netOpen, netClose)
		replacements = append(replacements, jsReplacement{start: start, end: end, text: ""})
	}
	for _, position := range jsEndpointKeyPositions(code) {
		if _, ok := handled[position]; !ok {
			diagnostics = append(diagnostics, endpointSelectionError("archived scenario declares storage.net.endpoint in a form replay "+
				"cannot evaluate, such as a variable reference or an unquoted key; write it as a literal object inside a literal "+
				"\"net\" object, or remove it"))
		}
	}
	return applyJSReplacements(source, replacements), diagnostics
}

type jsEndpointDeclaration struct {
	mode                 string
	dnsTimeoutMillis     int
	connectTimeoutMillis int
	environmentSpecific  []string
}

// JavaScript boolean literals accepted as endpoint setting values.
const (
	jsTrue  = "true"
	jsFalse = "false"
)

var jsEndpointKeys = map[string]struct{}{
	"selection": {}, "hostname": {}, "dns": {}, "connect": {}, "server": {}, "timeoutMilliSec": {},
}

// parseJSEndpointObject reads a literal endpoint object, rejecting anything replay cannot evaluate.
func parseJSEndpointObject(text string, vars map[string]string) (jsEndpointDeclaration, error) {
	var declaration jsEndpointDeclaration
	if jsUnquotedKeyRe.MatchString(jsBlankStrings(text)) {
		return declaration, fmt.Errorf("declares storage.net.endpoint with an unquoted key, which replay cannot evaluate")
	}
	for _, key := range jsObjectKeyRe.FindAllStringSubmatch(text, -1) {
		if _, ok := jsEndpointKeys[key[1]]; !ok {
			return declaration, fmt.Errorf("declares an unsupported storage.net.endpoint setting %q", key[1])
		}
	}
	mode, _, err := jsLiteralValue(text, "selection", vars)
	if err != nil {
		return declaration, err
	}
	if mode == "" {
		return declaration, fmt.Errorf("declares storage.net.endpoint without a selection mode; replay cannot resolve " +
			"endpoint settings inherited from another config, so state the mode in every endpoint object")
	}
	declaration.mode = mode
	if hostname, _, err := jsLiteralValue(text, "hostname", vars); err != nil {
		return declaration, err
	} else if hostname != "" {
		declaration.environmentSpecific = append(declaration.environmentSpecific, "storage.net.endpoint.hostname")
	}
	sections := map[string]string{}
	for _, section := range []string{"dns", "connect"} {
		raw, present := jsFieldRawValue(text, section)
		if present && !strings.HasPrefix(raw, "{") {
			return declaration, fmt.Errorf("sets storage.net.endpoint.%s to %s, which is not a literal object", section, raw)
		}
		sections[section] = jsObjectForKey(text, section)
	}
	if server, _, err := jsLiteralValue(sections["dns"], "server", vars); err != nil {
		return declaration, err
	} else if server != "" {
		declaration.environmentSpecific = append(declaration.environmentSpecific, "storage.net.endpoint.dns.server")
	}
	if declaration.dnsTimeoutMillis, err = jsLiteralInt(sections["dns"], "timeoutMilliSec", vars); err != nil {
		return declaration, err
	}
	if declaration.connectTimeoutMillis, err = jsLiteralInt(sections["connect"], "timeoutMilliSec", vars); err != nil {
		return declaration, err
	}
	return declaration, nil
}

// jsLiteralValue returns a field written as a single literal token: a string (with export
// placeholders expanded), a number, a boolean, or the name of an exported variable.
func jsLiteralValue(text, name string, vars map[string]string) (string, bool, error) {
	if text == "" {
		return "", false, nil
	}
	raw, present := jsFieldRawValue(text, name)
	if !present {
		return "", false, nil
	}
	token, ok := jsFieldToken(text, name)
	if !ok || token != strings.TrimSpace(raw) {
		return "", true, fmt.Errorf("sets storage.net.endpoint %s to an expression replay cannot evaluate: %s", name, raw)
	}
	switch {
	case strings.HasPrefix(token, `"`):
		return expandWithExports(unquoteJSString(strings.Trim(token, `"`)), vars), true, nil
	case token == jsTrue || token == jsFalse || (token[0] >= '0' && token[0] <= '9') || token[0] == '-':
		return token, true, nil
	}
	if value, ok := vars[token]; ok {
		return value, true, nil
	}
	return "", true, fmt.Errorf("sets storage.net.endpoint %s from the variable %s, which replay cannot evaluate", name, token)
}

func jsLiteralInt(text, name string, vars map[string]string) (int, error) {
	value, present, err := jsLiteralValue(text, name, vars)
	if err != nil || !present {
		return 0, err
	}
	number, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return 0, fmt.Errorf("sets storage.net.endpoint %s to %q, which is not a whole number of milliseconds", name, value)
	}
	return number, nil
}

func endpointSelectionError(message string) Diagnostic {
	return Diagnostic{Severity: severityError, Code: failureInvalidEndpointSelection, Message: message}
}

// jsEndpointKeyPositions finds every object key named endpoint in code, quoted or not, ignoring
// string contents. code must already have its comments masked.
func jsEndpointKeyPositions(code string) []int {
	var positions []int
	followedByColon := func(i int) bool {
		j := skipJSWhitespace(code, i)
		return j < len(code) && code[j] == ':'
	}
	for i := 0; i < len(code); i++ {
		c := code[i]
		switch {
		case c == '"' || c == '\'' || c == '`':
			end := jsStringEnd(code, i)
			if end > i && code[i+1:end] == "endpoint" && followedByColon(end+1) {
				positions = append(positions, i)
			}
			i = end
		case isJSIdentifierStart(c):
			start := i
			for i+1 < len(code) && isJSIdentifierPart(code[i+1]) {
				i++
			}
			if code[start:i+1] == "endpoint" && followedByColon(i+1) {
				positions = append(positions, start)
			}
		}
	}
	return positions
}

// jsCodeMask returns source with comment text replaced by spaces (line breaks kept), so scans
// see only executable code at unchanged offsets.
func jsCodeMask(source string) string {
	out := []byte(source)
	for i := 0; i < len(out); i++ {
		switch {
		case out[i] == '"' || out[i] == '\'' || out[i] == '`':
			i = jsStringEnd(source, i)
		case out[i] == '/' && i+1 < len(out) && out[i+1] == '/':
			for ; i < len(out) && out[i] != '\n'; i++ {
				out[i] = ' '
			}
		case out[i] == '/' && i+1 < len(out) && out[i+1] == '*':
			end := strings.Index(source[i+2:], "*/")
			stop := len(out)
			if end >= 0 {
				stop = i + 2 + end + 2
			}
			for j := i; j < stop; j++ {
				if out[j] != '\n' {
					out[j] = ' '
				}
			}
			i = stop - 1
		}
	}
	return string(out)
}

// jsBlankStrings replaces string contents with spaces, keeping the quotes and offsets.
func jsBlankStrings(source string) string {
	out := []byte(source)
	for i := 0; i < len(out); i++ {
		if out[i] == '"' || out[i] == '\'' || out[i] == '`' {
			end := jsStringEnd(source, i)
			for j := i + 1; j < end; j++ {
				out[j] = ' '
			}
			i = end
		}
	}
	return string(out)
}

// jsStringEnd returns the index of the quote closing the string that opens at open.
func jsStringEnd(source string, open int) int {
	quote := source[open]
	for i := open + 1; i < len(source); i++ {
		switch source[i] {
		case '\\':
			i++
		case quote:
			return i
		}
	}
	return len(source) - 1
}

func isJSIdentifierStart(c byte) bool {
	return c == '_' || c == '$' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isJSIdentifierPart(c byte) bool {
	return isJSIdentifierStart(c) || (c >= '0' && c <= '9')
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
