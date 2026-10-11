package replay

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

var jsNumberRe = regexp.MustCompile(`^-?[0-9]+(?:\.[0-9]+)?`)

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
// generated defaults alone carry endpoint selection. A lexical scan skips comments and treats
// strings as whole tokens, so neither can be mistaken for configuration. Replay only evaluates
// literal declarations: an endpoint key directly inside a "net" object whose whole value is an
// object literal of literal values or exported variables, stating its mode. Any other endpoint
// declaration, including a partial one that would inherit settings from another config, is
// rejected rather than left to override the validated defaults.
func extractJSEndpointSelections(source string, vars map[string]string, archived *ArchivedEndpointSelection) (string, []Diagnostic) {
	code := jsCodeMask(source)
	var diagnostics []Diagnostic
	var replacements []jsReplacement
	for _, occurrence := range jsEndpointOccurrences(code) {
		declaration, valueEnd, err := parseJSEndpointValue(code, occurrence, vars)
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
		start, end := widenToAdjacentComma(code, occurrence.keyStart, valueEnd, occurrence.parentOpen, occurrence.parentClose)
		replacements = append(replacements, jsReplacement{start: start, end: end, text: ""})
	}
	return applyJSReplacements(source, replacements), diagnostics
}

// jsEndpointOccurrence is an object key named endpoint and the object that contains it.
type jsEndpointOccurrence struct {
	keyStart    int    // offset of the key
	valueStart  int    // offset of the value after the colon
	parentKey   string // key whose value is the containing object; empty if there is none
	parentOpen  int    // offset of the containing object's opening brace; -1 if there is none
	parentClose int    // offset of the containing object's closing brace; -1 if it never closes
}

// jsEndpointOccurrences walks code, which must have its comments masked, as tokens and reports
// every key named endpoint with its containing object. Strings are skipped as whole tokens, so
// their contents never count as keys.
func jsEndpointOccurrences(code string) []jsEndpointOccurrence {
	type frame struct {
		key         string
		open        int
		occurrences []int // indexes of the endpoint keys directly inside this object
	}
	var stack []frame
	var occurrences []jsEndpointOccurrence
	pendingKey := "" // a key whose colon was just read, until its value starts
	colonAfter := func(end int) (int, bool) {
		j := skipJSWhitespace(code, end)
		return j, j < len(code) && code[j] == ':'
	}
	keyRead := func(name string, start, colon int) {
		if name == "endpoint" {
			occurrence := jsEndpointOccurrence{keyStart: start, valueStart: skipJSWhitespace(code, colon+1), parentOpen: -1, parentClose: -1}
			if len(stack) > 0 {
				parent := &stack[len(stack)-1]
				occurrence.parentKey, occurrence.parentOpen = parent.key, parent.open
				parent.occurrences = append(parent.occurrences, len(occurrences))
			}
			occurrences = append(occurrences, occurrence)
		}
		pendingKey = name
	}
	for i := 0; i < len(code); i++ {
		c := code[i]
		switch {
		case isJSWhitespace(c):
		case c == '"' || c == '\'' || c == '`':
			end := jsStringEnd(code, i)
			if colon, ok := colonAfter(end + 1); ok && c != '`' && end > i {
				keyRead(code[i+1:end], i, colon)
				i = colon
				continue
			}
			pendingKey = ""
			i = end
		case isJSIdentifierStart(c):
			start := i
			for i+1 < len(code) && isJSIdentifierPart(code[i+1]) {
				i++
			}
			if colon, ok := colonAfter(i + 1); ok {
				keyRead(code[start:i+1], start, colon)
				i = colon
				continue
			}
			pendingKey = ""
		case c == '{':
			stack = append(stack, frame{key: pendingKey, open: i})
			pendingKey = ""
		case c == '}':
			if len(stack) > 0 {
				for _, index := range stack[len(stack)-1].occurrences {
					occurrences[index].parentClose = i
				}
				stack = stack[:len(stack)-1]
			}
			pendingKey = ""
		default:
			pendingKey = ""
		}
	}
	return occurrences
}

type jsEndpointDeclaration struct {
	mode                 string
	dnsTimeoutMillis     int
	connectTimeoutMillis int
	environmentSpecific  []string
}

// parseJSEndpointValue evaluates one endpoint declaration and returns the offset after its value.
func parseJSEndpointValue(code string, occurrence jsEndpointOccurrence, vars map[string]string) (jsEndpointDeclaration, int, error) {
	if occurrence.parentKey != "net" || occurrence.parentClose < 0 || occurrence.valueStart >= len(code) || code[occurrence.valueStart] != '{' {
		return jsEndpointDeclaration{}, 0, fmt.Errorf("declares storage.net.endpoint in a form replay cannot evaluate, " +
			"such as a variable reference or a key outside a \"net\" object; write it as an object literal inside " +
			"the \"net\" object, or remove it")
	}
	entries, end, err := parseJSObjectLiteral(code, occurrence.valueStart)
	if err != nil {
		return jsEndpointDeclaration{}, 0, fmt.Errorf("declares storage.net.endpoint with %w", err)
	}
	if next := skipJSWhitespace(code, end); next >= len(code) || (code[next] != ',' && code[next] != '}') {
		return jsEndpointDeclaration{}, 0, fmt.Errorf("sets storage.net.endpoint to an expression rather than a single object literal")
	}
	declaration, err := endpointDeclaration(entries, vars)
	return declaration, end, err
}

// jsEntry is one key of an object literal: a single literal token or a nested object literal.
type jsEntry struct {
	key      string
	token    string
	object   []jsEntry
	isObject bool
}

// parseJSObjectLiteral parses the object literal opening at open: keys that are names or strings,
// values that are single literal tokens or nested object literals. Spreads, computed keys and
// expressions are errors. It returns the offset after the closing brace.
func parseJSObjectLiteral(code string, open int) ([]jsEntry, int, error) {
	entries := []jsEntry{}
	i := open + 1
	for {
		i = skipJSWhitespace(code, i)
		if i >= len(code) {
			return nil, i, fmt.Errorf("an unterminated object")
		}
		if code[i] == '}' {
			return entries, i + 1, nil
		}
		key, next, err := parseJSKey(code, i)
		if err != nil {
			return nil, i, err
		}
		i = skipJSWhitespace(code, next)
		if i >= len(code) || code[i] != ':' {
			return nil, i, fmt.Errorf("an entry that is not a key and value")
		}
		i = skipJSWhitespace(code, i+1)
		entry := jsEntry{key: key}
		if i < len(code) && code[i] == '{' {
			if entry.object, i, err = parseJSObjectLiteral(code, i); err != nil {
				return nil, i, err
			}
			entry.isObject = true
		} else if entry.token, i, err = parseJSLiteralToken(code, i); err != nil {
			return nil, i, fmt.Errorf("%w in %q", err, key)
		}
		entries = append(entries, entry)
		i = skipJSWhitespace(code, i)
		switch {
		case i < len(code) && code[i] == ',':
			i++
		case i < len(code) && code[i] == '}':
			return entries, i + 1, nil
		default:
			return nil, i, fmt.Errorf("an expression in the value of %q", key)
		}
	}
}

func parseJSKey(code string, i int) (string, int, error) {
	switch c := code[i]; {
	case c == '"' || c == '\'':
		end := jsStringEnd(code, i)
		return unquoteJSString(code[i+1 : end]), end + 1, nil
	case isJSIdentifierStart(c):
		j := i + 1
		for j < len(code) && isJSIdentifierPart(code[j]) {
			j++
		}
		return code[i:j], j, nil
	}
	return "", i, fmt.Errorf("a key that is not a literal name, such as a spread or computed key")
}

func parseJSLiteralToken(code string, i int) (string, int, error) {
	if i < len(code) {
		switch c := code[i]; {
		case c == '"' || c == '\'':
			end := jsStringEnd(code, i)
			return code[i : end+1], end + 1, nil
		case c == '-' || (c >= '0' && c <= '9'):
			if number := jsNumberRe.FindString(code[i:]); number != "" {
				return number, i + len(number), nil
			}
		case isJSIdentifierStart(c):
			j := i + 1
			for j < len(code) && isJSIdentifierPart(code[j]) {
				j++
			}
			return code[i:j], j, nil
		}
	}
	return "", i, fmt.Errorf("a value that is not a literal")
}

// JavaScript boolean literals accepted as endpoint setting values.
const (
	jsTrue  = "true"
	jsFalse = "false"
)

// endpointDeclaration validates the parsed endpoint object and resolves its values.
func endpointDeclaration(entries []jsEntry, vars map[string]string) (jsEndpointDeclaration, error) {
	var declaration jsEndpointDeclaration
	fields, err := jsFields(entries, "storage.net.endpoint", map[string]bool{"selection": false, "hostname": false, "dns": true, "connect": true})
	if err != nil {
		return declaration, err
	}
	dns, err := jsFields(fields["dns"].object, "storage.net.endpoint.dns", map[string]bool{"server": false, "timeoutMilliSec": false})
	if err != nil {
		return declaration, err
	}
	connect, err := jsFields(fields["connect"].object, "storage.net.endpoint.connect", map[string]bool{"timeoutMilliSec": false})
	if err != nil {
		return declaration, err
	}
	if declaration.mode, err = jsResolve(fields["selection"].token, "selection", vars); err != nil {
		return declaration, err
	}
	if declaration.mode == "" {
		return declaration, fmt.Errorf("declares storage.net.endpoint without a selection mode; replay cannot resolve " +
			"endpoint settings inherited from another config, so state the mode in every endpoint object")
	}
	for _, environmentSpecific := range []struct {
		entry jsEntry
		path  string
	}{{fields["hostname"], "storage.net.endpoint.hostname"}, {dns["server"], "storage.net.endpoint.dns.server"}} {
		if value, err := jsResolve(environmentSpecific.entry.token, environmentSpecific.path, vars); err != nil {
			return declaration, err
		} else if value != "" {
			declaration.environmentSpecific = append(declaration.environmentSpecific, environmentSpecific.path)
		}
	}
	if declaration.dnsTimeoutMillis, err = jsResolveInt(dns["timeoutMilliSec"].token, "dns timeoutMilliSec", vars); err != nil {
		return declaration, err
	}
	if declaration.connectTimeoutMillis, err = jsResolveInt(connect["timeoutMilliSec"].token, "connect timeoutMilliSec", vars); err != nil {
		return declaration, err
	}
	return declaration, nil
}

// jsFields indexes an object's entries, rejecting unknown or repeated keys and values of the wrong
// shape; allowed maps each key to whether its value must be an object.
func jsFields(entries []jsEntry, path string, allowed map[string]bool) (map[string]jsEntry, error) {
	fields := map[string]jsEntry{}
	for _, entry := range entries {
		isObject, ok := allowed[entry.key]
		if !ok {
			return nil, fmt.Errorf("declares an unsupported setting %s.%s", path, entry.key)
		}
		if _, duplicate := fields[entry.key]; duplicate {
			return nil, fmt.Errorf("declares %s.%s more than once", path, entry.key)
		}
		if entry.isObject != isObject {
			return nil, fmt.Errorf("declares %s.%s with a value of the wrong shape", path, entry.key)
		}
		fields[entry.key] = entry
	}
	return fields, nil
}

// jsResolve evaluates a literal token: a string (with export placeholders expanded), a number, a
// boolean, or the name of an exported variable. An empty token means the setting is absent.
func jsResolve(token, name string, vars map[string]string) (string, error) {
	switch {
	case token == "":
		return "", nil
	case token[0] == '"' || token[0] == '\'':
		return expandWithExports(unquoteJSString(token[1:len(token)-1]), vars), nil
	case token == jsTrue || token == jsFalse || token[0] == '-' || (token[0] >= '0' && token[0] <= '9'):
		return token, nil
	}
	if value, ok := vars[token]; ok {
		return value, nil
	}
	return "", fmt.Errorf("sets %s from the variable %s, which replay cannot evaluate", name, token)
}

func jsResolveInt(token, name string, vars map[string]string) (int, error) {
	value, err := jsResolve(token, name, vars)
	if err != nil || value == "" {
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
