package replay

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// archivedNicSettingKeys are the NIC-set settings under storage.net. Replay never keeps an archived
// value (D61): only --interfaces sets a replay's NIC set, so its rejections always see it.
var archivedNicSettingKeys = []string{"interfaces", "nodeInterfaces"}

// The same settings in the engine's dash-joined spellings, which it also accepts in a scenario's
// config maps. Replay removes only the nested form, so it rejects these: the full path wherever it
// appears, and the shorter one where it may be the storage config.
var (
	flatNicSettingKeys        = []string{"storage-net-interfaces", "storage-net-nodeInterfaces"}
	storageFlatNicSettingKeys = []string{"net-interfaces", "net-nodeInterfaces"}
)

var jsIdentifierPattern = regexp.MustCompile(`[A-Za-z_$][A-Za-z0-9_$]*`)

// legacyKeyFlatNet is the parent key of the settings in the engine's dash-joined spelling.
const legacyKeyFlatNet = legacyKeyStorage + "-" + legacyKeyNet

// archivedNicSettings records, in order, the distinct NIC-set values an archive declared.
type archivedNicSettings struct {
	declared []string
}

func (a *archivedNicSettings) add(key, value string) {
	// An empty value is the default, which sets no NIC set, so there is nothing to warn about.
	if compact := strings.Join(strings.Fields(value), ""); compact == "" || compact == "{}" || compact == "[]" {
		return
	}
	declared := legacyKeyStorage + "." + legacyKeyNet + "." + key + "=" + value
	if !slices.Contains(a.declared, declared) {
		a.declared = append(a.declared, declared)
	}
}

// diagnostics returns one warning naming every dropped value, or none.
func (a *archivedNicSettings) diagnostics() []Diagnostic {
	if len(a.declared) == 0 {
		return nil
	}
	pronoun := "it"
	if len(a.declared) > 1 {
		pronoun = "them"
	}
	return []Diagnostic{{Severity: severityWarning, Message: fmt.Sprintf(
		"the source run used %s; replay ignores %s, pass --interfaces to use NICs", strings.Join(a.declared, " and "), pronoun)}}
}

// fromJSONConfig records the NIC-set settings of a merged JSON step config. JSON conversion
// writes modeled settings only, so they are dropped without an unmodeled-path warning.
func (a *archivedNicSettings) fromJSONConfig(config map[string]any) {
	for _, key := range archivedNicSettingKeys {
		if value := getPath(config, legacyKeyStorage, legacyKeyNet, key); value != nil {
			a.add(key, jsonSettingValue(value))
		}
	}
}

func isArchivedNicSettingPath(path string) bool {
	for _, key := range archivedNicSettingKeys {
		if hasJSONConfigPathPrefix(path, legacyKeyStorage+"."+legacyKeyNet+"."+key) {
			return true
		}
	}
	return false
}

// jsonSettingValue renders a list of strings as the flag would take it, and anything else as JSON.
func jsonSettingValue(value any) string {
	switch t := value.(type) {
	case string:
		return t
	case []any:
		entries := make([]string, 0, len(t))
		for _, entry := range t {
			text, ok := entry.(string)
			if !ok {
				return compactJSON(value)
			}
			entries = append(entries, text)
		}
		return strings.Join(entries, ",")
	}
	return compactJSON(value)
}

func compactJSON(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprint(value)
	}
	return string(encoded)
}

// extractJSNicSettings removes every NIC-set setting from the literal storage.net objects of a
// JavaScript scenario, inline step configs included, and records its value. A value is any
// expression up to the comma or brace that ends its entry, so a variable reference is dropped as
// well. A setting replay cannot place in a literal storage.net object is rejected, since left in
// place it could override the replay's own NIC set past its checks. Replay does not run scenario
// code, so a name the code computes stays out of reach.
//
// Settings are removed one at a time, in source order, and the scenario is scanned again after
// each removal. Two settings that end an object would otherwise both take the comma between them,
// and removing both ranges at the original offsets would cut into the surrounding code.
func extractJSNicSettings(source string, archived *archivedNicSettings) (string, []Diagnostic) {
	for {
		code := jsCodeMask(source)
		occurrence, ok := firstJSNicSetting(code)
		if !ok {
			return source, unremovableJSNicSettings(code)
		}
		end := jsEntryValueEnd(code, occurrence.valueStart, occurrence.parentClose)
		archived.add(occurrence.key, jsSettingValue(code[occurrence.valueStart:end]))
		// The range always holds the key, so each pass shortens the source and the loop ends.
		start, stop := widenToAdjacentComma(code, occurrence.keyStart, end, occurrence.parentOpen, occurrence.parentClose)
		source = source[:start] + source[stop:]
	}
}

// firstJSNicSetting returns the first NIC-set key directly inside a closed storage.net object.
func firstJSNicSetting(code string) (jsKeyOccurrence, bool) {
	for _, occurrence := range jsKeyOccurrences(code, archivedNicSettingKeys...) {
		if occurrence.parentKey == legacyKeyNet && occurrence.grandparent == legacyKeyStorage && occurrence.parentClose >= 0 {
			return occurrence, true
		}
	}
	return jsKeyOccurrence{}, false
}

// unremovableJSNicSettings rejects what is left once the literal storage.net settings are gone,
// where it may reach storage.net: a key in a "net" object outside "storage", a key in the engine's
// flat spelling, an assignment to one of the names and, when storage.net or storage is given by
// reference, a key in an object with no key of its own or in the named object the reference
// mentions. Any other key, such as one under storage.driver.limit, is not a NIC setting.
func unremovableJSNicSettings(code string) []Diagnostic {
	name := firstUnremovableJSNicSetting(code)
	if name == "" {
		return nil
	}
	return []Diagnostic{{Severity: severityError, Code: failureInvalidInterfaces, Message: fmt.Sprintf(
		"archived scenario sets %q in a form replay cannot remove, such as a variable, an assignment or a \"net\" object "+
			"outside \"storage\"; write it in the literal storage.net object, or remove it; replay sets NICs with --interfaces", name)}}
}

func firstUnremovableJSNicSetting(code string) string {
	netNames, netByReference := jsReferences(code, legacyKeyNet)
	for _, occurrence := range jsKeyOccurrences(code, archivedNicSettingKeys...) {
		switch {
		case occurrence.parentKey == legacyKeyNet, occurrence.parentKey == legacyKeyFlatNet,
			occurrence.parentKey == "" && netByReference, occurrence.parentKey != "" && slices.Contains(netNames, occurrence.parentKey):
			return occurrence.key
		}
	}
	if flat := jsKeyOccurrences(code, flatNicSettingKeys...); len(flat) > 0 {
		return flat[0].key
	}
	storageNames, storageByReference := jsReferences(code, legacyKeyStorage)
	for _, occurrence := range jsKeyOccurrences(code, storageFlatNicSettingKeys...) {
		switch {
		case occurrence.parentKey == legacyKeyStorage, occurrence.parentKey == "" && storageByReference,
			occurrence.parentKey != "" && slices.Contains(storageNames, occurrence.parentKey):
			return occurrence.key
		}
	}
	return firstJSMemberAssignment(code, archivedNicSettingKeys...)
}

// jsReferences returns the names in every value of key that is not an object literal, and whether
// that object is given by reference at all, through such a value or an assignment.
func jsReferences(code, key string) ([]string, bool) {
	var names []string
	byReference := firstJSMemberAssignment(code, key) != ""
	for _, occurrence := range jsKeyOccurrences(code, key) {
		if occurrence.valueStart >= len(code) || code[occurrence.valueStart] == '{' {
			continue
		}
		byReference = true
		if occurrence.parentClose >= 0 {
			names = append(names, jsIdentifierPattern.FindAllString(code[occurrence.valueStart:jsEntryValueEnd(code, occurrence.valueStart, occurrence.parentClose)], -1)...)
		}
	}
	return names, byReference
}

// firstJSMemberAssignment returns the first of names assigned as a member, x.name = or
// x["name"] =, outside strings.
func firstJSMemberAssignment(code string, names ...string) string {
	assigned := func(end int) bool {
		next := skipJSWhitespace(code, end)
		return next < len(code) && code[next] == '=' && (next+1 >= len(code) || (code[next+1] != '=' && code[next+1] != '>'))
	}
	for i := 0; i < len(code); i++ {
		switch c := code[i]; c {
		case '"', '\'', '`':
			end := jsStringEnd(code, i)
			if end >= len(code) {
				return ""
			}
			if next := skipJSWhitespace(code, end+1); c != '`' && jsPrecededBy(code, i, "[") && next < len(code) && code[next] == ']' &&
				slices.Contains(names, code[i+1:end]) && assigned(next+1) {
				return code[i+1 : end]
			}
			i = end
		case '.':
			start := skipJSWhitespace(code, i+1)
			stop := start
			for stop < len(code) && isJSIdentifierPart(code[stop]) {
				stop++
			}
			if slices.Contains(names, code[start:stop]) && assigned(stop) {
				return code[start:stop]
			}
		}
	}
	return ""
}

// jsEntryValueEnd returns the offset after the value starting at start: the last non-space
// before the first comma or closing brace outside brackets, braces, parentheses and strings.
func jsEntryValueEnd(code string, start, objectClose int) int {
	depth := 0
	end := start
	for i := start; i < objectClose; i++ {
		switch c := code[i]; {
		case c == '"' || c == '\'' || c == '`':
			i = jsStringEnd(code, i)
		case c == '[' || c == '{' || c == '(':
			depth++
		case c == ']' || c == '}' || c == ')':
			depth--
		case c == ',' && depth == 0:
			return end
		case isJSWhitespace(c):
			continue
		}
		end = i + 1
	}
	return end
}

// jsSettingValue renders an array or string literal as the flag would take it, and anything else
// as its source text.
func jsSettingValue(text string) string {
	text = strings.TrimSpace(text)
	if len(text) >= 2 && (text[0] == '"' || text[0] == '\'') && text[len(text)-1] == text[0] && jsStringEnd(text, 0) == len(text)-1 {
		return unquoteJSString(text[1 : len(text)-1])
	}
	if entries, ok := jsStringArrayLiteral(text); ok {
		return strings.Join(entries, ",")
	}
	return strings.Join(strings.Fields(text), " ")
}

func jsStringArrayLiteral(text string) ([]string, bool) {
	if !strings.HasPrefix(text, "[") {
		return nil, false
	}
	entries := []string{}
	i := skipJSWhitespace(text, 1)
	if i < len(text) && text[i] == ']' {
		return entries, i == len(text)-1
	}
	for i < len(text) {
		if text[i] != '"' && text[i] != '\'' {
			return nil, false
		}
		end := jsStringEnd(text, i)
		if end >= len(text) {
			return nil, false
		}
		entries = append(entries, unquoteJSString(text[i+1:end]))
		i = skipJSWhitespace(text, end+1)
		switch {
		case i < len(text) && text[i] == ',':
			i = skipJSWhitespace(text, i+1)
		case i == len(text)-1 && text[i] == ']':
			return entries, true
		default:
			return nil, false
		}
	}
	return nil, false
}
