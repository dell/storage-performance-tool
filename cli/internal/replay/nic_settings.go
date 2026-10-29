package replay

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// archivedNicSettingKeys are the NIC-set settings under storage.net. Replay never keeps an archived
// value (D61): only --interfaces sets a replay's NIC set, so its rejections always see it.
var archivedNicSettingKeys = []string{"interfaces", "nodeInterfaces"}

// archivedNicSettings records, in order, the distinct NIC-set values an archive declared.
type archivedNicSettings struct {
	declared []string
}

func (a *archivedNicSettings) add(key, value string) {
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

// extractJSNicSettings removes every NIC-set setting from the "net" objects of a JavaScript
// scenario, inline step configs included, and records its value. A value is any expression up to
// the comma or brace that ends its entry, so a variable reference is dropped as well.
func extractJSNicSettings(source string, archived *archivedNicSettings) string {
	code := jsCodeMask(source)
	var replacements []jsReplacement
	for _, occurrence := range jsKeyOccurrences(code, archivedNicSettingKeys...) {
		if occurrence.parentKey != legacyKeyNet || occurrence.parentClose < 0 {
			continue
		}
		end := jsEntryValueEnd(code, occurrence.valueStart, occurrence.parentClose)
		archived.add(occurrence.key, jsSettingValue(code[occurrence.valueStart:end]))
		start, stop := widenToAdjacentComma(code, occurrence.keyStart, end, occurrence.parentOpen, occurrence.parentClose)
		replacements = append(replacements, jsReplacement{start: start, end: stop, text: ""})
	}
	return applyJSReplacements(source, replacements)
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
