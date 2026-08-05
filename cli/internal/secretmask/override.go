package secretmask

import (
	"strings"
	"unicode"
)

const invalidEngineOverride = "<invalid-engine-override>"

// EngineOverride masks the value of a credential-bearing path=value override.
// It splits only the first equals sign so arbitrary non-sensitive values remain
// byte-for-byte useful in retained metadata. Malformed entries are unclassified
// and therefore never echoed into diagnostics or retained command artifacts.
func EngineOverride(override string) string {
	path, _, hasValue := strings.Cut(override, "=")
	if !hasValue || strings.TrimSpace(path) == "" {
		return invalidEngineOverride
	}
	if !isSensitiveEnginePath(path) {
		return override
	}
	return path + "=" + masked
}

// EngineOverrides returns a detached, sanitized snapshot.
func EngineOverrides(overrides []string) []string {
	if len(overrides) == 0 {
		return nil
	}
	sanitized := make([]string, len(overrides))
	for index, override := range overrides {
		sanitized[index] = EngineOverride(override)
	}
	return sanitized
}

// EngineOverrideList masks semicolon/newline-separated environment snapshots.
func EngineOverrideList(value string) string {
	parts := strings.FieldsFunc(value, func(char rune) bool {
		return char == ';' || char == '\n' || char == '\r'
	})
	if len(parts) == 0 {
		return EngineOverride(value)
	}
	for index, part := range parts {
		parts[index] = EngineOverride(strings.TrimSpace(part))
	}
	return strings.Join(parts, "; ")
}

func isSensitiveEnginePath(path string) bool {
	normalized := strings.Map(func(char rune) rune {
		if unicode.IsLetter(char) || unicode.IsDigit(char) {
			return unicode.ToLower(char)
		}
		return -1
	}, strings.TrimSpace(path))
	if normalized == "" {
		return false
	}
	for _, marker := range []string{"secret", "password", "token", "credential"} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	for _, suffix := range []string{
		"accesskey",
		"accesskeyid",
		"secretkey",
		"secretaccesskey",
		"keyid",
		"authuid",
		"authuser",
		"authusername",
		"authkey",
	} {
		if strings.HasSuffix(normalized, suffix) {
			return true
		}
	}
	return false
}
