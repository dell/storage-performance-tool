package scenario

import (
	"fmt"
	"strings"
	"time"
)

// BaseTimestamp returns a new UTC timestamp string in yyyymmdd.HHMMSS.mmm.
// Callers that need a stable timestamp across multiple GenerateScenario
// invocations should call this once and store the result in Params.BaseTimestamp.
func BaseTimestamp() string {
	return time.Now().UTC().Format("20060102.150405.000")
}

// resolveTimestamp returns params.BaseTimestamp if non-empty, otherwise
// generates a fresh timestamp via BaseTimestamp().
func resolveTimestamp(params Params) string {
	if params.BaseTimestamp != "" {
		return params.BaseTimestamp
	}
	return BaseTimestamp()
}

// formatStepID builds: <label>-<step-number>-<base-ts>-<op>.
// Empty labels retain the default mt prefix.
func formatStepID(label string, stepNumber int, baseTS, op string) string {
	op = strings.ToLower(strings.TrimSpace(op))
	if op == "" {
		op = "step"
	}
	return fmt.Sprintf("%s-%03d-%s-%s", SanitizeLabel(label), stepNumber, baseTS, op)
}

// SanitizeLabel enforces the allowed character set and length for labels.
// Allowed: A–Z, a–z, 0–9, dot, underscore, hyphen. Others become '_'.
// Empty after trimming yields default "mt".
func SanitizeLabel(s string) string {
	const (
		allowed = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-"
		maxLen  = 64
		def     = "mt"
	)

	// Trim whitespace
	s = strings.TrimSpace(s)
	if s == "" {
		return def
	}

	// Replace any disallowed rune with '_'
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if strings.ContainsRune(allowed, r) {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	out := b.String()

	// Enforce max length
	if len(out) > maxLen {
		out = out[:maxLen]
	}

	// Avoid empty (could happen if input had only spaces)
	if out == "" {
		return def
	}
	return out
}
