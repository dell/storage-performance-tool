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

// formatStepID builds: mt-<step-number>-<base-ts>-<op>
// Example: mt-1-20250909.154500.123-create
func formatStepID(stepNumber int, baseTS, op string) string {
	op = strings.ToLower(strings.TrimSpace(op))
	if op == "" {
		op = "step"
	}
	return fmt.Sprintf("mt-%03d-%s-%s", stepNumber, baseTS, op)
}
