package scenario

import (
	"fmt"
	"strings"
	"time"
)

// baseTimestamp returns a UTC timestamp string in yyyymmdd.HHMMSS.mmm
// This is stable for the scenario generation call and intended to be
// shared across all steps in a single test for natural sorting.
func baseTimestamp() string {
	return time.Now().UTC().Format("20060102.150405.000")
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
