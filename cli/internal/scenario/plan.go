package scenario

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
)

// StepInfo describes a single scenario step for results tracking.
type StepInfo struct {
	ID     string
	Number int
	Op     string
}

// StepPlan is an ordered list of steps discovered in a generated scenario.
type StepPlan struct {
	Steps []StepInfo
}

var stepIDCaptureRe = regexp.MustCompile(`"id"\s*:\s*"([A-Za-z0-9._-]+)-([0-9]{3})-([0-9]{8}\.[0-9]{6}\.[0-9]{3})-([a-z0-9_-]+)"`)

// BuildStepPlanFromScenario scans the scenario text and extracts step IDs in order.
// It does not attempt to infer relationships beyond what is encoded in the ID.
func BuildStepPlanFromScenario(scenarioText string) (StepPlan, error) {
	matches := stepIDCaptureRe.FindAllStringSubmatch(scenarioText, -1)
	if len(matches) == 0 {
		return StepPlan{}, fmt.Errorf("no step ids found in scenario")
	}

	seen := make(map[string]bool)
	// Preallocate capacity to number of matches to satisfy linter; dedup may reduce final len.
	steps := make([]StepInfo, 0, len(matches))
	for _, m := range matches {
		// m[1]=label, m[2]=number, m[3]=ts, m[4]=op
		id := fmt.Sprintf("%s-%s-%s-%s", m[1], m[2], m[3], m[4])
		if seen[id] {
			continue
		}
		seen[id] = true
		n, _ := strconv.Atoi(m[2])
		steps = append(steps, StepInfo{ID: id, Number: n, Op: m[4]})
	}

	// Preserve natural order by step number (stable if duplicates appeared out of order)
	sort.SliceStable(steps, func(i, j int) bool { return steps[i].Number < steps[j].Number })
	return StepPlan{Steps: steps}, nil
}

// GenerateScenarioWithPlan returns the scenario and a parsed StepPlan.
// It wraps GenerateScenario to ensure the plan matches the emitted content.
func GenerateScenarioWithPlan(params Params) (string, StepPlan, error) {
	s, err := GenerateScenario(params)
	if err != nil {
		return "", StepPlan{}, err
	}
	plan, err := BuildStepPlanFromScenario(s)
	if err != nil {
		return s, StepPlan{}, err
	}
	return s, plan, nil
}
