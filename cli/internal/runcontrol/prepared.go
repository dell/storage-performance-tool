/*
Copyright © 2026 Dell Technologies
*/

package runcontrol

import (
	"context"
	"sync"

	"github.com/dell/storage-performance-tool/cli/internal/integrityplan"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

// PreparedRun is the immutable handoff between command preparation and run
// lifecycle ownership. Accessors return copies so launch adapters, archival,
// and completion tracking all observe the same prepared input bytes.
type PreparedRun struct {
	params       scenario.Params
	scenarioJS   []byte
	defaultsYAML []byte
	plan         scenario.StepPlan
	verifyPlan   *integrityplan.Plan
	scenarioPath string
	cleanup      func(context.Context) error

	cleanupOnce sync.Once
	cleanupErr  error
}

// NewPreparedRun captures one generated run bundle. The caller remains
// responsible for validating whether a missing step plan is allowed.
func NewPreparedRun(
	params scenario.Params,
	scenarioJS, defaultsYAML []byte,
	plan scenario.StepPlan,
	scenarioPath string,
	cleanup func(context.Context) error,
	verificationPlans ...integrityplan.Plan,
) *PreparedRun {
	prepared := &PreparedRun{
		params:       cloneParams(params),
		scenarioJS:   append([]byte(nil), scenarioJS...),
		defaultsYAML: append([]byte(nil), defaultsYAML...),
		plan:         cloneStepPlan(plan),
		scenarioPath: scenarioPath,
		cleanup:      cleanup,
	}
	if len(verificationPlans) > 0 && verificationPlans[0].Valid() {
		planCopy := verificationPlans[0].Clone()
		prepared.verifyPlan = &planCopy
	}
	return prepared
}

// Params returns a detached snapshot of the prepared scenario parameters.
func (r *PreparedRun) Params() scenario.Params {
	if r == nil {
		return scenario.Params{}
	}
	return cloneParams(r.params)
}

// ScenarioJS returns the exact prepared scenario bytes.
func (r *PreparedRun) ScenarioJS() []byte {
	if r == nil {
		return nil
	}
	return append([]byte(nil), r.scenarioJS...)
}

// DefaultsYAML returns the exact prepared engine-default bytes.
func (r *PreparedRun) DefaultsYAML() []byte {
	if r == nil {
		return nil
	}
	return append([]byte(nil), r.defaultsYAML...)
}

// Plan returns the step plan parsed from ScenarioJS.
func (r *PreparedRun) Plan() scenario.StepPlan {
	if r == nil {
		return scenario.StepPlan{}
	}
	return cloneStepPlan(r.plan)
}

// VerificationPlan returns the immutable typed plan for an integrity run.
func (r *PreparedRun) VerificationPlan() (integrityplan.Plan, bool) {
	if r == nil || r.verifyPlan == nil {
		return integrityplan.Plan{}, false
	}
	return r.verifyPlan.Clone(), true
}

// ExpectedStepIDs returns the ordered step IDs used by completion tracking.
func (r *PreparedRun) ExpectedStepIDs() []string {
	plan := r.Plan()
	ids := make([]string, 0, len(plan.Steps))
	for _, step := range plan.Steps {
		ids = append(ids, step.ID)
	}
	return ids
}

// ScenarioPath returns the local compatibility file containing ScenarioJS.
func (r *PreparedRun) ScenarioPath() string {
	if r == nil {
		return ""
	}
	return r.scenarioPath
}

// RunID returns the configured numeric identity captured before launch.
func (r *PreparedRun) RunID() int64 {
	if r == nil {
		return 0
	}
	return r.params.RunID
}

// FileMounts returns the exact external-file staging contract for launchers.
func (r *PreparedRun) FileMounts() []scenario.FileMount {
	if r == nil {
		return nil
	}
	return append([]scenario.FileMount(nil), r.params.ItemFileMounts...)
}

// Cleanup releases preparation-owned files exactly once. Multiple adapters may
// safely join the same cleanup result without repeating removal.
func (r *PreparedRun) Cleanup(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.cleanupOnce.Do(func() {
		if r.cleanup != nil {
			r.cleanupErr = r.cleanup(ctx)
		}
	})
	return r.cleanupErr
}

func cloneParams(params scenario.Params) scenario.Params {
	clone := params
	clone.Endpoints = append([]string(nil), params.Endpoints...)
	clone.EngineOverrides = append([]string(nil), params.EngineOverrides...)
	clone.ItemFileMounts = append([]scenario.FileMount(nil), params.ItemFileMounts...)
	clone.ItemStagingDirs = append([]string(nil), params.ItemStagingDirs...)
	return clone
}

func cloneStepPlan(plan scenario.StepPlan) scenario.StepPlan {
	return scenario.StepPlan{Steps: append([]scenario.StepInfo(nil), plan.Steps...)}
}
