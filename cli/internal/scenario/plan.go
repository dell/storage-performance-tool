package scenario

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/integrityplan"
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
	steps := make([]StepInfo, 0, len(matches))
	for _, m := range matches {
		// m[1]=label, m[2]=number, m[3]=ts, m[4]=op
		id := fmt.Sprintf("%s-%s-%s-%s", m[1], m[2], m[3], m[4])
		if seen[id] {
			return StepPlan{}, fmt.Errorf("duplicate step id %q in scenario", id)
		}
		seen[id] = true
		n, _ := strconv.Atoi(m[2])
		steps = append(steps, StepInfo{ID: id, Number: n, Op: m[4]})
	}

	// Preserve natural order by step number.
	sort.SliceStable(steps, func(i, j int) bool { return steps[i].Number < steps[j].Number })
	return StepPlan{Steps: steps}, nil
}

// BuildVerificationPlan assigns semantic roles to one already-rendered,
// fail-closed integrity step plan. Rendering remains the source of exact IDs;
// downstream consumers no longer reconstruct planned roles from suffixes.
func BuildVerificationPlan(params Params, steps StepPlan) (integrityplan.Plan, error) {
	if !IsIntegrityWorkload(params) {
		return integrityplan.Plan{}, fmt.Errorf(
			"typed verification plan does not support workload %q", params.WorkloadType)
	}
	if params.RunID <= 0 {
		return integrityplan.Plan{}, fmt.Errorf("typed verification plan requires a positive run id")
	}
	byRole := make(map[integrityplan.StepRole][]StepInfo)
	stepIDs := make(map[string]struct{}, len(steps.Steps))
	for _, step := range steps.Steps {
		if step.ID == "" {
			return integrityplan.Plan{}, fmt.Errorf("verification plan contains an empty step id")
		}
		if _, duplicate := stepIDs[step.ID]; duplicate {
			return integrityplan.Plan{}, fmt.Errorf("verification plan contains duplicate step id %q", step.ID)
		}
		stepIDs[step.ID] = struct{}{}
		var role integrityplan.StepRole
		switch step.Op {
		case constants.IntegrityStepRoleCreate:
			role = integrityplan.StepRoleCreate
		case constants.IntegrityStepRoleList:
			role = integrityplan.StepRoleList
		case constants.IntegrityStepRoleVerify:
			role = integrityplan.StepRoleVerify
		case stepOpDelete:
			role = integrityplan.StepRoleCleanup
		default:
			return integrityplan.Plan{}, fmt.Errorf(
				"verification step %q has unsupported role %q", step.ID, step.Op)
		}
		byRole[role] = append(byRole[role], step)
	}
	one := func(role integrityplan.StepRole, required bool) (*integrityplan.PlannedStep, error) {
		matches := byRole[role]
		if len(matches) == 0 {
			if required {
				return nil, fmt.Errorf("verification plan is missing %s step", role)
			}
			return nil, nil
		}
		if len(matches) != 1 {
			return nil, fmt.Errorf("verification plan has %d %s steps", len(matches), role)
		}
		return &integrityplan.PlannedStep{ID: matches[0].ID, Number: matches[0].Number, Role: role}, nil
	}
	if params.DeferVerification && params.WorkloadType != WorkloadTypeWriteVerify {
		return integrityplan.Plan{}, fmt.Errorf(
			"deferred verification is valid only for write-verify")
	}
	if params.DeferVerification && params.Cleanup {
		return integrityplan.Plan{}, fmt.Errorf(
			"deferred write-verify cannot include cleanup")
	}
	verifier, err := one(integrityplan.StepRoleVerify, !params.DeferVerification)
	if err != nil {
		return integrityplan.Plan{}, err
	}
	cleanup, err := one(integrityplan.StepRoleCleanup, false)
	if err != nil {
		return integrityplan.Plan{}, err
	}
	if (cleanup != nil) != params.Cleanup {
		return integrityplan.Plan{}, fmt.Errorf(
			"verification cleanup step presence does not match requested cleanup=%t", params.Cleanup)
	}
	versions := params.Versions
	if versions == "" {
		versions = VersionsCurrent
	}
	plan := integrityplan.Plan{
		RunID: params.RunID, Workload: params.WorkloadType,
		Cleanup: cleanup, Multipart: params.PartSize != "",
		AllowEmpty:        params.AllowEmptySelection,
		DiscoveryVersions: integrityplan.DiscoveryVersions(versions),
	}
	if verifier != nil {
		plan.Verifier = *verifier
	}
	switch params.WorkloadType {
	case WorkloadTypeWriteVerify:
		producer, producerErr := one(integrityplan.StepRoleCreate, true)
		if producerErr != nil {
			return integrityplan.Plan{}, producerErr
		}
		if len(byRole[integrityplan.StepRoleList]) != 0 {
			return integrityplan.Plan{}, fmt.Errorf("write-verify plan unexpectedly contains a LIST step")
		}
		plan.Producer = producer
		plan.Input = integrityplan.InputWritten
		if params.DeferVerification {
			if verifier != nil {
				return integrityplan.Plan{}, fmt.Errorf(
					"deferred write-verify plan unexpectedly contains a verification READ step")
			}
			plan.Kind = integrityplan.PlanKindWriteSeed
		} else {
			plan.Kind = integrityplan.PlanKindWriteRead
		}
	case WorkloadTypeReadVerify:
		if len(byRole[integrityplan.StepRoleCreate]) != 0 {
			return integrityplan.Plan{}, fmt.Errorf("read-verify plan unexpectedly contains a CREATE step")
		}
		if params.ItemsFile != "" {
			if len(byRole[integrityplan.StepRoleList]) != 0 {
				return integrityplan.Plan{}, fmt.Errorf("external read-verify plan unexpectedly contains a LIST step")
			}
			plan.Kind = integrityplan.PlanKindReadExternal
			plan.Input = integrityplan.InputExternal
		} else {
			producer, producerErr := one(integrityplan.StepRoleList, true)
			if producerErr != nil {
				return integrityplan.Plan{}, producerErr
			}
			plan.Kind = integrityplan.PlanKindReadDiscovered
			plan.Producer = producer
			plan.Input = integrityplan.InputDiscovered
		}
	}
	if !plan.Valid() {
		return integrityplan.Plan{}, fmt.Errorf("generated verification plan is internally inconsistent")
	}
	return plan, nil
}
