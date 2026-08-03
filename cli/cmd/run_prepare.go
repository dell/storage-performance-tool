/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/dell/storage-performance-tool/cli/internal/integrityplan"
	"github.com/dell/storage-performance-tool/cli/internal/runcontrol"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

type runPreparationDependencies struct {
	PrepareExternal  func(scenario.Params) (scenario.Params, error)
	CleanupExternal  func(context.Context, scenario.Params) error
	GenerateScenario func(scenario.Params) (string, error)
	GenerateDefaults func(scenario.Params) ([]byte, error)
	BuildStepPlan    func(string) (scenario.StepPlan, error)
	BuildVerifyPlan  func(scenario.Params, scenario.StepPlan) (integrityplan.Plan, error)
	WriteScenario    func(string, []byte, os.FileMode) error
	RemoveScenario   func(string) error
}

var defaultRunPreparationDependencies = runPreparationDependencies{
	PrepareExternal:  prepareExternalItemFilesForRun,
	CleanupExternal:  scenario.CleanupPreparedItemFiles,
	GenerateScenario: scenario.GenerateScenario,
	GenerateDefaults: scenario.GenerateDefaults,
	BuildStepPlan:    scenario.BuildStepPlanFromScenario,
	BuildVerifyPlan:  scenario.BuildVerificationPlan,
	WriteScenario:    os.WriteFile,
	RemoveScenario:   os.Remove,
}

func prepareRunBundle(
	params scenario.Params, scenarioPath string, preserveScenario bool,
) (*runcontrol.PreparedRun, error) {
	return prepareRunBundleWithDependencies(
		params, scenarioPath, preserveScenario, defaultRunPreparationDependencies)
}

func prepareRunBundleWithDependencies(
	params scenario.Params,
	scenarioPath string,
	preserveScenario bool,
	deps runPreparationDependencies,
) (*runcontrol.PreparedRun, error) {
	preparedParams, err := deps.PrepareExternal(params)
	if err != nil {
		return nil, err
	}

	var cleanupOnce sync.Once
	var cleanupErr error
	cleanup := func(ctx context.Context) error {
		cleanupOnce.Do(func() {
			if ctx == nil {
				ctx = context.Background()
			}
			var cleanupErrs []error
			if err := ctx.Err(); err != nil {
				cleanupErr = fmt.Errorf("prepared run cleanup canceled: %w", err)
				return
			}
			if deps.CleanupExternal != nil {
				cleanupErrs = append(cleanupErrs, deps.CleanupExternal(ctx, preparedParams))
			}
			if preserveScenario || scenarioPath == "" {
				cleanupErr = errors.Join(cleanupErrs...)
				return
			}
			if err := ctx.Err(); err != nil {
				cleanupErrs = append(cleanupErrs, fmt.Errorf("prepared run cleanup canceled: %w", err))
				cleanupErr = errors.Join(cleanupErrs...)
				return
			}
			if removeErr := deps.RemoveScenario(scenarioPath); removeErr != nil && !os.IsNotExist(removeErr) {
				cleanupErrs = append(cleanupErrs, fmt.Errorf("remove prepared scenario: %w", removeErr))
			}
			cleanupErr = errors.Join(cleanupErrs...)
		})
		return cleanupErr
	}
	fail := func(cause error) (*runcontrol.PreparedRun, error) {
		return nil, errors.Join(cause, cleanup(context.Background()))
	}

	scenarioText, err := deps.GenerateScenario(preparedParams)
	if err != nil {
		return fail(fmt.Errorf("generate scenario: %w", err))
	}
	defaultsYAML, err := deps.GenerateDefaults(preparedParams)
	if err != nil {
		return fail(fmt.Errorf("generate defaults: %w", err))
	}
	plan, planErr := deps.BuildStepPlan(scenarioText)
	if planErr != nil && scenario.IsIntegrityWorkload(preparedParams) {
		return fail(fmt.Errorf("build verification step plan: %w", planErr))
	}
	if planErr != nil {
		plan = scenario.StepPlan{}
	}
	var verifyPlan integrityplan.Plan
	if scenario.IsIntegrityWorkload(preparedParams) {
		if deps.BuildVerifyPlan == nil {
			return fail(fmt.Errorf("build typed verification plan: missing builder"))
		}
		verifyPlan, err = deps.BuildVerifyPlan(preparedParams, plan)
		if err != nil {
			return fail(fmt.Errorf("build typed verification plan: %w", err))
		}
	}

	scenarioBytes := []byte(scenarioText)
	if err := deps.WriteScenario(scenarioPath, scenarioBytes, 0o600); err != nil {
		return fail(fmt.Errorf("write prepared scenario: %w", err))
	}
	if verifyPlan.Valid() {
		return runcontrol.NewPreparedRun(
			preparedParams, scenarioBytes, defaultsYAML, plan, scenarioPath, cleanup, verifyPlan), nil
	}
	return runcontrol.NewPreparedRun(
		preparedParams, scenarioBytes, defaultsYAML, plan, scenarioPath, cleanup), nil
}
