/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

func TestPrepareRunBundleGeneratesEachInputOnce(t *testing.T) {
	var prepareCalls, scenarioCalls, defaultsCalls, planCalls, cleanupCalls int
	path := filepath.Join(t.TempDir(), "prepared.js")
	deps := runPreparationDependencies{
		PrepareExternal: func(params scenario.Params) (scenario.Params, error) {
			prepareCalls++
			params.ItemFileMounts = []scenario.FileMount{{HostPath: "/host/input", ContainerPath: "/input"}}
			return params, nil
		},
		CleanupExternal: func(scenario.Params) { cleanupCalls++ },
		GenerateScenario: func(scenario.Params) (string, error) {
			scenarioCalls++
			return "exact scenario", nil
		},
		GenerateDefaults: func(scenario.Params) ([]byte, error) {
			defaultsCalls++
			return []byte("exact defaults"), nil
		},
		BuildStepPlan: func(string) (scenario.StepPlan, error) {
			planCalls++
			return scenario.StepPlan{Steps: []scenario.StepInfo{{ID: "step-001"}}}, nil
		},
		WriteScenario:  os.WriteFile,
		RemoveScenario: os.Remove,
	}

	prepared, err := prepareRunBundleWithDependencies(
		scenario.Params{WorkloadType: scenario.WorkloadTypeWriteVerify, RunID: 42}, path, false, deps)
	if err != nil {
		t.Fatalf("prepareRunBundleWithDependencies() error = %v", err)
	}
	if prepareCalls != 1 || scenarioCalls != 1 || defaultsCalls != 1 || planCalls != 1 {
		t.Fatalf("calls prepare/scenario/defaults/plan = %d/%d/%d/%d", prepareCalls, scenarioCalls, defaultsCalls, planCalls)
	}
	onDisk, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(onDisk) != string(prepared.ScenarioJS()) || string(prepared.DefaultsYAML()) != "exact defaults" {
		t.Fatalf("prepared inputs differ: disk=%q scenario=%q defaults=%q", onDisk, prepared.ScenarioJS(), prepared.DefaultsYAML())
	}
	if got := prepared.FileMounts(); len(got) != 1 || got[0].HostPath != "/host/input" {
		t.Fatalf("file mounts = %+v", got)
	}
	if err := prepared.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := prepared.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if cleanupCalls != 1 {
		t.Fatalf("external cleanup calls = %d, want 1", cleanupCalls)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("scenario still exists after cleanup: %v", err)
	}
}

func TestPrepareRunBundleBlocksVerificationWhenPlanFails(t *testing.T) {
	planErr := errors.New("unparseable plan")
	var cleanupCalls, writeCalls int
	deps := runPreparationDependencies{
		PrepareExternal:  func(params scenario.Params) (scenario.Params, error) { return params, nil },
		CleanupExternal:  func(scenario.Params) { cleanupCalls++ },
		GenerateScenario: func(scenario.Params) (string, error) { return "scenario", nil },
		GenerateDefaults: func(scenario.Params) ([]byte, error) { return []byte("defaults"), nil },
		BuildStepPlan:    func(string) (scenario.StepPlan, error) { return scenario.StepPlan{}, planErr },
		WriteScenario: func(string, []byte, os.FileMode) error {
			writeCalls++
			return nil
		},
		RemoveScenario: os.Remove,
	}
	_, err := prepareRunBundleWithDependencies(
		scenario.Params{WorkloadType: scenario.WorkloadTypeReadVerify}, "unused.js", false, deps)
	if !errors.Is(err, planErr) {
		t.Fatalf("error = %v, want plan error", err)
	}
	if writeCalls != 0 {
		t.Fatalf("scenario writes = %d, want 0", writeCalls)
	}
	if cleanupCalls != 1 {
		t.Fatalf("external cleanup calls = %d, want 1", cleanupCalls)
	}
}
