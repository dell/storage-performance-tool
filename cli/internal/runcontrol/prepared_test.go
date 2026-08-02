/*
Copyright © 2026 Dell Technologies
*/

package runcontrol

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

func TestPreparedRunReturnsDetachedExactContent(t *testing.T) {
	params := scenario.Params{
		RunID:     42,
		Endpoints: []string{"one"},
		ItemFileMounts: []scenario.FileMount{{
			HostPath: "/host/items.csv", ContainerPath: "/spt-input/items/items.csv",
		}},
	}
	scenarioBytes := []byte("scenario-original")
	defaultsBytes := []byte("defaults-original")
	plan := scenario.StepPlan{Steps: []scenario.StepInfo{{ID: "step-001", Number: 1}}}
	prepared := NewPreparedRun(params, scenarioBytes, defaultsBytes, plan, "run.js", nil)

	params.Endpoints[0] = "mutated"
	scenarioBytes[0] = 'X'
	defaultsBytes[0] = 'X'
	plan.Steps[0].ID = "mutated"
	if got := string(prepared.ScenarioJS()); got != "scenario-original" {
		t.Fatalf("scenario = %q", got)
	}
	if got := string(prepared.DefaultsYAML()); got != "defaults-original" {
		t.Fatalf("defaults = %q", got)
	}
	if got := prepared.Params().Endpoints[0]; got != "one" {
		t.Fatalf("endpoint = %q", got)
	}
	if got := prepared.Plan().Steps[0].ID; got != "step-001" {
		t.Fatalf("step id = %q", got)
	}
	if prepared.RunID() != 42 || prepared.ScenarioPath() != "run.js" {
		t.Fatalf("identity/path = %d/%q", prepared.RunID(), prepared.ScenarioPath())
	}
}

func TestPreparedRunCleanupRunsExactlyOnce(t *testing.T) {
	var calls atomic.Int32
	prepared := NewPreparedRun(
		scenario.Params{}, nil, nil, scenario.StepPlan{}, "",
		func(context.Context) error {
			calls.Add(1)
			return nil
		},
	)

	const callers = 8
	var wg sync.WaitGroup
	wg.Add(callers)
	for range callers {
		go func() {
			defer wg.Done()
			if err := prepared.Cleanup(context.Background()); err != nil {
				t.Errorf("Cleanup() error = %v", err)
			}
		}()
	}
	wg.Wait()
	if got := calls.Load(); got != 1 {
		t.Fatalf("cleanup calls = %d, want 1", got)
	}
}
