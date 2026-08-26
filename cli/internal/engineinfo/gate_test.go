package engineinfo_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/engineinfo"
)

func TestGateAppliesExactForcePolicy(t *testing.T) {
	tests := []struct {
		name       string
		result     engineinfo.FleetResult
		collectErr error
		force      bool
		wantErr    bool
		wantForced bool
	}{
		{name: "consistent", result: fleetWithStatus(engineinfo.ConsistencyConsistent)},
		{name: "indeterminate", result: fleetWithStatus(engineinfo.ConsistencyIndeterminate)},
		{name: "mismatch rejected", result: fleetWithStatus(engineinfo.ConsistencyMismatch), wantErr: true},
		{name: "mismatch forced", result: fleetWithStatus(engineinfo.ConsistencyMismatch), force: true, wantForced: true},
		{name: "collection failure", result: fleetWithStatus(engineinfo.ConsistencyIndeterminate), collectErr: errors.New("transport exhausted"), wantErr: true},
		{name: "collection failure cannot be forced", result: fleetWithStatus(engineinfo.ConsistencyIndeterminate), collectErr: errors.New("malformed schema 1"), force: true, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			collector := gateCollector{result: test.result, err: test.collectErr}
			outcome, err := engineinfo.EvaluateGate(
				context.Background(), collector, []engineinfo.ParticipantDescriptor{}, test.force)
			if (err != nil) != test.wantErr {
				t.Fatalf("EvaluateGate() error = %v, wantErr %t", err, test.wantErr)
			}
			if outcome.Fleet.Consistency.Forced != test.wantForced {
				t.Fatalf("forced = %t, want %t", outcome.Fleet.Consistency.Forced, test.wantForced)
			}
			if test.wantForced && (!outcome.Proceed || !strings.Contains(outcome.Fleet.Consistency.Reason, "--force")) {
				t.Fatalf("forced outcome = %+v, want proceeding mismatch with force reason", outcome)
			}
			if test.wantErr && outcome.Proceed {
				t.Fatalf("failed outcome proceeds: %+v", outcome)
			}
			if test.collectErr != nil && outcome.Decision != engineinfo.GateCollectionFailure {
				t.Fatalf("collection decision = %q", outcome.Decision)
			}
		})
	}
}

func TestGatePreservesCollectorEvidenceOnFailure(t *testing.T) {
	want := fleetWithStatus(engineinfo.ConsistencyIndeterminate)
	want.Participants = []engineinfo.ParticipantResult{
		{NodeID: "entry.example:9999", Role: engineinfo.RoleEntry, CollectionStatus: engineinfo.StatusCollected},
		{NodeID: "worker.example:9999", Role: engineinfo.RoleWorker, CollectionStatus: engineinfo.StatusCollectionFailed, Reason: "engine version request failed"},
	}
	outcome, err := engineinfo.EvaluateGate(context.Background(), gateCollector{
		result: want,
		err:    errors.New("engine build information collection failed for 1 participant(s)"),
	}, nil, true)
	if err == nil {
		t.Fatal("EvaluateGate() error = nil, want collection failure")
	}
	if len(outcome.Fleet.Participants) != 2 || outcome.Fleet.Participants[1].CollectionStatus != engineinfo.StatusCollectionFailed {
		t.Fatalf("failure evidence = %+v, want every planned participant", outcome.Fleet.Participants)
	}
	if outcome.Fleet.Consistency.Forced {
		t.Fatal("collection failure was incorrectly forced")
	}
	if outcome.Decision != engineinfo.GateCollectionFailure {
		t.Fatalf("decision = %q, want collection failure", outcome.Decision)
	}
}

type gateCollector struct {
	result engineinfo.FleetResult
	err    error
}

func (c gateCollector) Collect(context.Context, []engineinfo.ParticipantDescriptor) (engineinfo.FleetResult, error) {
	return c.result, c.err
}

func fleetWithStatus(status engineinfo.ConsistencyStatus) engineinfo.FleetResult {
	return engineinfo.FleetResult{Consistency: engineinfo.ConsistencyAssessment{
		Status: status,
		Reason: "test assessment",
	}}
}
