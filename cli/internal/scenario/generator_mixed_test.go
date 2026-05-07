package scenario

import (
	"strings"
	"testing"
)

func TestGenerateMixedScenario(t *testing.T) {
	baseParams := Params{
		WorkloadType:  workloadTypeMixed,
		Endpoint:      "http://s3.example.com:9000",
		AccessKey:     "testaccess",
		SecretKey:     "testsecret",
		Bucket:        "testbucket",
		Threads:       80,
		ObjectSize:    "100MB",
		Duration:      "5m",
		SeedCount:     2500,
		GetDistrib:    45,
		PutDistrib:    30,
		DeleteDistrib: 15,
		StatDistrib:   10,
		BaseTimestamp: "20260411.120000.000",
	}

	tests := []struct {
		name        string
		params      Params
		wantErr     bool
		mustContain []string
		mustAbsent  []string
	}{
		{
			name:   "standard 4-op: seed + mixed + no cleanup",
			params: baseParams,
			mustContain: []string{
				`var concurrency = 80`,
				`var itemSize = "100MB"`,
				`var duration = "5m"`,
				`var seedCount = 2500`,
				`PreconditionLoad`,
				`"count": seedCount`,
				`MixedLoad`,
				`"weights": {`,
				`"get": 45`,
				`"put": 30`,
				`"delete": 15`,
				`"stat": 10`,
				`"time": duration`,
				`mt-001-20260411.120000.000-seed`,
				`mt-002-20260411.120000.000-mixed`,
			},
			mustAbsent: []string{
				`DeleteLoad`,
			},
		},
		{
			name:   "with cleanup: seed + mixed + two cleanup phases",
			params: func() Params { p := baseParams; p.Cleanup = true; return p }(),
			mustContain: []string{
				`PreconditionLoad`,
				`MixedLoad`,
				`DeleteLoad`,
				`mt-001-20260411.120000.000-seed`,
				`mt-002-20260411.120000.000-mixed`,
				`mt-003-20260411.120000.000-cleanup-seed`,
				`mt-004-20260411.120000.000-cleanup-put`,
				`items.csv`,
				`put-remaining.csv`,
			},
		},
		{
			name: "with read-items-file: no seed phase",
			params: func() Params {
				p := baseParams
				p.ReadItemsFile = "/data/my-items.csv"
				p.SeedCount = 0
				return p
			}(),
			mustContain: []string{
				`MixedLoad`,
				`"/data/my-items.csv"`,
				`mt-001-20260411.120000.000-mixed`,
			},
			mustAbsent: []string{
				`PreconditionLoad`,
			},
		},
		{
			name: "with delete-items-file: mixed config includes it",
			params: func() Params {
				p := baseParams
				p.DeleteItemsFile = "/data/delete-seed.csv"
				return p
			}(),
			mustContain: []string{
				`PreconditionLoad`,
				`MixedLoad`,
				`"/data/delete-seed.csv"`,
			},
		},
		{
			name: "zero-weight ops still emitted with value 0",
			params: func() Params {
				p := baseParams
				p.GetDistrib = 75
				p.PutDistrib = 25
				p.DeleteDistrib = 0
				p.StatDistrib = 0
				return p
			}(),
			mustContain: []string{
				`"get": 75`,
				`"put": 25`,
				`"delete": 0`,
				`"stat": 0`,
			},
		},
		{
			name: "GET+STAT scenario (no PUT/DELETE)",
			params: func() Params {
				p := baseParams
				p.GetDistrib = 60
				p.PutDistrib = 0
				p.DeleteDistrib = 0
				p.StatDistrib = 40
				return p
			}(),
			mustContain: []string{
				`"get": 60`,
				`"stat": 40`,
				`"put": 0`,
				`"delete": 0`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := GenerateMixedScenario(tt.params)
			if (err != nil) != tt.wantErr {
				t.Fatalf("GenerateMixedScenario() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}

			for _, want := range tt.mustContain {
				if !strings.Contains(result, want) {
					t.Errorf("scenario missing expected string %q\nscenario:\n%s", want, result)
				}
			}
			for _, absent := range tt.mustAbsent {
				if strings.Contains(result, absent) {
					t.Errorf("scenario unexpectedly contains %q\nscenario:\n%s", absent, result)
				}
			}
		})
	}
}

func TestGenerateMixedScenarioPlanParseable(t *testing.T) {
	params := Params{
		WorkloadType:  workloadTypeMixed,
		Endpoint:      "http://s3.example.com:9000",
		AccessKey:     "ak",
		SecretKey:     "sk",
		Bucket:        "b",
		Threads:       8,
		ObjectSize:    "1MB",
		Duration:      "2m",
		SeedCount:     100,
		GetDistrib:    45,
		PutDistrib:    30,
		DeleteDistrib: 15,
		StatDistrib:   10,
		Cleanup:       true,
		BaseTimestamp: "20260411.120000.000",
	}

	scenario, err := GenerateMixedScenario(params)
	if err != nil {
		t.Fatalf("GenerateMixedScenario() error: %v", err)
	}

	plan, err := BuildStepPlanFromScenario(scenario)
	if err != nil {
		t.Fatalf("BuildStepPlanFromScenario() error: %v\nscenario:\n%s", err, scenario)
	}

	if len(plan.Steps) != 4 {
		t.Errorf("expected 4 steps (seed, mixed, cleanup-seed, cleanup-put), got %d: %v", len(plan.Steps), plan.Steps)
	}

	wantOps := []string{"seed", "mixed", "cleanup-seed", "cleanup-put"}
	for i, step := range plan.Steps {
		if i >= len(wantOps) {
			break
		}
		if step.Op != wantOps[i] {
			t.Errorf("step[%d] op = %q, want %q", i, step.Op, wantOps[i])
		}
	}
}

func TestGenerateMixedScenarioViaSwitch(t *testing.T) {
	params := Params{
		WorkloadType:  workloadTypeMixed,
		Endpoint:      "http://s3.example.com:9000",
		AccessKey:     "ak",
		SecretKey:     "sk",
		Bucket:        "b",
		Threads:       4,
		ObjectSize:    "1MB",
		Duration:      "2m",
		SeedCount:     100,
		GetDistrib:    70,
		PutDistrib:    30,
		DeleteDistrib: 0,
		BaseTimestamp: "20260411.120000.000",
	}

	result, err := GenerateScenario(params)
	if err != nil {
		t.Fatalf("GenerateScenario() error: %v", err)
	}
	if !strings.Contains(result, "MixedLoad") {
		t.Errorf("expected MixedLoad in scenario, got:\n%s", result)
	}
}
