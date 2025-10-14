package scenario

import (
	"regexp"
	"strings"
	"testing"
)

var stepIDRe = regexp.MustCompile(`mt-([0-9]{3})-([0-9]{8}\.[0-9]{6}\.[0-9]{3})-([a-z0-9_-]+)`) // step, ts, op

func TestMockCleanup_StepIDsStableAndNumbered(t *testing.T) {
	scenarioStr, err := GenerateMockScenario(Params{
		WorkloadType: "mock",
		Threads:      2,
		ObjectSize:   "1MB",
		ObjectCount:  10,
		Cleanup:      true,
	})
	if err != nil {
		t.Fatalf("GenerateMockScenario error: %v", err)
	}

	// Extract all step ids from the scenario text
	ids := stepIDRe.FindAllStringSubmatch(scenarioStr, -1)
	if len(ids) < 2 {
		t.Fatalf("expected at least 2 step ids in cleanup scenario, got %d\nScenario:\n%s", len(ids), scenarioStr)
	}

	// We expect at least 2: create then delete
	first := ids[0]
	second := ids[1]

	if first[1] != "001" || second[1] != "002" {
		t.Fatalf("expected step numbers 001 and 002, got %s and %s", first[1], second[1])
	}

	if first[3] != "create" || second[3] != "delete" {
		t.Fatalf("expected ops create/delete, got %s/%s", first[3], second[3])
	}

	if first[2] != second[2] {
		t.Fatalf("expected shared timestamp, got %s and %s", first[2], second[2])
	}
}

func TestWriteCount_StepIDPresent(t *testing.T) {
	s, err := GenerateWriteScenario(Params{
		WorkloadType: "write",
		Endpoint:     "http://minio:9000",
		AccessKey:    "k",
		SecretKey:    "s",
		Bucket:       "b",
		Threads:      1,
		ObjectSize:   "1MB",
		ObjectCount:  10,
		Cleanup:      false,
	})
	if err != nil {
		t.Fatalf("GenerateWriteScenario error: %v", err)
	}

	// Find the first step id
	m := stepIDRe.FindStringSubmatch(s)
	if m == nil {
		t.Fatalf("no step id found in write scenario\nScenario:\n%s", s)
	}
	if m[1] != "001" {
		t.Fatalf("expected step number 001, got %s", m[1])
	}
	if m[3] != "create" {
		t.Fatalf("expected op create, got %s", m[3])
	}
	if !strings.Contains(s, m[0]) {
		t.Fatalf("expected step id %s to be present in scenario", m[0])
	}
}

func TestListScenario_StepIDSuffix(t *testing.T) {
	s, err := GenerateListScenario(Params{
		WorkloadType: "list",
		Bucket:       "logs",
		Threads:      2,
	})
	if err != nil {
		t.Fatalf("GenerateListScenario error: %v", err)
	}

	matches := stepIDRe.FindAllStringSubmatch(s, -1)
	if len(matches) != 1 {
		t.Fatalf("expected exactly 1 step id, got %d\nScenario:\n%s", len(matches), s)
	}
	if matches[0][3] != "list" {
		t.Fatalf("expected step suffix 'list', got %s", matches[0][3])
	}
}
