package driverstats

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestValidateTerminalAcceptsNodeAndCompleteFleetStatistics(t *testing.T) {
	node := &Stats{Driver: "s3-rdma", Counters: map[string]int64{"rdma_transferred": 3}}
	if err := ValidateTerminal(node, 0); err != nil {
		t.Fatalf("node statistics were rejected: %v", err)
	}
	fleet := &Stats{Driver: "s3-rdma", Counters: map[string]int64{"rdma_transferred": 6}, Reporting: 2}
	if err := ValidateTerminal(fleet, 2); err != nil {
		t.Fatalf("complete fleet total was rejected: %v", err)
	}
}

func TestValidateTerminalRejectsStatisticsThatMustNotBeStored(t *testing.T) {
	tests := map[string]struct {
		stats             *Stats
		fleetContributors int
	}{
		"missing":                        {stats: nil, fleetContributors: 0},
		"no driver name":                 {stats: &Stats{Counters: map[string]int64{"a": 1}}, fleetContributors: 0},
		"partial total":                  {stats: &Stats{Driver: "s3-rdma", Reporting: 1, Partial: true}, fleetContributors: 2},
		"partial with every contributor": {stats: &Stats{Driver: "s3-rdma", Reporting: 2, Partial: true}, fleetContributors: 2},
		"different drivers":              {stats: &Stats{Partial: true, Drivers: []string{"s3", "s3-rdma"}}, fleetContributors: 2},
		"fewer contributors reporting":   {stats: &Stats{Driver: "s3-rdma", Reporting: 1}, fleetContributors: 2},
		"more contributors reporting":    {stats: &Stats{Driver: "s3-rdma", Reporting: 3}, fleetContributors: 2},
		"fleet total without a count":    {stats: &Stats{Driver: "s3-rdma"}, fleetContributors: 1},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if err := ValidateTerminal(tc.stats, tc.fleetContributors); err == nil {
				t.Fatal("statistics that must not be stored were accepted")
			}
		})
	}
}

func TestStatsKeepUnknownNamesThroughJSON(t *testing.T) {
	const row = `{"driver":"test-driver","counters":{"widgets_made":7,"zeta":0},"gauges":{"queue_depth":-2},` +
		`"reporting":2,"partial":false}`
	var stats Stats
	if err := json.Unmarshal([]byte(row), &stats); err != nil {
		t.Fatalf("decode: %v", err)
	}
	want := Stats{
		Driver:    "test-driver",
		Counters:  map[string]int64{"widgets_made": 7, "zeta": 0},
		Gauges:    map[string]int64{"queue_depth": -2},
		Reporting: 2,
	}
	if !reflect.DeepEqual(stats, want) {
		t.Fatalf("decoded statistics = %+v, want %+v", stats, want)
	}
	stored, err := json.Marshal(&stats)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var reloaded Stats
	if err := json.Unmarshal(stored, &reloaded); err != nil {
		t.Fatalf("decode stored statistics: %v", err)
	}
	if !reflect.DeepEqual(reloaded, want) {
		t.Fatalf("stored statistics = %+v, want %+v", reloaded, want)
	}
}
