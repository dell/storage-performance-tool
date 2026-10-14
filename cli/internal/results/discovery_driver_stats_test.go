package results

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/driverstats"
)

const (
	driverStatsRunID      = int64(17)
	driverStatsPriorRunID = int64(16)
)

var driverStatsFleetContributors = []string{"local", "node-a"}

func TestCaptureTerminalDriverStatsReadsFleetTotalOfMultiHostRun(t *testing.T) {
	server := driverStatsServer(
		metricsRows(
			driverStatsRow(driverStatsRunID, "read-current", "READ", "fleet", "aggregate", fleetDriverStats(2)),
		),
		// A fleet answer is authoritative: the node row must not be read.
		metricsRows(
			driverStatsRow(driverStatsRunID, "read-current", "READ", "node", "entry", map[string]any{
				"driver": "s3-rdma", "counters": map[string]int64{"rdma_transferred": 1},
			}),
		))
	defer server.Close()

	captured, err := CaptureTerminalDriverStatsForRunContext(
		context.Background(), server.URL, driverStatsRunID, []string{"read-current"},
		driverStatsFleetContributors, false)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]*driverstats.Stats{"read-current": {
		Driver:    "s3-rdma",
		Counters:  map[string]int64{"rdma_transferred": 120, "http_fallback": 0},
		Gauges:    map[string]int64{"pool_reserved_bytes": 8192, "transport_available": 2},
		Reporting: 2,
	}}
	if !reflect.DeepEqual(captured, want) {
		t.Fatalf("captured fleet statistics = %s, want %s", asJSON(t, captured), asJSON(t, want))
	}
}

func TestCaptureTerminalDriverStatsReadsSingleNodeRun(t *testing.T) {
	nodeStats := map[string]any{
		"driver":   "s3-rdma",
		"counters": map[string]int64{"rdma_transferred": 60},
		"gauges":   map[string]int64{"transport_available": 1},
	}
	want := &driverstats.Stats{
		Driver:   "s3-rdma",
		Counters: map[string]int64{"rdma_transferred": 60},
		Gauges:   map[string]int64{"transport_available": 1},
	}

	t.Run("fleet total of one contributor", func(t *testing.T) {
		row := driverStatsRow(driverStatsRunID, "create-current", "CREATE", "fleet", "aggregate", map[string]any{
			"driver":    "s3-rdma",
			"counters":  map[string]int64{"rdma_transferred": 60},
			"gauges":    map[string]int64{"transport_available": 1},
			"reporting": 1,
			"partial":   false,
		})
		row["nodes_count"] = 1
		row["contributors_present"] = []string{"local"}
		server := driverStatsServer(metricsRows(row), endpointNotFound)
		defer server.Close()

		captured, err := CaptureTerminalDriverStatsForRunContext(
			context.Background(), server.URL, driverStatsRunID, []string{"create-current"}, []string{"local"}, true)
		if err != nil {
			t.Fatal(err)
		}
		wantFleet := *want
		wantFleet.Reporting = 1
		if !reflect.DeepEqual(captured, map[string]*driverstats.Stats{"create-current": &wantFleet}) {
			t.Fatalf("captured statistics = %s", asJSON(t, captured))
		}
	})

	t.Run("node row when the fleet endpoint does not exist", func(t *testing.T) {
		server := driverStatsServer(endpointNotFound, metricsRows(
			driverStatsRow(driverStatsRunID, "create-current", "CREATE", "node", "entry", nodeStats)))
		defer server.Close()

		captured, err := CaptureTerminalDriverStatsForRunContext(
			context.Background(), server.URL, driverStatsRunID, []string{"create-current"}, []string{"local"}, true)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(captured, map[string]*driverstats.Stats{"create-current": want}) {
			t.Fatalf("captured statistics = %s", asJSON(t, captured))
		}
	})
}

func TestCaptureTerminalDriverStatsCoversEveryOperationOfARun(t *testing.T) {
	// One driver serves all operations of a mixed step, so one of its rows carries the statistics.
	rows := []any{
		driverStatsRow(driverStatsRunID, "seed", "CREATE", "fleet", "aggregate", fleetDriverStats(2)),
		driverStatsRow(driverStatsRunID, "read", "READ", "fleet", "aggregate", fleetDriverStats(2)),
		driverStatsRow(driverStatsRunID, "mixed", "READ", "fleet", "aggregate", nil),
		driverStatsRow(driverStatsRunID, "mixed", "CREATE", "fleet", "aggregate", fleetDriverStats(2)),
		driverStatsRow(driverStatsRunID, "mixed", "DELETE", "fleet", "aggregate", nil),
		driverStatsRow(driverStatsRunID, "cleanup", "DELETE", "fleet", "aggregate", fleetDriverStats(2)),
	}
	server := driverStatsServer(metricsRows(rows...), endpointNotFound)
	defer server.Close()

	steps := []string{"seed", "read", "mixed", "cleanup"}
	captured, err := CaptureTerminalDriverStatsForRunContext(
		context.Background(), server.URL, driverStatsRunID, steps, driverStatsFleetContributors, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(captured) != len(steps) {
		t.Fatalf("captured %d steps, want %d: %s", len(captured), len(steps), asJSON(t, captured))
	}
	for _, stepID := range steps {
		if captured[stepID] == nil || captured[stepID].Counters["rdma_transferred"] != 120 {
			t.Fatalf("step %q statistics = %s", stepID, asJSON(t, captured[stepID]))
		}
	}
}

func TestCaptureTerminalDriverStatsIgnoresRowsOfOtherRuns(t *testing.T) {
	stale := fleetDriverStats(2)
	stale["counters"] = map[string]int64{"rdma_transferred": 999}
	staleRow := driverStatsRow(driverStatsPriorRunID, "read-current", "READ", "fleet", "aggregate", stale)
	// A retained row of an earlier run need not satisfy any check of this run.
	staleRow["terminal"] = false
	unbound := driverStatsRow(driverStatsRunID, "read-unbound", "READ", "fleet", "aggregate", stale)
	delete(unbound, "run_id")
	server := driverStatsServer(
		metricsRows(
			staleRow,
			unbound,
			driverStatsRow(driverStatsPriorRunID, "read-prior", "READ", "fleet", "aggregate", stale),
			driverStatsRow(driverStatsRunID, "read-current", "READ", "fleet", "aggregate", fleetDriverStats(2)),
		),
		endpointNotFound)
	defer server.Close()

	captured, err := CaptureTerminalDriverStatsForRunContext(
		context.Background(), server.URL, driverStatsRunID, []string{"read-current"},
		driverStatsFleetContributors, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(captured) != 1 || captured["read-current"] == nil ||
		captured["read-current"].Counters["rdma_transferred"] != 120 {
		t.Fatalf("captured statistics = %s", asJSON(t, captured))
	}
}

func TestCaptureTerminalDriverStatsIsEmptyWhenNothingIsReported(t *testing.T) {
	nodeWithStats := metricsRows(driverStatsRow(driverStatsRunID, "read-current", "READ", "node", "entry",
		map[string]any{"driver": "s3-rdma", "counters": map[string]int64{"rdma_transferred": 1}}))
	nullStats := driverStatsRow(driverStatsRunID, "read-current", "READ", "fleet", "aggregate", nil)
	nullStats["driver_stats"] = nil
	tests := map[string]struct {
		fleet http.HandlerFunc
		node  http.HandlerFunc
	}{
		"engine without driver statistics": {
			fleet: metricsRows(driverStatsRow(driverStatsRunID, "read-current", "READ", "fleet", "aggregate", nil)),
			node:  endpointNotFound,
		},
		"null statistics": {fleet: metricsRows(nullStats), node: endpointNotFound},
		"no rows":         {fleet: metricsRows(), node: endpointNotFound},
		// The fleet endpoint answered, so its answer stands: no fall back to a node row.
		"fleet without statistics beside a node row with them": {
			fleet: metricsRows(driverStatsRow(driverStatsRunID, "read-current", "READ", "fleet", "aggregate", nil)),
			node:  nodeWithStats,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			server := driverStatsServer(tc.fleet, tc.node)
			defer server.Close()
			captured, err := CaptureTerminalDriverStatsForRunContext(
				context.Background(), server.URL, driverStatsRunID, []string{"read-current"}, []string{"local"}, true)
			if err != nil {
				t.Fatalf("a run without driver statistics was refused: %v", err)
			}
			if len(captured) != 0 {
				t.Fatalf("captured statistics = %s, want none", asJSON(t, captured))
			}
		})
	}
}

func TestCaptureTerminalDriverStatsKeepsUnknownNames(t *testing.T) {
	server := driverStatsServer(endpointNotFound, metricsRows(
		driverStatsRow(driverStatsRunID, "step", "CREATE", "node", "worker", map[string]any{
			"driver":   "test-driver",
			"counters": map[string]int64{"widgets_made": 7, "zeta": 0},
			"gauges":   map[string]int64{"queue_depth": -2},
		})))
	defer server.Close()

	captured, err := CaptureTerminalDriverStatsForRunContext(
		context.Background(), server.URL, driverStatsRunID, []string{"step"}, []string{"local"}, true)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]*driverstats.Stats{"step": {
		Driver:   "test-driver",
		Counters: map[string]int64{"widgets_made": 7, "zeta": 0},
		Gauges:   map[string]int64{"queue_depth": -2},
	}}
	if !reflect.DeepEqual(captured, want) {
		t.Fatalf("captured statistics = %s, want %s", asJSON(t, captured), asJSON(t, want))
	}
}

func TestCaptureTerminalDriverStatsFailsClosedOnInvalidFleetResponse(t *testing.T) {
	for name, fleet := range invalidFleetDriverStatsResponses() {
		t.Run(name, func(t *testing.T) {
			server := driverStatsServer(fleet, endpointNotFound)
			defer server.Close()
			captured, err := CaptureTerminalDriverStatsForRunContext(
				context.Background(), server.URL, driverStatsRunID, []string{"read-current"},
				driverStatsFleetContributors, false)
			if err == nil {
				t.Fatalf("invalid fleet response was accepted: %s", asJSON(t, captured))
			}
			if captured != nil {
				t.Fatalf("refused capture returned statistics: %s", asJSON(t, captured))
			}
		})
	}
}

// invalidFleetDriverStatsResponses are fleet responses for the expected step "read-current"
// that a terminal capture refuses, by what is wrong with them.
func invalidFleetDriverStatsResponses() map[string]http.HandlerFunc {
	valid := func() map[string]any {
		return driverStatsRow(driverStatsRunID, "read-current", "READ", "fleet", "aggregate", fleetDriverStats(2))
	}
	with := func(mutate func(row map[string]any)) http.HandlerFunc {
		row := valid()
		mutate(row)
		return metricsRows(row)
	}
	withStats := func(mutate func(stats map[string]any)) http.HandlerFunc {
		stats := fleetDriverStats(2)
		mutate(stats)
		return metricsRows(driverStatsRow(driverStatsRunID, "read-current", "READ", "fleet", "aggregate", stats))
	}
	return map[string]http.HandlerFunc{
		"malformed response": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`[{"run_id":"17","step_id":"read-current","driver_stats":`))
		},
		"server error": func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		},
		"statistics are not an object": with(func(row map[string]any) { row["driver_stats"] = "s3-rdma" }),
		"counter is not a whole number": withStats(func(stats map[string]any) {
			stats["counters"] = map[string]any{"rdma_transferred": "many"}
		}),
		"not terminal":       with(func(row map[string]any) { row["terminal"] = false }),
		"wrong scope":        with(func(row map[string]any) { row["scope"] = "node" }),
		"wrong role":         with(func(row map[string]any) { row["role"] = "worker" }),
		"wrong cluster":      with(func(row map[string]any) { row["cluster_id"] = "other-cluster" }),
		"partial row":        with(func(row map[string]any) { row["partial"] = true }),
		"missing node count": with(func(row map[string]any) { delete(row, "nodes_count") }),
		"missing contributor": with(func(row map[string]any) {
			row["nodes_count"] = 1
			row["contributors_present"] = []string{"local"}
		}),
		"missing contributor identities": with(func(row map[string]any) { delete(row, "contributors_present") }),
		"duplicate contributor": with(func(row map[string]any) {
			row["contributors_present"] = []string{"local", "local"}
		}),
		"stale contributor": with(func(row map[string]any) {
			row["contributors_present"] = []string{"local", "node-stale"}
		}),
		"malformed run identity": with(func(row map[string]any) { row["run_id"] = map[string]any{"id": 17} }),
		"partial statistics": withStats(func(stats map[string]any) {
			stats["reporting"] = 1
			stats["partial"] = true
		}),
		"partial statistics from every contributor": withStats(func(stats map[string]any) { stats["partial"] = true }),
		"different drivers": metricsRows(driverStatsRow(driverStatsRunID, "read-current", "READ", "fleet", "aggregate",
			map[string]any{"partial": true, "drivers": []string{"s3", "s3-rdma"}})),
		"fewer contributors reporting": withStats(func(stats map[string]any) { stats["reporting"] = 1 }),
		"no contributor count":         withStats(func(stats map[string]any) { delete(stats, "reporting") }),
		"no driver name":               withStats(func(stats map[string]any) { delete(stats, "driver") }),
		"unexpected current-run step": metricsRows(
			valid(),
			driverStatsRow(driverStatsRunID, "read-unplanned", "READ", "fleet", "aggregate", fleetDriverStats(2))),
		"two rows with statistics for one step": metricsRows(
			valid(),
			driverStatsRow(driverStatsRunID, "read-current", "CREATE", "fleet", "aggregate", fleetDriverStats(2))),
	}
}

func TestCaptureTerminalDriverStatsDoesNotFallBackWhenFleetAnswered(t *testing.T) {
	// Everything the node fallback needs is in place: one local contributor, fallback allowed
	// and a valid node row. Only a missing fleet endpoint may use it.
	partial := driverStatsRow(driverStatsRunID, "read-current", "READ", "fleet", "aggregate", map[string]any{
		"driver": "s3-rdma", "counters": map[string]int64{}, "gauges": map[string]int64{},
		"reporting": 0, "partial": true,
	})
	partial["nodes_count"] = 1
	partial["contributors_present"] = []string{"local"}
	tests := map[string]http.HandlerFunc{
		"server error": func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		},
		"malformed response": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"error":"not ready"}`))
		},
		"partial statistics": metricsRows(partial),
	}
	for name, fleet := range tests {
		t.Run(name, func(t *testing.T) {
			server := driverStatsServer(fleet, metricsRows(
				driverStatsRow(driverStatsRunID, "read-current", "READ", "node", "entry", map[string]any{
					"driver": "s3-rdma", "counters": map[string]int64{"rdma_transferred": 1},
				})))
			defer server.Close()
			captured, err := CaptureTerminalDriverStatsForRunContext(
				context.Background(), server.URL, driverStatsRunID, []string{"read-current"}, []string{"local"}, true)
			if err == nil || captured != nil {
				t.Fatalf("node row replaced an invalid fleet answer: err=%v captured=%s", err, asJSON(t, captured))
			}
		})
	}
}

func TestCaptureTerminalDriverStatsStoresNothingWhenOneStepFails(t *testing.T) {
	partial := fleetDriverStats(1)
	partial["partial"] = true
	server := driverStatsServer(
		metricsRows(
			driverStatsRow(driverStatsRunID, "seed", "CREATE", "fleet", "aggregate", fleetDriverStats(2)),
			driverStatsRow(driverStatsRunID, "read", "READ", "fleet", "aggregate", partial),
		),
		endpointNotFound)
	defer server.Close()

	captured, err := CaptureTerminalDriverStatsForRunContext(
		context.Background(), server.URL, driverStatsRunID, []string{"seed", "read"},
		driverStatsFleetContributors, false)
	if err == nil || captured != nil {
		t.Fatalf("partial statistics were not refused as a whole: err=%v captured=%s", err, asJSON(t, captured))
	}
}

func TestCaptureTerminalDriverStatsRestrictsNodeFallback(t *testing.T) {
	nodeRow := func(role string) http.HandlerFunc {
		return metricsRows(driverStatsRow(driverStatsRunID, "read-current", "READ", "node", role, map[string]any{
			"driver": "s3-rdma", "counters": map[string]int64{"rdma_transferred": 1},
		}))
	}
	tests := map[string]struct {
		node              http.HandlerFunc
		contributors      []string
		allowNodeFallback bool
	}{
		"multi-node run":             {node: nodeRow("entry"), contributors: driverStatsFleetContributors},
		"fallback not allowed":       {node: nodeRow("entry"), contributors: []string{"local"}},
		"single remote contributor":  {node: nodeRow("entry"), contributors: []string{"node-a"}, allowNodeFallback: true},
		"several with fallback":      {node: nodeRow("entry"), contributors: driverStatsFleetContributors, allowNodeFallback: true},
		"aggregate role on node row": {node: nodeRow("aggregate"), contributors: []string{"local"}, allowNodeFallback: true},
		"node endpoint missing":      {node: endpointNotFound, contributors: []string{"local"}, allowNodeFallback: true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			server := driverStatsServer(endpointNotFound, tc.node)
			defer server.Close()
			if captured, err := CaptureTerminalDriverStatsForRunContext(
				context.Background(), server.URL, driverStatsRunID, []string{"read-current"},
				tc.contributors, tc.allowNodeFallback); err == nil {
				t.Fatalf("unproven node fallback was accepted: %s", asJSON(t, captured))
			}
		})
	}
}

func TestCaptureTerminalDriverStatsRequiresExactExpectedIdentity(t *testing.T) {
	tests := map[string]struct {
		runID        int64
		steps        []string
		contributors []string
	}{
		"no run identity":        {runID: 0, steps: []string{"read-current"}, contributors: []string{"local"}},
		"no steps":               {runID: driverStatsRunID, contributors: []string{"local"}},
		"empty step":             {runID: driverStatsRunID, steps: []string{"read-current", " "}, contributors: []string{"local"}},
		"duplicate step":         {runID: driverStatsRunID, steps: []string{"read", "read"}, contributors: []string{"local"}},
		"no contributors":        {runID: driverStatsRunID, steps: []string{"read-current"}},
		"empty contributor":      {runID: driverStatsRunID, steps: []string{"read-current"}, contributors: []string{"local", " "}},
		"duplicate contributors": {runID: driverStatsRunID, steps: []string{"read-current"}, contributors: []string{"local", "local"}},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := CaptureTerminalDriverStatsForRunContext(
				context.Background(), "http://127.0.0.1:1", tc.runID, tc.steps, tc.contributors, false); err == nil {
				t.Fatal("invalid expected identity was accepted")
			}
		})
	}
}

func TestObserveTerminalDriverStatsLeavesOutStepsThatAreStillRunning(t *testing.T) {
	running := driverStatsRow(driverStatsRunID, "read", "READ", "fleet", "aggregate", fleetDriverStats(2))
	running["terminal"] = false
	// The run learns the identifiers of its steps while it is monitored: a step can be running
	// before the caller can name it.
	notNamedYet := driverStatsRow(driverStatsRunID, "started-just-now", "READ", "fleet", "aggregate", fleetDriverStats(2))
	notNamedYet["terminal"] = false
	server := driverStatsServer(
		metricsRows(
			driverStatsRow(driverStatsRunID, "seed", "CREATE", "fleet", "aggregate", fleetDriverStats(2)),
			running,
			notNamedYet,
			// A step that has not started has no row; a step of a driver that reports nothing has
			// a row without statistics.
			driverStatsRow(driverStatsRunID, "cleanup", "DELETE", "fleet", "aggregate", nil),
		),
		endpointNotFound)
	defer server.Close()

	observation, err := ObserveTerminalDriverStatsForRunContext(
		context.Background(), server.URL, driverStatsRunID, []string{"seed", "read", "cleanup", "later"},
		driverStatsFleetContributors, false)
	if err != nil {
		t.Fatalf("observe a run in progress: %v", err)
	}
	want := map[string]*driverstats.Stats{"seed": {
		Driver:    "s3-rdma",
		Counters:  map[string]int64{"rdma_transferred": 120, "http_fallback": 0},
		Gauges:    map[string]int64{"pool_reserved_bytes": 8192, "transport_available": 2},
		Reporting: 2,
	}}
	if !reflect.DeepEqual(observation.Steps, want) {
		t.Fatalf("observed statistics = %s, want %s", asJSON(t, observation.Steps), asJSON(t, want))
	}
	if len(observation.Refused) != 0 {
		t.Fatalf("a run in progress was refused: %v", observation.Refused)
	}

	// The same response is not a terminal capture: the running step refuses it.
	captured, err := CaptureTerminalDriverStatsForRunContext(
		context.Background(), server.URL, driverStatsRunID, []string{"seed", "read", "cleanup", "later"},
		driverStatsFleetContributors, false)
	if err == nil || captured != nil {
		t.Fatalf("terminal capture accepted a running step: err=%v captured=%s", err, asJSON(t, captured))
	}
}

func TestObserveTerminalDriverStatsJudgesEachStepOnItsOwn(t *testing.T) {
	partial := fleetDriverStats(1)
	partial["partial"] = true
	server := driverStatsServer(
		metricsRows(
			driverStatsRow(driverStatsRunID, "seed", "CREATE", "fleet", "aggregate", fleetDriverStats(2)),
			driverStatsRow(driverStatsRunID, "read", "READ", "fleet", "aggregate", partial),
		),
		endpointNotFound)
	defer server.Close()

	observation, err := ObserveTerminalDriverStatsForRunContext(
		context.Background(), server.URL, driverStatsRunID, []string{"seed", "read"},
		driverStatsFleetContributors, false)
	if err != nil {
		t.Fatalf("observe: %v", err)
	}
	if _, kept := observation.Steps["seed"]; !kept || len(observation.Steps) != 1 {
		t.Fatalf("observed statistics = %s, want those of the valid step only", asJSON(t, observation.Steps))
	}
	if refusal := observation.Refused["read"]; refusal == nil || len(observation.Refused) != 1 ||
		!strings.Contains(refusal.Error(), `"read"`) {
		t.Fatalf("refused steps = %v, want the partial step with its reason", observation.Refused)
	}
}

func TestObserveTerminalDriverStatsRefusesWhatTheCaptureRefuses(t *testing.T) {
	// What the observation makes of each response the capture refuses: it cannot be read, its
	// step is still running, or the named step is refused.
	const unreadable, running = "unreadable", "running"
	outcomes := map[string]string{
		"malformed response":          unreadable,
		"server error":                unreadable,
		"not terminal":                running,
		"unexpected current-run step": "read-unplanned",
	}
	for name, fleet := range invalidFleetDriverStatsResponses() {
		t.Run(name, func(t *testing.T) {
			server := driverStatsServer(fleet, endpointNotFound)
			defer server.Close()
			observation, err := ObserveTerminalDriverStatsForRunContext(
				context.Background(), server.URL, driverStatsRunID, []string{"read-current"},
				driverStatsFleetContributors, false)
			outcome, listed := outcomes[name]
			if !listed {
				outcome = "read-current"
			}
			switch outcome {
			case unreadable:
				if err == nil || observation != nil {
					t.Fatalf("unreadable response was observed: err=%v observation=%+v", err, observation)
				}
			case running:
				if err != nil || len(observation.Steps) != 0 || len(observation.Refused) != 0 {
					t.Fatalf("running step was judged: err=%v observation=%+v", err, observation)
				}
			default:
				if err != nil {
					t.Fatalf("observe: %v", err)
				}
				if observation.Refused[outcome] == nil || len(observation.Refused) != 1 {
					t.Fatalf("refused steps = %v, want %q", observation.Refused, outcome)
				}
				if _, kept := observation.Steps[outcome]; kept {
					t.Fatalf("refused step %q also has statistics: %s", outcome, asJSON(t, observation.Steps))
				}
			}
		})
	}
}

func TestObserveTerminalDriverStatsFollowsTheEndpointRuleOfTheCapture(t *testing.T) {
	nodeStats := map[string]any{"driver": "s3-rdma", "counters": map[string]int64{"http_rdma_unavailable": 9}}
	node := metricsRows(driverStatsRow(driverStatsRunID, "create-current", "CREATE", "node", "entry", nodeStats))

	t.Run("node row of a single local engine without a fleet endpoint", func(t *testing.T) {
		server := driverStatsServer(endpointNotFound, node)
		defer server.Close()
		observation, err := ObserveTerminalDriverStatsForRunContext(
			context.Background(), server.URL, driverStatsRunID, []string{"create-current"}, []string{"local"}, true)
		if err != nil {
			t.Fatalf("observe: %v", err)
		}
		if got := observation.Steps["create-current"]; got == nil || got.Counters["http_rdma_unavailable"] != 9 {
			t.Fatalf("observed statistics = %s, want the node row", asJSON(t, observation.Steps))
		}
	})
	t.Run("no node fallback for a fleet", func(t *testing.T) {
		server := driverStatsServer(endpointNotFound, node)
		defer server.Close()
		observation, err := ObserveTerminalDriverStatsForRunContext(
			context.Background(), server.URL, driverStatsRunID, []string{"create-current"},
			driverStatsFleetContributors, false)
		if err == nil || observation != nil {
			t.Fatalf("fleet observation fell back to a node row: err=%v observation=%+v", err, observation)
		}
	})
	t.Run("expected identity is required", func(t *testing.T) {
		server := driverStatsServer(node, node)
		defer server.Close()
		observation, err := ObserveTerminalDriverStatsForRunContext(
			context.Background(), server.URL, driverStatsRunID, nil, []string{"local"}, true)
		if err == nil || observation != nil {
			t.Fatalf("observation without expected steps was accepted: err=%v observation=%+v", err, observation)
		}
	})
}

// driverStatsRow builds a terminal metrics row as the engine writes it; stats is its
// driver_stats object, or nil for a row without one.
func driverStatsRow(runID int64, stepID, opType, scope, role string, stats map[string]any) map[string]any {
	row := map[string]any{
		"metrics_schema": 4,
		"scope":          scope,
		"role":           role,
		"run_id":         strconv.FormatInt(runID, 10),
		"cluster_id":     fmt.Sprintf("spt-run-%d", runID),
		"step_id":        stepID,
		"op_type":        opType,
		"terminal":       true,
	}
	if stats != nil {
		row["driver_stats"] = stats
	}
	if scope == "fleet" {
		row["nodes_count"] = len(driverStatsFleetContributors)
		row["contributors_present"] = driverStatsFleetContributors
		row["partial"] = false
	}
	return row
}

func fleetDriverStats(reporting int) map[string]any {
	return map[string]any{
		"driver":    "s3-rdma",
		"counters":  map[string]int64{"rdma_transferred": 120, "http_fallback": 0},
		"gauges":    map[string]int64{"pool_reserved_bytes": 8192, "transport_available": 2},
		"reporting": reporting,
		"partial":   false,
	}
}

func metricsRows(rows ...any) http.HandlerFunc {
	if rows == nil {
		rows = []any{}
	}
	return func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rows)
	}
}

func endpointNotFound(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotFound)
}

func driverStatsServer(fleet, node http.HandlerFunc) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics/fleet/json", fleet)
	mux.HandleFunc("/metrics/json", node)
	return httptest.NewServer(mux)
}

func asJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode %T: %v", value, err)
	}
	return string(encoded)
}
