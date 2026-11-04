package server

import (
	"reflect"
	"testing"
)

func TestAttachedMetricsGapPreservesSamplesAndRecovers(t *testing.T) {
	for _, name := range []string{"HTTP error", "malformed JSON", "empty samples"} {
		t.Run(name, func(t *testing.T) {
			a, api := newAttachedAPIFixture(t)
			first := attachedReply("run-a", "2026-10-30T12:00:01Z", 10)
			api.set(first)
			state := attachedRequest(t, a, "POST", "/api/attach", `{"host":"127.0.0.1"}`, 200)
			initial := state["metrics"].(map[string]any)["samples"]
			gap := first
			switch name {
			case "HTTP error":
				gap.metricsCode = 503
			case "malformed JSON":
				gap.metricsBody = "{"
			case "empty samples":
				gap.metricsBody = "[]"
			}
			api.set(gap)
			for i := 0; i < 2; i++ {
				state = attachedRequest(t, a, "GET", "/api/state", "", 200)
				assertAttachedCounts(t, state, []float64{10})
				if got := state["metrics"].(map[string]any)["samples"]; !reflect.DeepEqual(got, initial) {
					t.Fatalf("gap changed the last measurement: %#v; want %#v", got, initial)
				}
			}
			api.set(attachedReply("run-a", "2026-10-30T12:00:02Z", 20))
			// A new sample is appended exactly once; repeated polling of the
			// same timestamp must not invent additional chart points.
			for i := 0; i < 2; i++ {
				state = attachedRequest(t, a, "GET", "/api/state", "", 200)
				assertAttachedCounts(t, state, []float64{10, 20})
				latest := state["metrics"].(map[string]any)["samples"].([]any)
				if len(latest) != 1 || latest[0].(map[string]any)["cumulative_ops"] != float64(20) {
					t.Fatalf("wrong latest measurement after recovery: %#v", latest)
				}
			}
		})
	}
}

func TestAttachedMetricsInitiallyUnavailable(t *testing.T) {
	a, api := newAttachedAPIFixture(t)
	reply := attachedReply("run-a", "2026-10-30T12:00:01Z", 10)
	reply.metricsCode = 503
	api.set(reply)
	state := attachedRequest(t, a, "POST", "/api/attach", `{"host":"127.0.0.1"}`, 200)
	assertAttachedCounts(t, state, []float64{})
	if samples := state["metrics"].(map[string]any)["samples"].([]any); len(samples) != 0 {
		t.Fatalf("invented measurements before first sample: %#v", samples)
	}
	api.set(attachedReply("run-a", "2026-10-30T12:00:02Z", 20))
	state = attachedRequest(t, a, "GET", "/api/state", "", 200)
	assertAttachedCounts(t, state, []float64{20})
}
