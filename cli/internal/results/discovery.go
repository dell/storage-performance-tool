package results

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

// DiscoverStepIDs fetches /metrics/json and returns unique step IDs found.
func DiscoverStepIDs(baseURL string) ([]string, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(strings.TrimSuffix(baseURL, "/") + "/metrics/json")
	if err != nil {
		return nil, fmt.Errorf("fetch metrics/json: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("metrics/json status: %d", resp.StatusCode)
	}
	var steps []struct {
		StepID string `json:"step_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&steps); err != nil {
		return nil, fmt.Errorf("decode metrics/json: %w", err)
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(steps))
	for _, s := range steps {
		if s.StepID == "" || seen[s.StepID] {
			continue
		}
		seen[s.StepID] = true
		out = append(out, s.StepID)
	}
	sort.Strings(out)
	return out, nil
}
