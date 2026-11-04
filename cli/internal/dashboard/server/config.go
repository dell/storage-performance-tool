package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

type dashboardSettings struct {
	Branding dashboardBranding `json:"branding"`
	Limits   workloadLimits    `json:"limits"`
	Compare  compareSettings   `json:"compare"`
}

// compareSettings are the defaults for the catalog's regression threshold check.
type compareSettings struct {
	ThresholdPct   float64 `json:"threshold_pct"`
	LatencyFloorUS float64 `json:"latency_floor_us"`
	MinOps         float64 `json:"min_ops"`
}

type dashboardBranding struct {
	ProductName      string `json:"product_name"`
	EnvironmentLabel string `json:"environment_label"`
}

type workloadLimits struct {
	Guided guidedLimits `json:"guided"`
	Expert expertLimits `json:"expert"`
}

type guidedLimits struct {
	ObjectCounts []int    `json:"object_counts"`
	ObjectSizes  []string `json:"object_sizes"`
	Threads      []int    `json:"threads"`
	Runtimes     []int    `json:"max_runtime_seconds"`
}

type expertLimits struct {
	ObjectCountMin int   `json:"object_count_min"`
	ObjectCountMax int   `json:"object_count_max"`
	ObjectSizeMin  int64 `json:"object_size_bytes_min"`
	ObjectSizeMax  int64 `json:"object_size_bytes_max"`
	ThreadsMin     int   `json:"threads_min"`
	ThreadsMax     int   `json:"threads_max"`
	RuntimeMin     int   `json:"max_runtime_seconds_min"`
	RuntimeMax     int   `json:"max_runtime_seconds_max"`
	MaxTotalBytes  int64 `json:"max_total_bytes"`
}

func defaultSettings() dashboardSettings {
	return dashboardSettings{
		Branding: dashboardBranding{ProductName: "Storage Performance Tool", EnvironmentLabel: "Configured environment"},
		Limits: workloadLimits{
			Guided: guidedLimits{ObjectCounts: []int{10, 25, 50, 100, 250, 320}, ObjectSizes: []string{"64KiB", "256KiB", "1MiB", "4MiB", "16MiB"}, Threads: []int{1, 2}, Runtimes: []int{60, 120, 300, 600}},
			Expert: expertLimits{ObjectCountMin: expertMinObjects, ObjectCountMax: expertMaxObjects, ObjectSizeMin: expertMinObjectBytes, ObjectSizeMax: expertMaxObjectBytes, ThreadsMin: expertMinThreads, ThreadsMax: expertMaxThreads, RuntimeMin: expertMinRuntime, RuntimeMax: expertMaxRuntime, MaxTotalBytes: expertMaxTotalBytes},
		},
		Compare: defaultCompareSettings(),
	}
}

func loadSettings(path string) (dashboardSettings, error) {
	settings := defaultSettings()
	if path == "" {
		path = os.Getenv("DASHBOARD_CONFIG")
	}
	if path != "" {
		f, err := os.Open(path) // #nosec G304 -- path comes from operator-provided --config or DASHBOARD_CONFIG, not a request
		if err != nil {
			return dashboardSettings{}, fmt.Errorf("open dashboard settings: %w", err)
		}
		defer func() { _ = f.Close() }()
		dec := json.NewDecoder(f)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&settings); err != nil {
			return dashboardSettings{}, fmt.Errorf("read dashboard settings: %w", err)
		}
		var extra any
		if err := dec.Decode(&extra); err != io.EOF {
			return dashboardSettings{}, errors.New("dashboard settings must contain one JSON object")
		}
	}
	if err := settings.validate(); err != nil {
		return dashboardSettings{}, err
	}
	return settings, nil
}

func (s dashboardSettings) validate() error {
	if strings.TrimSpace(s.Branding.ProductName) == "" || strings.TrimSpace(s.Branding.EnvironmentLabel) == "" {
		return errors.New("dashboard branding product_name and environment_label must be non-empty")
	}
	g := s.Limits.Guided
	if len(g.ObjectCounts) == 0 || len(g.ObjectSizes) == 0 || len(g.Threads) == 0 || len(g.Runtimes) == 0 {
		return errors.New("dashboard guided limits must each contain at least one choice")
	}
	for _, v := range g.ObjectCounts {
		if v < 1 || v > 1_000_000 {
			return errors.New("guided object counts must be between 1 and 1000000")
		}
	}
	for _, v := range g.Threads {
		if v < 1 || v > 4096 {
			return errors.New("guided thread counts must be between 1 and 4096")
		}
	}
	for _, v := range g.Runtimes {
		if v < 1 || v > 31*24*60*60 {
			return errors.New("guided runtimes must be between 1 second and 31 days")
		}
	}
	for _, v := range g.ObjectSizes {
		size, ok := parseSize(v)
		if !ok {
			return fmt.Errorf("invalid guided object size %q", v)
		}
		for _, count := range g.ObjectCounts {
			if size > s.Limits.Expert.MaxTotalBytes/int64(count) {
				return fmt.Errorf("guided object count %d and size %q exceed the configured maximum total data", count, v)
			}
		}
	}
	if hasDuplicates(g.ObjectCounts) || hasDuplicates(g.ObjectSizes) || hasDuplicates(g.Threads) || hasDuplicates(g.Runtimes) {
		return errors.New("dashboard guided limits must not contain duplicate choices")
	}
	if err := s.Compare.validate(); err != nil {
		return err
	}
	e := s.Limits.Expert
	if e.ObjectCountMin < 1 || e.ObjectCountMax < e.ObjectCountMin || e.ObjectCountMax > 1_000_000 || e.ObjectSizeMin < 1 || e.ObjectSizeMax < e.ObjectSizeMin || e.ObjectSizeMax > 9_999_999<<30 || e.ThreadsMin < 1 || e.ThreadsMax < e.ThreadsMin || e.ThreadsMax > 4096 || e.RuntimeMin < 1 || e.RuntimeMax < e.RuntimeMin || e.RuntimeMax > 31*24*60*60 || e.MaxTotalBytes < e.ObjectSizeMin {
		return errors.New("dashboard expert limits must be positive, ordered, and within supported bounds")
	}
	return nil
}

func hasDuplicates[T comparable](values []T) bool {
	seen := make(map[T]bool, len(values))
	for _, value := range values {
		if seen[value] {
			return true
		}
		seen[value] = true
	}
	return false
}

func (a *app) guidedLimitSets() (map[int]bool, map[string]bool, map[int]bool, map[int]bool) {
	g := a.effectiveSettings().Limits.Guided
	ints := func(values []int) map[int]bool {
		out := make(map[int]bool, len(values))
		for _, v := range values {
			out[v] = true
		}
		return out
	}
	sizes := make(map[string]bool, len(g.ObjectSizes))
	for _, v := range g.ObjectSizes {
		sizes[v] = true
	}
	return ints(g.ObjectCounts), sizes, ints(g.Threads), ints(g.Runtimes)
}

func (a *app) limitsPayload() map[string]any {
	g, e := a.effectiveSettings().Limits.Guided, a.effectiveSettings().Limits.Expert
	sizes := append([]string(nil), g.ObjectSizes...)
	sort.Slice(sizes, func(i, j int) bool { x, _ := parseSize(sizes[i]); y, _ := parseSize(sizes[j]); return x < y })
	return map[string]any{
		"guided": map[string]any{"object_counts": g.ObjectCounts, "object_sizes": sizes, keyThreads: g.Threads, keyMaxRuntimeSeconds: g.Runtimes},
		"expert": map[string]any{"object_count": map[string]int{keyMin: e.ObjectCountMin, keyMax: e.ObjectCountMax}, "object_size_bytes": map[string]int64{keyMin: e.ObjectSizeMin, keyMax: e.ObjectSizeMax}, keyThreads: map[string]int{keyMin: e.ThreadsMin, keyMax: e.ThreadsMax}, keyMaxRuntimeSeconds: map[string]int{keyMin: e.RuntimeMin, keyMax: e.RuntimeMax}, "max_total_bytes": e.MaxTotalBytes},
	}
}

func (a *app) effectiveSettings() dashboardSettings {
	if strings.TrimSpace(a.settings.Branding.ProductName) == "" {
		return defaultSettings()
	}
	return a.settings
}
