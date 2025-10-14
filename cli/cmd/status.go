/*
Copyright © 2025 Dell Technologies
*/

package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/config"
	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/spf13/cobra"
)

const (
	statusCommandTimeout    = 5 * time.Second
	statusHTTPClientTimeout = 3 * time.Second
)

func newStatusCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show live readiness and run state for Spt nodes",
		Long: `Polls the configured Spt API port on each host and prints a short
summary of readiness, run state, and recent metrics. This is intended for
operators to spot stalled or unhealthy nodes while a workload is running.`,
		SilenceUsage: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			_ = config.LoadDotEnv()
			if err := initializeLogger(); err != nil {
				return err
			}
			return applyEnvDefaultsToVerifyFlags(cmd)
		},
		RunE: runStatus,
	}

	cmd.Flags().String("test-hosts", "", "Comma-separated list of hosts to inspect (defaults to localhost)")
	cmd.Flags().String("api-port", constants.SptAPIPort, "Spt API port to query on each host")
	return cmd
}

var statusCmd = newStatusCommand()

func init() {
	rootCmd.AddCommand(statusCmd)
}

type nodeSummary struct {
	HostLabel        string
	RoleLabel        string
	APIURL           string
	Ready            bool
	ReadyStatus      string
	ReadyHTTPStatus  int
	NodeID           string
	Scope            string
	HealthStatus     string
	HealthRole       string
	HealthScope      string
	HealthHTTPStatus int
	RunID            string
	RunState         string
	RunMessage       string
	RunProgress      float64
	MetricsState     string
	OpsPerSec        int64
	MBPerSec         float64
	CompletionPct    *int
	OverallPct       *int
	SampleTimestamp  time.Time
	SampleAge        time.Duration
	Unbounded        bool
	Warnings         []string
	FatalErr         error
}

type readyPayload struct {
	Ready  bool   `json:"ready"`
	Status string `json:"status"`
	Scope  string `json:"scope"`
	Role   string `json:"role"`
	NodeID string `json:"node_id"`
}

type healthPayload struct {
	Status    string `json:"status"`
	Scope     string `json:"scope"`
	Role      string `json:"role"`
	NodeID    string `json:"node_id"`
	ClusterID string `json:"cluster_id"`
}

type runStatusPayload struct {
	RunID    string
	State    string
	Message  string
	Progress float64
}

type metricsStep struct {
	CompletionPercent int    `json:"completion_percent"`
	OverallCompletion int    `json:"overall_completion_percent"`
	Unbounded         bool   `json:"unbounded"`
	TestState         int    `json:"test_state"`
	SampleTimestamp   string `json:"sample_ts"`
	Operations        struct {
		SuccessRateLast float64 `json:"success_rate_last"`
	} `json:"operations"`
	Bandwidth struct {
		BytesRateLast float64 `json:"bytes_rate_last"`
	} `json:"bandwidth"`
}

type metricsSnapshot struct {
	CompletionPercent int
	OverallCompletion int
	Unbounded         bool
	TestState         int
	OpsPerSec         int64
	MBPerSec          float64
	SampleTimestamp   time.Time
}

func runStatus(cmd *cobra.Command, _ []string) error {
	isTesting = true
	apiPort := getAPIPort(cmd)
	hostString, _ := cmd.Flags().GetString("test-hosts")
	hostInfos, err := hostparse.ParseTestHosts(hostString)
	if err != nil {
		return fmt.Errorf("invalid test hosts: %w", err)
	}

	logging.LogInfo("status", "collecting node status",
		"hosts", len(hostInfos),
		"api_port", apiPort)

	ctx, cancel := context.WithTimeout(context.Background(), statusCommandTimeout)
	defer cancel()

	httpClient := &http.Client{Timeout: statusHTTPClientTimeout}
	summaries := make([]nodeSummary, 0, len(hostInfos))

	for idx, info := range hostInfos {
		roleLabel := "worker"
		if idx == 0 {
			roleLabel = "entry"
		}
		summary := inspectNode(ctx, httpClient, info, roleLabel, apiPort)
		summaries = append(summaries, summary)
	}

	printStatusSummaries(cmd, apiPort, summaries)

	var failed []string
	for _, s := range summaries {
		if s.FatalErr != nil {
			failed = append(failed, fmt.Sprintf("%s: %v", s.HostLabel, s.FatalErr))
		}
	}

	if len(failed) > 0 {
		return fmt.Errorf("status failed for %d host(s): %s", len(failed), strings.Join(failed, "; "))
	}

	return nil
}

func inspectNode(ctx context.Context, httpClient *http.Client, host *hostparse.HostInfo, roleLabel, apiPort string) nodeSummary {
	baseURL := strings.TrimRight(host.GetAPIURL(apiPort), "/")
	summary := nodeSummary{
		HostLabel: host.Original,
		RoleLabel: roleLabel,
		APIURL:    baseURL,
	}

	readyCtx, readyCancel := context.WithTimeout(ctx, statusHTTPClientTimeout)
	ready, statusCode, err := fetchReady(readyCtx, httpClient, baseURL)
	readyCancel()
	if err != nil {
		summary.FatalErr = fmt.Errorf("ready probe failed: %w", err)
		return summary
	}

	summary.Ready = ready.Ready
	summary.ReadyStatus = deriveReadyStatus(ready.Ready, ready.Status)
	summary.ReadyHTTPStatus = statusCode
	summary.NodeID = ready.NodeID
	summary.Scope = ready.Scope
	if ready.Role != "" {
		summary.RoleLabel = ready.Role
	}

	healthCtx, healthCancel := context.WithTimeout(ctx, statusHTTPClientTimeout)
	health, healthStatus, healthErr := fetchHealth(healthCtx, httpClient, baseURL)
	healthCancel()
	if healthErr != nil {
		summary.Warnings = append(summary.Warnings, fmt.Sprintf("health probe failed: %v", healthErr))
	} else {
		summary.HealthStatus = health.Status
		summary.HealthRole = health.Role
		summary.HealthScope = health.Scope
		summary.HealthHTTPStatus = healthStatus
	}

	statusCtx, statusCancel := context.WithTimeout(ctx, statusHTTPClientTimeout)
	runStatus, statusErr := fetchRunStatus(statusCtx, httpClient, baseURL)
	statusCancel()
	if statusErr != nil {
		summary.Warnings = append(summary.Warnings, fmt.Sprintf("status probe failed: %v", statusErr))
	} else {
		summary.RunID = runStatus.RunID
		summary.RunState = runStatus.State
		summary.RunMessage = runStatus.Message
		summary.RunProgress = runStatus.Progress
	}

	metricsCtx, metricsCancel := context.WithTimeout(ctx, statusHTTPClientTimeout)
	metrics, metricsErr := fetchMetrics(metricsCtx, httpClient, baseURL)
	metricsCancel()
	if metricsErr != nil {
		summary.Warnings = append(summary.Warnings, fmt.Sprintf("metrics probe failed: %v", metricsErr))
	} else if metrics != nil {
		summary.MetricsState = describeTestState(metrics.TestState)
		summary.OpsPerSec = metrics.OpsPerSec
		summary.MBPerSec = metrics.MBPerSec
		summary.SampleTimestamp = metrics.SampleTimestamp
		if !metrics.SampleTimestamp.IsZero() {
			summary.SampleAge = time.Since(metrics.SampleTimestamp)
		}
		summary.Unbounded = metrics.Unbounded
		summary.CompletionPct = ptrInt(metrics.CompletionPercent)
		summary.OverallPct = ptrInt(metrics.OverallCompletion)
	}

	return summary
}

func fetchReady(ctx context.Context, client *http.Client, baseURL string) (readyPayload, int, error) {
	var payload readyPayload
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, joinURL(baseURL, "/ready"), nil)
	if err != nil {
		return payload, 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return payload, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	statusCode := resp.StatusCode
	if statusCode != http.StatusOK && statusCode != http.StatusServiceUnavailable {
		bodyPreview := readBodyPreview(resp.Body)
		return payload, statusCode, fmt.Errorf("unexpected status %d from /ready: %s", statusCode, bodyPreview)
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil && !errors.Is(err, io.EOF) {
		return payload, statusCode, fmt.Errorf("decode /ready response: %w", err)
	}
	if statusCode == http.StatusOK && !payload.Ready {
		payload.Ready = true
	}
	if statusCode == http.StatusServiceUnavailable && payload.Status == "" {
		payload.Status = "starting"
	}
	return payload, statusCode, nil
}

func fetchHealth(ctx context.Context, client *http.Client, baseURL string) (healthPayload, int, error) {
	var payload healthPayload
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, joinURL(baseURL, "/health"), nil)
	if err != nil {
		return payload, 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return payload, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return payload, resp.StatusCode, fmt.Errorf("unexpected status %d from /health", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil && !errors.Is(err, io.EOF) {
		return payload, resp.StatusCode, fmt.Errorf("decode /health response: %w", err)
	}
	return payload, resp.StatusCode, nil
}

func fetchRunStatus(ctx context.Context, client *http.Client, baseURL string) (runStatusPayload, error) {
	var payload runStatusPayload
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, joinURL(baseURL, "/status"), nil)
	if err != nil {
		return payload, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return payload, err
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK:
		var body map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return payload, fmt.Errorf("decode /status response: %w", err)
		}
		if v, ok := body["runId"].(string); ok {
			payload.RunID = v
		}
		if v, ok := body["state"].(string); ok {
			payload.State = strings.ToUpper(strings.TrimSpace(v))
		}
		if v, ok := body["message"].(string); ok {
			payload.Message = v
		}
		if v, ok := body["progress"].(float64); ok {
			payload.Progress = v
		}
	case http.StatusNotFound:
		payload.State = constants.StateIdle
		payload.Message = "No test running"
	default:
		preview := readBodyPreview(resp.Body)
		return payload, fmt.Errorf("unexpected status %d from /status: %s", resp.StatusCode, preview)
	}
	return payload, nil
}

func fetchMetrics(ctx context.Context, client *http.Client, baseURL string) (*metricsSnapshot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, joinURL(baseURL, "/metrics/json"), nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics/json status %d", resp.StatusCode)
	}
	var steps []metricsStep
	if err := json.NewDecoder(resp.Body).Decode(&steps); err != nil {
		return nil, fmt.Errorf("decode metrics/json: %w", err)
	}
	if len(steps) == 0 {
		return nil, fmt.Errorf("metrics/json response empty")
	}
	step := steps[0]
	snapshot := &metricsSnapshot{
		CompletionPercent: step.CompletionPercent,
		OverallCompletion: step.OverallCompletion,
		Unbounded:         step.Unbounded,
		TestState:         step.TestState,
		OpsPerSec:         int64(math.Round(step.Operations.SuccessRateLast)),
		MBPerSec:          step.Bandwidth.BytesRateLast / 1_000_000,
	}
	if ts, err := parseMetricTimestamp(step.SampleTimestamp); err == nil {
		snapshot.SampleTimestamp = ts
	}
	return snapshot, nil
}

func printStatusSummaries(cmd *cobra.Command, apiPort string, summaries []nodeSummary) {
	out := cmd.OutOrStdout()
	_, _ = fmt.Fprintf(out, "Node status (port %s)\n", apiPort)

	for _, summary := range summaries {
		icon := "❌"
		if summary.FatalErr == nil && summary.Ready {
			icon = "✅"
		}

		role := summary.RoleLabel
		readyLabel := "NOT READY"
		if summary.Ready {
			readyLabel = "READY"
		}
		_, _ = fmt.Fprintf(out, "%s [%s] %s: %s (http %d, status=%s",
			icon, role, summary.HostLabel, readyLabel, summary.ReadyHTTPStatus, fallback(summary.ReadyStatus, "unknown"))
		if summary.NodeID != "" {
			_, _ = fmt.Fprintf(out, ", node=%s", summary.NodeID)
		}
		_, _ = fmt.Fprintln(out, ")")

		if summary.RunState != "" || summary.RunID != "" || summary.RunMessage != "" {
			parts := []string{}
			if summary.RunState != "" {
				parts = append(parts, fmt.Sprintf("state=%s", summary.RunState))
			}
			if summary.RunID != "" {
				parts = append(parts, fmt.Sprintf("run=%s", summary.RunID))
			}
			if summary.RunProgress > 0 {
				parts = append(parts, fmt.Sprintf("progress=%.1f%%", summary.RunProgress))
			}
			if summary.RunMessage != "" {
				parts = append(parts, fmt.Sprintf("message=%q", summary.RunMessage))
			}
			if len(parts) > 0 {
				_, _ = fmt.Fprintf(out, "  run: %s\n", strings.Join(parts, ", "))
			}
		}

		metricParts := buildMetricSummary(summary)
		if len(metricParts) > 0 {
			_, _ = fmt.Fprintf(out, "  metrics: %s\n", strings.Join(metricParts, ", "))
		}

		var warningCopy []string
		warningCopy = append(warningCopy, summary.Warnings...)
		if summary.FatalErr != nil {
			warningCopy = append(warningCopy, summary.FatalErr.Error())
		}
		if len(warningCopy) > 0 {
			sort.Strings(warningCopy)
			_, _ = fmt.Fprintf(out, "  warn: %s\n", strings.Join(warningCopy, "; "))
		}
	}
}

func buildMetricSummary(summary nodeSummary) []string {
	parts := make([]string, 0, 4)
	if summary.MetricsState != "" {
		parts = append(parts, fmt.Sprintf("state=%s", summary.MetricsState))
	}
	if summary.CompletionPct != nil {
		parts = append(parts, fmt.Sprintf("completion=%d%%", *summary.CompletionPct))
	}
	if summary.OverallPct != nil && *summary.OverallPct != *summary.CompletionPct {
		parts = append(parts, fmt.Sprintf("overall=%d%%", *summary.OverallPct))
	}
	if summary.OpsPerSec > 0 {
		parts = append(parts, fmt.Sprintf("ops=%d/s", summary.OpsPerSec))
	}
	if summary.MBPerSec > 0 {
		parts = append(parts, fmt.Sprintf("throughput=%.1fMB/s", summary.MBPerSec))
	}
	if !summary.SampleTimestamp.IsZero() {
		age := summary.SampleAge
		if age < 0 {
			age = 0
		}
		parts = append(parts, fmt.Sprintf("sample=%s (%s ago)", summary.SampleTimestamp.UTC().Format(time.RFC3339), formatDuration(age)))
	}
	if summary.Unbounded {
		parts = append(parts, "unbounded")
	}
	return parts
}

func describeTestState(state int) string {
	switch state {
	case constants.TestStateIdle:
		return "IDLE"
	case constants.TestStateRunning:
		return "RUNNING"
	case constants.TestStateCompleted:
		return "COMPLETED"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", state)
	}
}

func deriveReadyStatus(ready bool, status string) string {
	if status != "" {
		return status
	}
	if ready {
		return "ready"
	}
	return "starting"
}

func parseMetricTimestamp(raw string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, fmt.Errorf("empty timestamp")
	}
	if ts, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return ts, nil
	}
	return time.Parse(time.RFC3339, raw)
}

func joinURL(base, path string) string {
	return strings.TrimRight(base, "/") + path
}

func readBodyPreview(r io.Reader) string {
	data, _ := io.ReadAll(io.LimitReader(r, 160))
	text := strings.TrimSpace(string(data))
	if text == "" {
		return ""
	}
	return text
}

func ptrInt(v int) *int {
	if v == 0 {
		return nil
	}
	value := v
	return &value
}

func fallback(value, def string) string {
	if strings.TrimSpace(value) == "" {
		return def
	}
	return value
}

func formatDuration(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	if d < time.Second {
		return d.String()
	}
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(math.Mod(d.Seconds(), 60)))
	}
	hours := int(d.Hours())
	minutes := int(math.Mod(d.Minutes(), 60))
	seconds := int(math.Mod(d.Seconds(), 60))
	return fmt.Sprintf("%dh%dm%ds", hours, minutes, seconds)
}
