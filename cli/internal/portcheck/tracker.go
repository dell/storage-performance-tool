package portcheck

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
)

// RunTracker polls the Spt API to detect run and step completion.
type RunTracker struct {
	Client               *SptAPIClient
	HTTPClient           *http.Client
	PollInterval         time.Duration
	IdleGrace            time.Duration
	StableConfirmations  int // consecutive confirmations for a step file to be considered stable
	Clock                Clock
	lastJSONTimestamp    int64
	seenActive           bool
	Debug                bool
	RequireTerminalState bool
}

// NewRunTracker constructs a tracker with sensible defaults.
func NewRunTracker(baseURL string) *RunTracker {
	api := NewSptAPIClient(baseURL, DefaultHTTPTimeout)
	return &RunTracker{
		Client:              api,
		HTTPClient:          &http.Client{Timeout: DefaultHTTPTimeout},
		PollInterval:        500 * time.Millisecond,
		IdleGrace:           20 * time.Second,
		StableConfirmations: 2,
		Clock:               api.Clock,
	}
}

// StepLifecycle is the stable artifact-requiredness state for one planned step.
type StepLifecycle string

// StepLifecyclePlanned and the constants in this block define stable per-step lifecycle states.
const (
	StepLifecyclePlanned    StepLifecycle = "planned"
	StepLifecycleStarted    StepLifecycle = "started"
	StepLifecycleCompleted  StepLifecycle = "completed"
	StepLifecycleFailed     StepLifecycle = "failed"
	StepLifecycleNotStarted StepLifecycle = "not_started"
)

// StepCompletion represents lifecycle and completion info for one step.
type StepCompletion struct {
	StepID      string
	Lifecycle   StepLifecycle
	Planned     bool
	Started     bool
	Completed   bool
	Failed      bool
	CompletedAt time.Time
}

// RunResult summarizes the outcome of WaitForCompletion.
type RunResult struct {
	FinalState      string
	RunID           int64
	FailureStepID   string
	FailureCategory string
	FailureMessage  string
	Steps           map[string]StepCompletion
	UsedIdle        bool // true if idle fallback was used
}

type stepProbe struct {
	seenSize    int64
	seenMod     string
	okCount     int
	started     bool
	failed      bool
	completed   bool
	completedAt time.Time
}

// WaitForCompletion polls until the run is terminal or all steps are confirmed complete.
func (t *RunTracker) WaitForCompletion(ctx context.Context, stepIDs []string) (*RunResult, error) {
	if t.Client == nil {
		return nil, fmt.Errorf("nil client")
	}

	stepState := make(map[string]*stepProbe, len(stepIDs))
	for _, id := range stepIDs {
		stepState[id] = &stepProbe{}
	}

	idleSince := time.Time{}
	final := &RunResult{Steps: make(map[string]StepCompletion, len(stepIDs))}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-t.Clock.After(t.PollInterval):
		}

		// 1) Check run state
		status, terminal := t.checkRunState(ctx)
		state := status.State
		if t.Debug {
			logging.LogDebug("auto-results", "poll",
				"state", state,
				"seen_active", t.seenActive,
				"terminal", terminal)
		}
		if state != "" {
			final.FinalState = state
			final.RunID = status.RunID
			final.FailureStepID = status.StepID
			final.FailureCategory = status.Category
			final.FailureMessage = status.Message
		}
		if current, ok := stepState[status.StepID]; ok {
			current.started = true
			if state == constants.StateFailed {
				current.failed = true
			}
		}

		// 2) Probe steps
		allDone := true
		for id, st := range stepState {
			if st.completed {
				continue
			}
			done, size, mod, err := t.probeStepFile(ctx, id)
			if err != nil {
				logging.LogDebug("tracker", "probe error", "stepId", id, "error", err.Error())
			}
			if done && size > 0 {
				st.started = true
				if st.okCount == 0 {
					st.seenSize = size
					st.seenMod = mod
					st.okCount = 1
				} else if size == st.seenSize && mod == st.seenMod {
					st.okCount++
				} else {
					// size changed; reset confirmation counter
					st.seenSize = size
					st.seenMod = mod
					st.okCount = 1
				}
				if st.okCount >= t.StableConfirmations {
					st.completed = true
					st.completedAt = t.Clock.Now()
				}
			} else {
				st.okCount = 0
			}

			if !st.completed {
				allDone = false
			}
			if t.Debug {
				logging.LogDebug("auto-results", "step",
					"id", id, "done", st.completed, "ok_count", st.okCount, "size", st.seenSize)
			}
		}

		// 3) Idle fallback when /run is not available
		usedIdle, isIdle := t.checkIdle(ctx)
		if t.Debug {
			logging.LogDebug("auto-results", "idle_check", "used", usedIdle, "is_idle", isIdle)
		}
		if usedIdle && isIdle {
			if idleSince.IsZero() {
				idleSince = t.Clock.Now()
			}
			if t.Clock.Now().Sub(idleSince) >= t.IdleGrace {
				final.FinalState = constants.StateIdle
				final.UsedIdle = true
				terminal = true
			}
		} else {
			idleSince = time.Time{}
		}
		if t.Debug {
			logging.LogDebug("auto-results", "idle_probe",
				"used", usedIdle,
				"since", idleSince,
				"grace", t.IdleGrace,
				"terminal", terminal,
				"all_done", allDone)
		}

		// Exit conditions
		if terminal || (len(stepState) > 0 && allDone && (!t.RequireTerminalState ||
			(final.FinalState != constants.StateRunning && final.FinalState != constants.StateStarting &&
				final.FinalState != constants.StateInitializing))) {
			break
		}
	}

	terminalStepIndex := -1
	for index, id := range stepIDs {
		if id == final.FailureStepID {
			terminalStepIndex = index
			break
		}
	}
	for index, id := range stepIDs {
		st := stepState[id]
		lifecycle := StepLifecyclePlanned
		switch {
		case st.completed:
			lifecycle = StepLifecycleCompleted
		case st.failed || (final.FinalState == constants.StateFailed && id == final.FailureStepID):
			lifecycle = StepLifecycleFailed
		case st.started || (terminalStepIndex >= 0 && index <= terminalStepIndex):
			lifecycle = StepLifecycleStarted
		case terminalStepIndex >= 0 && index > terminalStepIndex &&
			(final.FinalState == constants.StateFailed || final.FinalState == constants.StateStopped):
			lifecycle = StepLifecycleNotStarted
		}
		final.Steps[id] = StepCompletion{
			StepID: id, Lifecycle: lifecycle, Planned: true, Started: st.started || lifecycle == StepLifecycleStarted ||
				lifecycle == StepLifecycleCompleted || lifecycle == StepLifecycleFailed,
			Completed: st.completed, Failed: lifecycle == StepLifecycleFailed, CompletedAt: st.completedAt,
		}
	}
	return final, nil
}

func (t *RunTracker) checkRunState(ctx context.Context) (status terminalStatus, terminal bool) {
	// New contract: rely on /status only and retain its structured terminal cause.
	if status, err := t.Client.getStatusDetail(ctx); err == nil {
		switch status.State {
		case constants.StateRunning, constants.StateStarting, constants.StateInitializing:
			t.seenActive = true
			return status, false
		case constants.StateIdle:
			return status, t.seenActive
		case constants.StateCompleted, constants.StateFailed, constants.StateStopped:
			return status, true
		default:
			return status, false
		}
	}
	return terminalStatus{}, false
}

// checkIdle tries to infer idle state via /metrics when /run is absent or unhelpful.
// Returns (usedIdleProbe, currentlyIdle).
func (t *RunTracker) checkIdle(ctx context.Context) (bool, bool) {
	steps, err := t.Client.getMetricsJSON(ctx)
	if err != nil || len(steps) == 0 {
		// If metrics endpoint responds but returns no steps, consider this an idle signal
		// in single-host runs after we've observed activity.
		if err == nil && len(steps) == 0 {
			return true, t.seenActive
		}
		return false, false
	}
	// Aggregate across steps
	var maxTS int64
	var sumRates float64
	hasTerminal := false
	for _, s := range steps {
		if s.Timestamp > maxTS {
			maxTS = s.Timestamp
		}
		sumRates += s.Operations.SuccessRateLast
		sumRates += s.Bandwidth.BytesRateLast
		if s.Terminal {
			hasTerminal = true
		}
	}
	// Idle if no rates and timestamp unchanged since last check
	unchanged := (maxTS == t.lastJSONTimestamp && t.lastJSONTimestamp != 0)
	t.lastJSONTimestamp = maxTS
	// Prefer explicit terminal flag when present, keep legacy heuristic as fallback
	if (hasTerminal && sumRates == 0 && unchanged) || (sumRates == 0 && unchanged) {
		return true, true
	}
	return true, false
}

// probeStepFile checks if metrics.total.csv exists for a step using HEAD and returns its size and last-modified when available.
func (t *RunTracker) probeStepFile(ctx context.Context, stepID string) (bool, int64, string, error) {
	u, err := url.Parse(t.Client.BaseURL)
	if err != nil {
		return false, 0, "", err
	}
	u.Path = path.Join(u.Path, "/logs/", stepID, "metrics.FileTotal")

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, u.String(), nil)
	if err != nil {
		return false, 0, "", fmt.Errorf("create request: %w", err)
	}
	resp, err := t.HTTPClient.Do(req)
	if err != nil {
		return false, 0, "", fmt.Errorf("http do: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return false, 0, "", nil
	}
	// Parse Content-Length and Last-Modified headers
	var size int64
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		if n, perr := strconv.ParseInt(strings.TrimSpace(cl), 10, 64); perr == nil {
			size = n
		}
	}
	lm := resp.Header.Get("Last-Modified")
	return true, size, lm, nil
}
