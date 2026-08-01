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
	"github.com/dell/storage-performance-tool/cli/internal/results"
)

// RunTracker polls the Spt API to detect run and step completion.
type RunTracker struct {
	Client               *SptAPIClient
	HTTPClient           *http.Client
	PollInterval         time.Duration
	IdleGrace            time.Duration
	StartupTimeout       time.Duration
	UnavailableTimeout   time.Duration
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
		PollInterval:        constants.AutoResultsTrackerPollInterval,
		IdleGrace:           constants.AutoResultsTrackerIdleGrace,
		StartupTimeout:      constants.AutoResultsStartupTimeout,
		UnavailableTimeout:  constants.AutoResultsUnavailableTimeout,
		StableConfirmations: constants.AutoResultsTrackerStableConfirmations,
		Clock:               api.Clock,
	}
}

// StepLifecycle remains an alias for compatibility with existing tracker consumers.
type StepLifecycle = results.StepLifecycle

// StepLifecyclePlanned and the constants in this block define stable per-step lifecycle states.
const (
	StepLifecyclePlanned    = results.StepLifecyclePlanned
	StepLifecycleStarted    = results.StepLifecycleStarted
	StepLifecycleCompleted  = results.StepLifecycleCompleted
	StepLifecycleFailed     = results.StepLifecycleFailed
	StepLifecycleNotStarted = results.StepLifecycleNotStarted
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
	unavailableSince := time.Time{}
	startupSince := t.Clock.Now()
	t.seenActive = false
	t.lastJSONTimestamp = 0
	final := &RunResult{Steps: make(map[string]StepCompletion, len(stepIDs))}

	for {
		select {
		case <-ctx.Done():
			return populateStepLifecycles(final, stepState, stepIDs), ctx.Err()
		case <-t.Clock.After(t.PollInterval):
		}

		// 1) Check run state
		status, terminal, trackerAvailable := t.checkRunState(ctx)
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
			done, size, mod, probeAvailable, err := t.probeStepFile(ctx, id)
			if probeAvailable {
				trackerAvailable = true
			}
			if err != nil {
				logging.LogDebug("tracker", "probe error", "stepId", id, "error", err.Error())
			}
			if done && size > 0 {
				t.seenActive = true
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
		usedIdle, isIdle, idleAvailable := t.checkIdle(ctx)
		if idleAvailable {
			trackerAvailable = true
		}
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

		// A healthy but slow run may remain in one state indefinitely. Bound only
		// continuous loss of every API signal, not the valid run duration.
		if trackerAvailable {
			unavailableSince = time.Time{}
		} else if unavailableSince.IsZero() {
			unavailableSince = t.Clock.Now()
		} else {
			unavailableTimeout := t.UnavailableTimeout
			if unavailableTimeout <= 0 {
				unavailableTimeout = constants.AutoResultsUnavailableTimeout
			}
			if t.Clock.Now().Sub(unavailableSince) >= unavailableTimeout {
				return populateStepLifecycles(final, stepState, stepIDs), fmt.Errorf(
					"completion tracker unavailable for %s", unavailableTimeout)
			}
		}

		// Before the engine has exposed real activity, healthy but empty APIs do
		// not prove that a run ever started. Bound that distinct startup state
		// without imposing any duration cap after the run is active.
		if !t.seenActive && !terminal {
			startupTimeout := t.StartupTimeout
			if startupTimeout <= 0 {
				startupTimeout = constants.AutoResultsStartupTimeout
			}
			if t.Clock.Now().Sub(startupSince) >= startupTimeout {
				return populateStepLifecycles(final, stepState, stepIDs), fmt.Errorf(
					"completion tracker observed no run activity for %s", startupTimeout)
			}
		}

		// Exit conditions
		if terminal || (len(stepState) > 0 && allDone && (!t.RequireTerminalState ||
			(final.FinalState != constants.StateRunning && final.FinalState != constants.StateStarting &&
				final.FinalState != constants.StateInitializing))) {
			break
		}
	}

	return populateStepLifecycles(final, stepState, stepIDs), nil
}

func populateStepLifecycles(final *RunResult, stepState map[string]*stepProbe, stepIDs []string) *RunResult {
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
	return final
}

func (t *RunTracker) checkRunState(ctx context.Context) (status terminalStatus, terminal, available bool) {
	// New contract: rely on /status only and retain its structured terminal cause.
	if status, err := t.Client.getStatusDetail(ctx); err == nil {
		switch status.State {
		case constants.StateRunning:
			t.seenActive = true
			return status, false, true
		case constants.StateStarting, constants.StateInitializing:
			return status, false, true
		case constants.StateIdle:
			return status, t.seenActive, true
		case constants.StateCompleted, constants.StateFailed, constants.StateStopped:
			return status, true, true
		default:
			return status, false, false
		}
	}
	return terminalStatus{}, false, false
}

// checkIdle tries to infer idle state via /metrics when /run is absent or unhelpful.
// Returns (usedIdleProbe, currentlyIdle, APIAvailable).
func (t *RunTracker) checkIdle(ctx context.Context) (bool, bool, bool) {
	steps, err := t.Client.getMetricsJSON(ctx)
	if err != nil || len(steps) == 0 {
		// If metrics endpoint responds but returns no steps, consider this an idle signal
		// in single-host runs after we've observed activity.
		if err == nil && len(steps) == 0 {
			return true, t.seenActive, true
		}
		return false, false, false
	}
	t.seenActive = true
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
		return true, true, true
	}
	return true, false, true
}

// probeStepFile checks if metrics.total.csv exists for a step using HEAD and returns its size and last-modified when available.
func (t *RunTracker) probeStepFile(ctx context.Context, stepID string) (bool, int64, string, bool, error) {
	u, err := url.Parse(t.Client.BaseURL)
	if err != nil {
		return false, 0, "", false, err
	}
	u.Path = path.Join(u.Path, "/logs/", stepID, "metrics.FileTotal")

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, u.String(), nil)
	if err != nil {
		return false, 0, "", false, fmt.Errorf("create request: %w", err)
	}
	resp, err := t.HTTPClient.Do(req)
	if err != nil {
		return false, 0, "", false, fmt.Errorf("http do: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return false, 0, "", false, nil
	}
	// Parse Content-Length and Last-Modified headers
	var size int64
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		if n, perr := strconv.ParseInt(strings.TrimSpace(cl), 10, 64); perr == nil {
			size = n
		}
	}
	lm := resp.Header.Get("Last-Modified")
	return true, size, lm, true, nil
}
