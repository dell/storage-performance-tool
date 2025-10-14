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
	Client              *SptAPIClient
	HTTPClient          *http.Client
	PollInterval        time.Duration
	IdleGrace           time.Duration
	StableConfirmations int // consecutive confirmations for a step file to be considered stable
	Clock               Clock
	lastJSONTimestamp   int64
	seenActive          bool
	Debug               bool
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

// StepCompletion represents completion info for one step.
type StepCompletion struct {
	StepID      string
	Completed   bool
	CompletedAt time.Time
}

// RunResult summarizes the outcome of WaitForCompletion.
type RunResult struct {
	FinalState string
	Steps      map[string]StepCompletion
	UsedIdle   bool // true if idle fallback was used
}

type stepProbe struct {
	seenSize    int64
	seenMod     string
	okCount     int
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
		state, terminal := t.checkRunState(ctx)
		if t.Debug {
			logging.LogDebug("auto-results", "poll",
				"state", state,
				"seen_active", t.seenActive,
				"terminal", terminal)
		}
		if state != "" {
			final.FinalState = state
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
		if terminal || (len(stepState) > 0 && allDone) {
			break
		}
	}

	for id, st := range stepState {
		final.Steps[id] = StepCompletion{StepID: id, Completed: st.completed, CompletedAt: st.completedAt}
	}
	return final, nil
}

func (t *RunTracker) checkRunState(ctx context.Context) (state string, terminal bool) {
	// New contract: rely on /status only
	if st, err := t.Client.getStatusSimple(ctx); err == nil {
		switch st {
		case constants.StateRunning, constants.StateStarting, constants.StateInitializing:
			t.seenActive = true
			return st, false
		case constants.StateIdle:
			if t.seenActive {
				return st, true
			}
			return st, false
		case constants.StateCompleted, constants.StateFailed, constants.StateStopped:
			return st, true
		default:
			return st, false
		}
	}
	return "", false
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
