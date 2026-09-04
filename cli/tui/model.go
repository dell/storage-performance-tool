/*
Copyright © 2025 Dell Technologies
*/

// Package tui implements the terminal user interface for spt.
// It provides an interactive UI for monitoring benchmark progress
// and managing test execution using the Bubble Tea framework.
package tui

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/dell/storage-performance-tool/cli/internal/cmdline"
	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/dell/storage-performance-tool/cli/internal/mathutil"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/internal/textutil"
)

const (
	maxScrollback = 10000
)

// Compiled regex for matching Spt data lines
// Matches: [optional ANSI codes][digits].[digits]|[timestamp]|...
// sptDataLineRegex moved to test helpers (only used by test-only helpers)

// Compiled regex for stripping ANSI escape sequences
var ansiEscapeRegex = regexp.MustCompile(`\x1b\[[0-9;]*m`)
var traceEscapeRegex = regexp.MustCompile(`\x1b\[[0-9;?]*[ -/]*[@-~]`)

var (
	newTUIDockerManager             = func() (DockerInterface, error) { return NewDockerManager() }
	newTUIProgramWithTrace          = newProgramWithTrace
	newMultiHostTUITestOrchestrator = NewMultiHostTestOrchestrator
)

// Model represents the TUI state
type Model struct {
	width         int
	height        int
	processOutput []string // Scrollback buffer for all process output (stdout + stderr + spt messages)
	verbose       bool

	// Viewport state
	processScroll       int  // Current scroll position in process output
	processOutputHidden bool // Whether to hide the process output viewport
	chartsHidden        bool // Whether to hide the charts viewport

	// Container state
	containerID     string
	containerStatus string
	startTime       time.Time

	// Docker manager
	dockerManager DockerInterface

	// Test orchestrator for API-based control
	orchestrator TestOrchestratorInterface
	testStatus   string // Current test status from API

	// Scenario file management
	scenarioFile   string                   // Path to the scenario file for cleanup
	keepScenario   bool                     // Whether to keep the scenario file after exit
	scenarioParams *scenario.ScenarioParams // Test configuration parameters

	// Metrics collection
	metricsCollector     *MetricsCollector
	opsChartRenderer     *GenericChartRenderer
	latencyChartRenderer *GenericChartRenderer
	failureChartRenderer *GenericChartRenderer

	// Live view rendering
	liveViewRenderer *LiveViewRenderer

	// Historical navigation state
	// historicalIndex represents the offset backward from real-time:
	//   0 = live mode (viewing most recent sample)
	//  -1 = viewing 1 sample back in time
	//  -2 = viewing 2 samples back in time, etc.
	// When new samples arrive, this index is automatically decremented
	// to keep the user focused on the same data point.
	historicalIndex int

	// Styling
	statusStyle lipgloss.Style
	borderStyle lipgloss.Style

	// Baseline provider (optional): returns an initial MultiNodeMetricsUpdate
	baselineFn func() *MultiNodeMetricsUpdate

	// Frame dump (diagnostics)
	frameDumpPath        string
	frameDumpMax         int
	frameDumpStrip       bool
	frameDumpLiveOnly    bool
	frameDumpCount       *int
	frameDumpMinInterval time.Duration
	frameDumpLast        *time.Time
	traceSink            *tuiTraceSink
}

type tuiTraceSink struct {
	mu   sync.Mutex
	file *os.File
}

func newTUITraceSink(file *os.File) *tuiTraceSink {
	if file == nil {
		return nil
	}
	return &tuiTraceSink{file: file}
}

func sanitizeTUITraceLine(line string) string {
	if line == "" {
		return ""
	}
	line = traceEscapeRegex.ReplaceAllString(line, "")
	var b strings.Builder
	b.Grow(len(line))
	for _, r := range line {
		if r == '\t' || !unicode.IsControl(r) {
			b.WriteRune(r)
		}
	}
	return strings.TrimSpace(b.String())
}

func (s *tuiTraceSink) writeLine(line string) {
	if s == nil || s.file == nil {
		return
	}
	clean := sanitizeTUITraceLine(line)
	if clean == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, _ = fmt.Fprintln(s.file, clean)
}

// Messages
type containerOutputMsg string
type containerStderrMsg string // New message type for stderr
type sptMessageMsg string
type containerStatusMsg string

// autoTerminateMsg triggers a clean TUI exit when received
type autoTerminateMsg struct{}
type tickMsg time.Time
type containerStartedMsg struct {
	containerID string
}
type metricsUpdateMsg PerformanceMetric
type apiStatusMsg TestStatus
type apiMetricsMsg PerformanceMetric
type multiNodeMetricsMsg MultiNodeMetricsUpdate

// tuiLaunchResult crosses the startup-goroutine boundary explicitly. A
// launcher error alone cannot identify whether POST /run was already accepted.
type tuiLaunchResult struct {
	Submission SubmissionState
	Err        error
}

var errTUIExitedBeforeSubmission = errors.New("TUI exited before the run was submitted")

func startTUILaunch(
	ctx context.Context,
	p *tea.Program,
	hooks LaunchHooks,
	start func(context.Context) error,
	reportFailure func(error),
) (<-chan tuiLaunchResult, context.CancelFunc) {
	launchCtx, cancel := context.WithCancel(normalizeContext(ctx))
	resultCh := make(chan tuiLaunchResult, 1)
	go func() {
		timer := time.NewTimer(constants.TUIStartupDelay)
		defer timer.Stop()
		select {
		case <-launchCtx.Done():
			resultCh <- tuiLaunchResult{Submission: hooks.SubmissionState(), Err: launchCtx.Err()}
			p.Quit()
			return
		case <-timer.C:
		}

		err := start(launchCtx)
		resultCh <- tuiLaunchResult{Submission: hooks.SubmissionState(), Err: err}
		if err != nil {
			if reportFailure != nil {
				reportFailure(err)
			}
			p.Quit()
		}
	}()
	go func() {
		<-launchCtx.Done()
		p.Quit()
	}()
	return resultCh, cancel
}

func finishTUILaunch(
	ctx context.Context, programErr error, resultCh <-chan tuiLaunchResult, hooks LaunchHooks,
) error {
	var launchErr error
	timer := time.NewTimer(constants.TUILaunchJoinTimeout)
	defer timer.Stop()
	select {
	case result := <-resultCh:
		launchErr = result.Err
		if result.Submission != SubmissionNotSubmitted &&
			hooks.SubmissionState() == SubmissionNotSubmitted {
			launchErr = errors.Join(launchErr, errors.New("TUI launch submission state was lost"))
		}
		if result.Submission == SubmissionNotSubmitted && launchErr == nil {
			launchErr = errTUIExitedBeforeSubmission
		}
	case <-timer.C:
		launchErr = errors.New("timed out waiting for TUI startup cancellation and rollback")
	}
	if ctx != nil {
		launchErr = errors.Join(launchErr, ctx.Err())
	}
	return errors.Join(launchErr, programErr)
}

// InitialModel creates a new model instance
func InitialModel() Model {
	m := Model{
		processOutput:        make([]string, 0, maxScrollback),
		processScroll:        0,
		processOutputHidden:  false, // Start with all panels visible
		chartsHidden:         false, // Start with all panels visible
		containerStatus:      "Not Started",
		startTime:            time.Now(),
		metricsCollector:     NewMetricsCollector(),
		opsChartRenderer:     NewGenericChartRenderer("Operations/sec", "ops/s", ExtractOpsPerSec, FormatOpsValue),
		latencyChartRenderer: NewGenericChartRenderer("Latency P50", "", ExtractDisplayLatency, FormatLatencyValue),
		failureChartRenderer: NewGenericChartRendererWithScale("Failure Rate", "failures", ExtractFailureIncrement, FormatFailureValue, PageScrollSize),
		historicalIndex:      0, // Start in live mode
		statusStyle:          lipgloss.NewStyle().Background(lipgloss.Color("238")).Foreground(lipgloss.Color("255")),
		borderStyle:          lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color("238")),
		baselineFn:           nil,
		frameDumpPath:        "",
		frameDumpMax:         0,
		frameDumpStrip:       false,
		frameDumpLiveOnly:    false,
		frameDumpCount:       nil,
		traceSink:            nil,
	}

	// Optional frame dump configuration via env vars
	if path := os.Getenv("SPT_LIVEVIEW_DUMP"); path != "" {
		m.frameDumpPath = path
		m.frameDumpMax = 5
		if v := os.Getenv("SPT_LIVEVIEW_FRAMES"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				m.frameDumpMax = n
			}
		}
		// Allow either specific or generic ANSI-off envs
		if os.Getenv("SPT_FRAME_DUMP_STRIP_ANSI") == "1" || os.Getenv("SPT_ANSI_OFF") == "1" {
			m.frameDumpStrip = true
		}
		if os.Getenv("SPT_FRAME_DUMP_LIVEONLY") == "1" || os.Getenv("SPT_LIVEVIEW_ONLY") == "1" {
			m.frameDumpLiveOnly = true
		}
		c := 0
		m.frameDumpCount = &c

		// Rate limiting: default 1 Hz
		m.frameDumpMinInterval = time.Second
		if hzStr := os.Getenv("SPT_FRAME_DUMP_HZ"); hzStr != "" {
			if hz, err := strconv.ParseFloat(hzStr, 64); err == nil && hz > 0 {
				// interval = 1/hz seconds
				m.frameDumpMinInterval = time.Duration(float64(time.Second) / hz)
				if m.frameDumpMinInterval < 0 {
					m.frameDumpMinInterval = time.Second
				}
			}
		}
		var t time.Time
		m.frameDumpLast = &t
	}

	return m
}

func (m *Model) traceContainerOutput(line string) {
	if m.traceSink != nil {
		m.traceSink.writeLine(line)
	}
}

func (m *Model) traceStderr(msg string) {
	if m.traceSink != nil {
		m.traceSink.writeLine("[stderr] " + msg)
	}
}

func (m *Model) traceSptMessage(msg string) {
	if m.traceSink != nil {
		m.traceSink.writeLine("[spt] " + msg)
	}
}

func (m *Model) traceStatus(status string) {
	if m.traceSink != nil {
		m.traceSink.writeLine("[status] " + status)
	}
}

// Init initializes the model
func (m Model) Init() tea.Cmd {
	return tea.Batch(tickCmd(), emitBaselineCmd(m.baselineFn))
}

// tickCmd returns a command that sends tick messages for status updates
func tickCmd() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

// emitBaselineCmd schedules an initial multi-node status update via the event loop.
func emitBaselineCmd(fn func() *MultiNodeMetricsUpdate) tea.Cmd {
	if fn == nil {
		return func() tea.Msg { return nil }
	}
	return func() tea.Msg {
		if upd := fn(); upd != nil {
			return multiNodeMetricsMsg(*upd)
		}
		return nil
	}
}

// Update handles messages
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch mt := msg.(type) {
	case tea.WindowSizeMsg:
		mm := m
		mm.handleWindowResize(mt)
		return mm, nil
	case tea.KeyMsg:
		if mt.String() == "q" || mt.String() == "ctrl+c" {
			return m, tea.Quit
		}
		mm := m
		mm.handleKey(mt)
		return mm, nil
	case containerOutputMsg:
		mm := m
		if !mm.verbose && isTextMetricsLine(string(mt)) {
			return mm, nil
		}
		mm.traceContainerOutput(string(mt))
		mm.handleContainerOutput(string(mt))
		return mm, nil
	case containerStderrMsg:
		mm := m
		mm.traceStderr(string(mt))
		mm.addStderrMessage(string(mt))
		return mm, nil
	case sptMessageMsg:
		mm := m
		if !mm.verbose && isTextMetricsLine(string(mt)) {
			return mm, nil
		}
		mm.traceSptMessage(string(mt))
		mm.addSptMessage(string(mt))
		return mm, nil
	case containerStatusMsg:
		mm := m
		mm.traceStatus(string(mt))
		mm.containerStatus = string(mt)
		return mm, nil
	case autoTerminateMsg:
		// Exit the TUI; outer callers handle container shutdown
		return m, tea.Quit
	case containerStartedMsg:
		mm := m
		mm.containerID = mt.containerID
		mm.containerStatus = "Running"
		return mm, nil
	case metricsUpdateMsg:
		mm := m
		mm.handleMetric(PerformanceMetric(mt))
		return mm, nil
	case apiStatusMsg:
		mm := m
		mm.handleAPIStatus(mt)
		return mm, nil
	case apiMetricsMsg:
		mm := m
		mm.handleAPIMetrics(PerformanceMetric(mt))
		return mm, nil
	case multiNodeMetricsMsg:
		mm := m
		mm.handleMultiNodeMetrics(MultiNodeMetricsUpdate(mt))
		return mm, nil
	case tickMsg:
		return m, tickCmd()
	}
	return m, nil
}

func isTextMetricsLine(line string) bool {
	return strings.Contains(stripANSIEscapeSequences(line), "[METRICS]")
}

func (m *Model) handleWindowResize(msg tea.WindowSizeMsg) {
	m.width = msg.Width
	m.height = msg.Height
}

func (m *Model) handleKey(msg tea.KeyMsg) {
	if m.handleKeyScroll(msg) {
		return
	}
	m.handleKeyMode(msg)
}

func (m *Model) handleKeyScroll(msg tea.KeyMsg) bool {
	switch msg.String() {
	case "up", "k":
		if !m.processOutputHidden && m.processScroll > 0 {
			m.processScroll--
		}
		return true
	case "down", "j":
		if !m.processOutputHidden {
			maxScroll := len(m.processOutput) - m.getProcessViewportHeight()
			if maxScroll > 0 && m.processScroll < maxScroll {
				m.processScroll++
			}
		}
		return true
	case "pgup":
		if !m.processOutputHidden {
			m.processScroll = maxInt(0, m.processScroll-PageScrollSize)
		}
		return true
	case "pgdown":
		if !m.processOutputHidden {
			maxScroll := len(m.processOutput) - m.getProcessViewportHeight()
			if maxScroll > 0 {
				m.processScroll = minInt(maxScroll, m.processScroll+PageScrollSize)
			}
		}
		return true
	case "left", "h":
		samples := m.metricsCollector.GetSamples()
		if len(samples) > 0 {
			minIdx := -(len(samples) - 1)
			if m.historicalIndex > minIdx {
				m.historicalIndex--
			}
		}
		return true
	case "right", "l":
		if m.historicalIndex < 0 {
			m.historicalIndex++
		}
		return true
	}
	return false
}

func (m *Model) handleKeyMode(msg tea.KeyMsg) {
	switch msg.String() {
	case "esc":
		if m.historicalIndex != 0 {
			m.historicalIndex = 0
			m.processScroll = maxInt(0, len(m.processOutput)-m.getProcessViewportHeight())
		}
	case "m":
		m.processOutputHidden = !m.processOutputHidden
		if !m.processOutputHidden {
			m.processScroll = maxInt(0, len(m.processOutput)-m.getProcessViewportHeight())
		}
	case "g":
		m.chartsHidden = !m.chartsHidden
		if !m.chartsHidden {
			m.historicalIndex = 0
		}
	}
}

func (m *Model) handleContainerOutput(line string) {
	clean := stripANSIEscapeSequences(line)
	m.processOutput = append(m.processOutput, clean)
	if len(m.processOutput) > maxScrollback {
		m.processOutput = m.processOutput[1:]
	}
	if m.processScroll >= len(m.processOutput)-m.getProcessViewportHeight()-1 {
		m.processScroll = maxInt(0, len(m.processOutput)-m.getProcessViewportHeight())
	}
}

func (m *Model) handleMetric(metric PerformanceMetric) {
	m.metricsCollector.AddSample(metric)
	if m.historicalIndex < 0 {
		m.historicalIndex--
	}
}

func (m *Model) handleAPIStatus(s apiStatusMsg) {
	m.testStatus = s.State
	if s.State == constants.StateCompleted || s.State == constants.StateFailed {
		m.containerStatus = "Test " + s.State
	}
}

func (m *Model) handleAPIMetrics(metric PerformanceMetric) {
	m.metricsCollector.AddSample(metric)
	if m.historicalIndex < 0 {
		m.historicalIndex--
	}
	logging.LogMetricsParsing("received API metrics in TUI", "success", true, "ops_per_sec", metric.OpsPerSec, "source", "api")
}

func (m *Model) handleMultiNodeMetrics(update MultiNodeMetricsUpdate) {
	m.metricsCollector.AddMultiNodeSample(&update)
	if m.historicalIndex < 0 {
		m.historicalIndex--
	}
	if update.Aggregated != nil {
		logging.LogMetricsParsing("received multi-node metrics in TUI", "success", true,
			"ops_per_sec", update.Aggregated.OpsPerSec, "active_nodes", len(update.PerNode), "source", "multi-node-api")
	}
}

// addStderrMessage adds a stderr message with red highlighting
func (m *Model) addStderrMessage(msg string) {
	// Add red color highlighting for stderr messages
	coloredMsg := "\033[31m[stderr] " + msg + "\033[0m"

	// Add to process output buffer
	m.processOutput = append(m.processOutput, coloredMsg)
	if len(m.processOutput) > maxScrollback {
		m.processOutput = m.processOutput[1:]
	}

	// Auto-scroll to bottom
	m.processScroll = maxInt(0, len(m.processOutput)-m.getProcessViewportHeight())
}

// addSptMessage adds a message to both the display and debug buffer with yellow highlighting
func (m *Model) addSptMessage(msg string) {
	// Add yellow color highlighting for spt messages
	coloredMsg := "\033[33m[spt] " + msg + "\033[0m"

	// Add to process output buffer
	m.processOutput = append(m.processOutput, coloredMsg)
	if len(m.processOutput) > maxScrollback {
		m.processOutput = m.processOutput[1:]
	}

	// Auto-scroll to bottom
	m.processScroll = maxInt(0, len(m.processOutput)-m.getProcessViewportHeight())
}

// View renders the TUI
func (m Model) View() string {
	if m.width == 0 || m.height == 0 {
		return "Initializing..."
	}

	// Render live view panel
	liveViewPanel := m.renderLiveViewPanel()

	// Conditionally render process output viewport
	var processViewport string
	if !m.processOutputHidden {
		processHeight := m.getProcessViewportHeight()
		processViewport = m.renderProcessViewport(processHeight)
	} else {
		// When hidden, render empty space to maintain layout
		processViewport = ""
	}

	// Render status line
	statusLine := m.renderStatusLine()

	// Calculate chart widths (33%-33%-33% split)
	// Use conservative margin to prevent clipping
	// The -6 accounts for:
	// - Terminal edge margins (2)
	// - Extra borders when charts are side by side (2)
	// - Safety buffer (2)
	safeWidth := m.width - 6

	// Divide evenly among three charts
	chartWidth := safeWidth / 3

	opsChartWidth := chartWidth
	latencyChartWidth := chartWidth
	failureChartWidth := chartWidth

	// Distribute any remainder to prevent wasted space
	remainder := safeWidth - (chartWidth * 3)
	if remainder > 0 {
		latencyChartWidth += remainder // Give extra to middle chart
	}

	// Render charts conditionally
	var chartsLine string
	if !m.chartsHidden {
		// Get the selected sample index for highlighting
		selectedIndex := m.getSelectedSampleIndex()
		isHistorical := m.historicalIndex < 0

		// Render all three chart viewports
		opsChart := m.renderOpsChartViewport(opsChartWidth, selectedIndex, isHistorical)
		latencyChart := m.renderLatencyChartViewport(latencyChartWidth, selectedIndex, isHistorical)
		failureChart := m.renderFailureChartViewport(failureChartWidth, selectedIndex, isHistorical)

		// Combine charts side by side
		chartsLine = m.combineChartsHorizontally(opsChart, latencyChart, failureChart)
	} else {
		chartsLine = ""
	}

	// Combine all parts in new layout order: Live View -> [Charts] -> [Process Output] -> Status
	var result string
	if !m.chartsHidden && !m.processOutputHidden {
		result = liveViewPanel + "\n" + chartsLine + "\n" + processViewport + "\n" + statusLine
	} else if !m.chartsHidden && m.processOutputHidden {
		result = liveViewPanel + "\n" + chartsLine + "\n" + statusLine
	} else if m.chartsHidden && !m.processOutputHidden {
		result = liveViewPanel + "\n" + processViewport + "\n" + statusLine
	} else {
		// Both charts and process output hidden
		result = liveViewPanel + "\n" + statusLine
	}

	// Optional frame dump for diagnostics
	m.maybeDumpFrame(result, liveViewPanel, statusLine)
	return result
}

// maybeDumpFrame appends the current frame to the configured dump file (if enabled).
func (m Model) maybeDumpFrame(fullFrame, liveViewPanel, statusLine string) {
	if m.frameDumpPath == "" || m.frameDumpCount == nil {
		return
	}
	if *m.frameDumpCount >= m.frameDumpMax {
		return
	}
	// Rate limit: at most one frame per configured interval
	if m.frameDumpLast != nil && !m.frameDumpLast.IsZero() && time.Since(*m.frameDumpLast) < m.frameDumpMinInterval {
		return
	}
	content := fullFrame
	if m.frameDumpLiveOnly {
		content = liveViewPanel + "\n" + statusLine
	}
	if m.frameDumpStrip {
		content = ansiEscapeRegex.ReplaceAllString(content, "")
	}
	header := fmt.Sprintf("\n--- frame %d @ %s ---\n", *m.frameDumpCount+1, time.Now().Format(time.RFC3339Nano))
	f, err := os.OpenFile(m.frameDumpPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err == nil {
		_, _ = f.WriteString(header)
		_, _ = f.WriteString(content)
		_, _ = f.WriteString("\n")
		_ = f.Close()
	}
	// increment (shared via pointer to survive value receiver copy)
	*m.frameDumpCount++
	if m.frameDumpLast != nil {
		now := time.Now()
		*m.frameDumpLast = now
	}
}

func (m Model) getProcessViewportHeight() int {
	// If process output is hidden, return 0
	if m.processOutputHidden {
		return 0
	}

	// Total height minus:
	// - status line (StatusLineHeight)
	// - chart viewport (ChartViewportHeight with borders) - only if charts visible
	// - live view (LiveViewHeight with borders)
	// - extra padding (ExtraLayoutPadding)
	// Note: spt viewport removed, so process output gets more space

	// Calculate dynamic height but cap at ProcessViewportMaxHeight lines maximum to prevent overflow
	var dynamicHeight int
	if m.chartsHidden {
		// When charts are hidden, process output gets more space
		dynamicHeight = m.height - LayoutHeightWithoutCharts
	} else {
		// Normal calculation with charts visible
		dynamicHeight = m.height - LayoutHeightWithCharts
	}
	// Enforce minimum and maximum height bounds
	return minInt(ProcessViewportMaxHeight, maxInt(ProcessViewportMinHeight, dynamicHeight))
}

func (m Model) getLiveViewHeight() int {
	return LiveViewHeight // Fixed height for live view panel (compact: 1 header + 2 data rows + borders)
}

func (m Model) renderProcessViewport(height int) string {
	title := " Process Output "
	style := m.borderStyle

	// Get visible lines without highlighting (simplified from spt viewport)
	visibleLines := m.getVisibleLines(m.processOutput, m.processScroll, height)
	content := strings.Join(visibleLines, "\n")

	// Add scroll indicator
	if len(m.processOutput) > height {
		scrollInfo := fmt.Sprintf(" [%d-%d/%d] ",
			m.processScroll+1,
			minInt(m.processScroll+height, len(m.processOutput)),
			len(m.processOutput))
		title += scrollInfo
	}

	return style.
		Width(m.width - 2).
		Height(height + 2).
		Render(lipgloss.NewStyle().Render(title) + "\n" + content)
}

func (m Model) renderLiveViewPanel() string {
	height := m.getLiveViewHeight()

	if m.liveViewRenderer == nil {
		// Return empty panel with placeholder content if renderer not initialized
		emptyStyle := m.borderStyle.
			Width(m.width - 2).
			Height(height)
		return emptyStyle.Render("Live View (Initializing...)")
	}

	// Use the allocated height for live view rendering
	innerWidth := m.width - 2
	if innerWidth < 1 {
		innerWidth = m.width
	}
	content := m.liveViewRenderer.RenderLiveView(m.metricsCollector, innerWidth)

	// Ensure it uses the full allocated height
	style := m.borderStyle.
		Width(m.width - 2).
		Height(height)

	return style.Render(content)
}

func (m Model) renderOpsChartViewport(width int, selectedIndex int, isHistorical bool) string {
	style := m.borderStyle
	height := ChartContentHeight // Fixed height for chart content (1 header + 6 bars + 1 timestamp)

	// Generate chart content with selection info
	content := m.opsChartRenderer.RenderChartWithSelection(m.metricsCollector, width-4, selectedIndex, isHistorical)

	return style.
		Width(width).
		Height(height).
		Render(content)
}

func (m Model) renderLatencyChartViewport(width int, selectedIndex int, isHistorical bool) string {
	style := m.borderStyle
	height := ChartContentHeight // Fixed height for chart content (1 header + 6 bars + 1 timestamp)

	// Generate chart content with selection info
	content := m.latencyChartRenderer.RenderChartWithSelection(m.metricsCollector, width-4, selectedIndex, isHistorical)

	return style.
		Width(width).
		Height(height).
		Render(content)
}

func (m Model) renderFailureChartViewport(width int, selectedIndex int, isHistorical bool) string {
	style := m.borderStyle
	height := ChartContentHeight // Fixed height for chart content (1 header + 6 bars + 1 timestamp)

	// Generate chart content with selection info
	content := m.failureChartRenderer.RenderChartWithSelection(m.metricsCollector, width-4, selectedIndex, isHistorical)

	return style.
		Width(width).
		Height(height).
		Render(content)
}

// combineChartsHorizontally combines chart viewports side by side with no gap
func (m Model) combineChartsHorizontally(charts ...string) string {
	if len(charts) == 0 {
		return ""
	}

	// Split each chart into lines
	chartLines := make([][]string, len(charts))
	maxLines := 0

	for i, chart := range charts {
		chartLines[i] = strings.Split(chart, "\n")
		if len(chartLines[i]) > maxLines {
			maxLines = len(chartLines[i])
		}
	}

	// Pad shorter charts with empty lines
	for i := range chartLines {
		for len(chartLines[i]) < maxLines {
			chartLines[i] = append(chartLines[i], "")
		}
	}

	// Combine lines directly with no gap
	var combined []string
	for row := 0; row < maxLines; row++ {
		line := ""
		for chartIdx := range chartLines {
			line += chartLines[chartIdx][row]
		}
		combined = append(combined, line)
	}

	return strings.Join(combined, "\n")
}

func (m Model) renderStatusLine() string {
	now := time.Now()
	elapsed := now.Sub(m.startTime)

	// Determine test status display
	testStatusDisplay := constants.StateIdle
	if m.testStatus != "" {
		testStatusDisplay = m.testStatus
	}

	status := fmt.Sprintf(" %s | %s | %s | Test: %s | %s ",
		now.Format("2006-01-02"),
		now.Format("15:04:05"),
		m.containerStatus,
		testStatusDisplay,
		formatDuration(elapsed))

	// Add navigation help
	help := " ↑/↓: Scroll | ←/→: Scan | g: Graphs | m: Messages | q: Quit "

	statusWidth := len(status)
	helpWidth := len(help)
	padding := m.width - statusWidth - helpWidth

	if padding > 0 {
		status += strings.Repeat(" ", padding) + help
	}

	return m.statusStyle.Width(m.width).Render(status)
}

func (m Model) getVisibleLines(lines []string, scroll int, height int) []string {
	// Calculate max width for process viewport (account for borders and padding)
	maxWidth := m.width - ViewportContentPadding // borders + padding
	if maxWidth <= 0 {
		maxWidth = 80 // Default width for tests or very small windows
	}
	return m.getVisibleLinesWithWidth(lines, scroll, height, maxWidth)
}

func (m Model) getVisibleLinesWithWidth(lines []string, scroll int, height int, maxWidth int) []string {
	if len(lines) == 0 {
		return []string{"(empty)"}
	}

	start := scroll
	end := minInt(start+height, len(lines))

	if start >= len(lines) {
		start = maxInt(0, len(lines)-height)
		end = len(lines)
	}

	visible := make([]string, height)

	// Copy and truncate lines to prevent wrapping
	for i := 0; i < end-start; i++ {
		line := lines[start+i]
		clean := stripANSIEscapeSequences(line)
		if textutil.RuneLen(clean) > maxWidth && maxWidth > 0 {
			if containsBoxDrawing(clean) {
				visible[i] = truncateBoxDrawingLine(line, maxWidth)
			} else if maxWidth > 3 {
				visible[i] = line[:maxWidth-3] + "..."
			} else {
				visible[i] = line[:maxWidth]
			}
		} else {
			visible[i] = line
		}
	}

	// Fill remaining space with empty lines
	for i := end - start; i < height; i++ {
		visible[i] = ""
	}

	return visible
}

// getVisibleLinesWithHighlight returns visible lines with optional highlighting for selected timestamp
// getVisibleLinesWithHighlight moved to test helpers (unused in production)

func formatDuration(d time.Duration) string {
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}

func minInt(a, b int) int { return mathutil.MinInt(a, b) }

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// getSelectedSampleIndex converts the historical navigation index to an actual array index
// Returns -1 if no valid sample exists
func (m Model) getSelectedSampleIndex() int {
	samples := m.metricsCollector.GetSamples()
	if len(samples) == 0 {
		return -1
	}

	// Convert historical index to array index
	// historicalIndex 0 = latest sample (len-1)
	// historicalIndex -1 = one back (len-2)
	// historicalIndex -2 = two back (len-3), etc.
	actualIndex := len(samples) - 1 + m.historicalIndex

	// Bounds check
	if actualIndex < 0 || actualIndex >= len(samples) {
		return -1
	}

	return actualIndex
}

// getSelectedTimestamp returns the SptTimestamp of the currently selected sample
// Returns empty string if no sample is selected or in live mode
// getSelectedTimestamp moved to test helpers (unused in production)

// isSptDataLine checks if a line is a Spt data line.
// If targetTimestamp is provided, it also verifies the line contains that timestamp.
// If targetTimestamp is empty, it just checks if the line appears to be a data line.
// isSptDataLine moved to test helpers (unused in production)

// stripANSIEscapeSequences removes ANSI color/formatting escape sequences from a string
func stripANSIEscapeSequences(s string) string {
	return ansiEscapeRegex.ReplaceAllString(s, "")
}

func containsBoxDrawing(s string) bool {
	return strings.ContainsAny(s, "┌┬┐└┴┘├┼┤─│")
}

func truncateBoxDrawingLine(line string, maxWidth int) string {
	if maxWidth <= 0 {
		return ""
	}
	clean := stripANSIEscapeSequences(line)
	runes := []rune(clean)
	if len(runes) <= maxWidth {
		return line
	}
	truncated := string(runes[:maxWidth])
	prefix, suffix := extractANSIPrefixSuffix(line)
	return prefix + truncated + suffix
}

func extractANSIPrefixSuffix(s string) (string, string) {
	matches := ansiEscapeRegex.FindAllStringIndex(s, -1)
	if len(matches) == 0 {
		return "", ""
	}
	prefix := ""
	suffix := ""
	if matches[0][0] == 0 {
		prefix = s[matches[0][0]:matches[0][1]]
	}
	last := matches[len(matches)-1]
	if last[1] == len(s) {
		suffix = s[last[0]:last[1]]
	}
	return prefix, suffix
}

// StartTUI starts the TUI with Docker integration
func StartTUI() error {
	// logging.LogTUIEvent("starting", "interactive", true)
	p := tea.NewProgram(InitialModel(), tea.WithAltScreen())
	_, err := p.Run()
	if err != nil {
		logging.LogError("tui", "failed to run TUI", err)
	}
	return err
}

// StartTUIWithScenario starts the TUI with a scenario file and launches the container
func StartTUIWithScenario(image string, scenarioPath string) error {
	// Create empty params for backward compatibility - endpoint args will be empty
	params := scenario.ScenarioParams{}
	return StartTUIWithScenarioAndParamsWithTrace(image, scenarioPath, params, "", nil, "", false)
}

// StartTUIWithScenarioAndParams starts the TUI with a scenario file and scenario parameters
func StartTUIWithScenarioAndParams(image string, scenarioPath string, params scenario.ScenarioParams, apiPort string, setSummarySink func(func(string))) error {
	return StartTUIWithScenarioAndParamsWithTrace(image, scenarioPath, params, apiPort, setSummarySink, "", false)
}

// StartTUIWithScenarioAndParamsWithTrace starts the TUI with optional full-output trace capture.
func StartTUIWithScenarioAndParamsWithTrace(image string, scenarioPath string, params scenario.ScenarioParams, apiPort string, setSummarySink func(func(string)), tracePath string, traceAppend bool) error {
	return StartTUIWithScenarioAndParamsNetworkModeWithTrace(image, scenarioPath, params, apiPort, constants.DefaultNetworkMode, "", setSummarySink, tracePath, traceAppend)
}

// RunOptions carries orchestration-only launch settings separately from
// scenario configuration. Existing TUI entrypoints remain compatibility wrappers.
type RunOptions struct {
	Context              context.Context
	Verbose              bool
	APIPort              string
	NetworkMode          string
	ResultsRoot          string
	AutoTerminateSeconds int
	SetSummarySink       func(func(string))
	TracePath            string
	TraceAppend          bool
	ScenarioContent      []byte
	DefaultsContent      []byte
	LaunchHooks          LaunchHooks
}

// StartTUIWithScenarioRunOptions starts a local TUI run with explicit
// orchestration lifecycle options.
func StartTUIWithScenarioRunOptions(
	image, scenarioPath string, params scenario.ScenarioParams, options RunOptions,
) error {
	if options.AutoTerminateSeconds > 0 {
		return startTUIWithScenarioAndParamsNetworkModeTimeoutTrace(
			options.Context,
			image, scenarioPath, params, options.APIPort, options.NetworkMode,
			options.ResultsRoot, options.AutoTerminateSeconds, options.SetSummarySink,
			options.TracePath, options.TraceAppend, options.ScenarioContent,
			options.DefaultsContent, options.LaunchHooks, options.Verbose)
	}
	return startTUIWithScenarioAndParamsWithNetworkModeTrace(
		options.Context,
		image, scenarioPath, params, options.APIPort, options.NetworkMode,
		options.ResultsRoot, options.SetSummarySink, options.TracePath,
		options.TraceAppend, options.ScenarioContent, options.DefaultsContent,
		options.LaunchHooks, options.Verbose)
}

// StartTUIWithScenarioAndParamsNetworkModeWithTrace starts the TUI with a selected Docker network mode.
func StartTUIWithScenarioAndParamsNetworkModeWithTrace(image string, scenarioPath string, params scenario.ScenarioParams, apiPort string, networkMode string, resultsRoot string, setSummarySink func(func(string)), tracePath string, traceAppend bool) error {
	return startTUIWithScenarioAndParamsWithNetworkModeTrace(context.Background(), image, scenarioPath, params, apiPort, networkMode, resultsRoot, setSummarySink, tracePath, traceAppend, nil, nil, LaunchHooks{}, false)
}

// StartTUIWithScenarioContentAndParamsWithTrace starts the TUI with caller-provided scenario/defaults content.
func StartTUIWithScenarioContentAndParamsWithTrace(image string, scenarioPath string, params scenario.ScenarioParams, apiPort string, networkMode string, resultsRoot string, setSummarySink func(func(string)), tracePath string, traceAppend bool, scenarioContent, defaultsContent []byte) error {
	return startTUIWithScenarioAndParamsWithNetworkModeTrace(context.Background(), image, scenarioPath, params, apiPort, networkMode, resultsRoot, setSummarySink, tracePath, traceAppend, scenarioContent, defaultsContent, LaunchHooks{}, false)
}

func startTUIWithScenarioAndParamsWithNetworkModeTrace(runCtx context.Context, image string, scenarioPath string, params scenario.ScenarioParams, apiPort string, networkMode string, resultsRoot string, setSummarySink func(func(string)), tracePath string, traceAppend bool, scenarioContent, defaultsContent []byte, launchHooks LaunchHooks, verbose bool) error {
	launchHooks = ensureLaunchState(launchHooks)
	model := InitialModel()
	model.verbose = verbose
	if params.MinimalTUI {
		model.processOutputHidden = true
		model.chartsHidden = true
	}
	model.scenarioFile = scenarioPath        // Store for cleanup
	model.keepScenario = params.KeepScenario // Get from params
	model.scenarioParams = &params           // Store scenario params for live view

	// Initialize live view renderer with scenario params
	model.liveViewRenderer = NewLiveViewRenderer(&params)

	// Create Docker manager
	dm, err := newTUIDockerManager()
	if err != nil {
		logging.LogError("tui", "failed to create Docker manager", err)
		return fmt.Errorf("failed to create Docker manager: %w", err)
	}
	defer dm.Close()

	// Create the orchestrator for API-based control
	orchestrator := NewTestOrchestrator(dm, apiPort, resultsRoot)
	orchestrator.SetNetworkMode(networkMode)
	if err := launchHooks.RegisterResourceFinalizer(
		orchestrator.FinalizeDiagnosticsAndCleanupOutcome); err != nil {
		return fmt.Errorf("register session resource finalizer: %w", err)
	}
	model.orchestrator = orchestrator

	model.dockerManager = dm

	p, traceFile, err := newTUIProgramWithTrace(model, tracePath, traceAppend)
	if err != nil {
		return err
	}
	if traceFile != nil {
		defer func() { _ = traceFile.Close() }()
	}

	if setSummarySink != nil {
		setSummarySink(func(line string) {
			p.Send(sptMessageMsg(line))
		})
	}

	// Set up callbacks to send messages to TUI
	orchestrator.SetCallbacks(
		// Status updates
		func(status *TestStatus) {
			if status != nil {
				p.Send(apiStatusMsg(*status))
			}
		},
		// Metrics updates from API
		func(update *MultiNodeMetricsUpdate) {
			if update != nil {
				p.Send(multiNodeMetricsMsg(*update))
			}
		},
		// Container output (stdout + orchestrator messages)
		func(line string) {
			p.Send(containerOutputMsg(line))
		},
		// Container stderr messages
		func(err string) {
			p.Send(containerStderrMsg(err))
		},
	)

	launchResult, cancelLaunch := startTUILaunch(runCtx, p, launchHooks, func(ctx context.Context) error {
		p.Send(sptMessageMsg("Starting Spt in API mode..."))
		p.Send(containerStatusMsg("Starting"))
		var startErr error
		if scenarioContent != nil {
			startErr = orchestrator.StartTestWithContentAndLaunchHooks(
				ctx, image, params, scenarioContent, defaultsContent, launchHooks)
		} else {
			startErr = orchestrator.StartTestWithLaunchHooks(ctx, image, params, launchHooks)
		}
		if startErr == nil {
			p.Send(containerStatusMsg("API Running"))
			p.Send(sptMessageMsg("Test started via API"))
		}
		return startErr
	}, func(startErr error) {
		p.Send(sptMessageMsg(fmt.Sprintf("Error starting test: %v", startErr)))
		logging.LogError("tui", "failed to start test", startErr)
		p.Send(containerStatusMsg("Failed"))
	})

	// Run the program
	finalModel, programErr := p.Run()
	cancelLaunch()
	launchErr := finishTUILaunch(runCtx, programErr, launchResult, launchHooks)

	// Stop the test and cleanup via orchestrator
	var stopErr error
	if orchestrator != nil && launchHooks.Submitted() && !launchHooks.SessionManaged() {
		fmt.Println("Stopping Spt test...")
		if stopErr = orchestrator.StopTest(); stopErr != nil {
			fmt.Printf("Warning: Failed to stop test cleanly: %v\n", stopErr)
			logging.LogError("tui", "failed to stop test", stopErr)
		} else {
			fmt.Println("Test stopped successfully.")
		}
	}

	// Cleanup scenario file if needed (the orchestrator handles its own, this is for the input file)
	if scenarioPath != "" && !params.KeepScenario && !launchHooks.SessionManaged() {
		logging.GetLogger().Debug("Cleaning up input scenario file", "file", scenarioPath)
		_ = os.Remove(scenarioPath)
	}

	// Cleanup live view renderer
	if m, ok := finalModel.(Model); ok {
		if m.liveViewRenderer != nil {
			m.liveViewRenderer.Stop()
		}
	}

	return errors.Join(launchErr, stopErr, orchestrator.CompletionError())
}

// StartTUIWithMultiHostOrchestrator starts the TUI with a pre-configured multi-host orchestrator
func StartTUIWithMultiHostOrchestrator(orchestrator *MultiHostOrchestrator, image string, scenarioPath string, params scenario.ScenarioParams, setSummarySink func(func(string))) error {
	return StartTUIWithMultiHostOrchestratorWithTrace(orchestrator, image, scenarioPath, params, setSummarySink, "", false)
}

// StartTUIWithMultiHostOrchestratorWithTrace starts the multi-host TUI with optional full-output trace capture.
func StartTUIWithMultiHostOrchestratorWithTrace(orchestrator *MultiHostOrchestrator, image string, scenarioPath string, params scenario.ScenarioParams, setSummarySink func(func(string)), tracePath string, traceAppend bool) error {
	return startTUIWithMultiHostOrchestratorWithTrace(context.Background(), orchestrator, image, scenarioPath, params, setSummarySink, tracePath, traceAppend, nil, nil, LaunchHooks{}, false)
}

// StartTUIWithMultiHostOrchestratorContentWithTrace starts the multi-host TUI
// with caller-provided scenario/defaults content.
func StartTUIWithMultiHostOrchestratorContentWithTrace(orchestrator *MultiHostOrchestrator, image string, scenarioPath string, params scenario.ScenarioParams, setSummarySink func(func(string)), tracePath string, traceAppend bool, scenarioContent, defaultsContent []byte) error {
	return startTUIWithMultiHostOrchestratorWithTrace(context.Background(), orchestrator, image, scenarioPath, params, setSummarySink, tracePath, traceAppend, scenarioContent, defaultsContent, LaunchHooks{}, false)
}

// StartTUIWithMultiHostRunOptions starts a host-orchestrated TUI run with
// explicit orchestration lifecycle options.
func StartTUIWithMultiHostRunOptions(
	orchestrator *MultiHostOrchestrator,
	image, scenarioPath string,
	params scenario.ScenarioParams,
	options RunOptions,
) error {
	if options.AutoTerminateSeconds > 0 {
		return startTUIWithMultiHostOrchestratorTimeoutWithTrace(
			options.Context,
			orchestrator, image, scenarioPath, params, options.AutoTerminateSeconds,
			options.SetSummarySink, options.TracePath, options.TraceAppend,
			options.ScenarioContent, options.DefaultsContent, options.LaunchHooks, options.Verbose)
	}
	return startTUIWithMultiHostOrchestratorWithTrace(
		options.Context,
		orchestrator, image, scenarioPath, params, options.SetSummarySink,
		options.TracePath, options.TraceAppend, options.ScenarioContent,
		options.DefaultsContent, options.LaunchHooks, options.Verbose)
}

func startTUIWithMultiHostOrchestratorWithTrace(runCtx context.Context, orchestrator *MultiHostOrchestrator, image string, scenarioPath string, params scenario.ScenarioParams, setSummarySink func(func(string)), tracePath string, traceAppend bool, scenarioContent, defaultsContent []byte, launchHooks LaunchHooks, verbose bool) error {
	launchHooks = ensureLaunchState(launchHooks)
	model := InitialModel()
	model.verbose = verbose
	if params.MinimalTUI {
		model.processOutputHidden = true
		model.chartsHidden = true
	}
	model.scenarioFile = scenarioPath        // Store for cleanup
	model.keepScenario = params.KeepScenario // Get from params
	model.scenarioParams = &params           // Store scenario params for live view

	// Initialize live view renderer with scenario params
	model.liveViewRenderer = NewLiveViewRenderer(&params)

	// For multi-host, we'll use the first ready host's Docker manager for TUI integration
	// This is primarily for the TUI's internal needs
	readyHosts := orchestrator.GetReadyHosts()
	if len(readyHosts) == 0 {
		return fmt.Errorf("no ready hosts available for TUI startup")
	}

	// Use the first ready host's Docker manager for TUI integration
	model.dockerManager = readyHosts[0].DockerManager

	// Create a multi-host aware test orchestrator wrapper
	// This will coordinate with the MultiHostOrchestrator
	if err := launchHooks.RegisterResourceFinalizer(
		orchestrator.FinalizeDiagnosticsAndCleanupOutcome); err != nil {
		return fmt.Errorf("register session resource finalizer: %w", err)
	}
	multiOrchestrator := newMultiHostTUITestOrchestrator(orchestrator)
	model.orchestrator = multiOrchestrator
	// Provide baseline generator so Model.Init can emit it safely at startup
	model.baselineFn = multiOrchestrator.BuildBaselineUpdate

	p, traceFile, err := newTUIProgramWithTrace(model, tracePath, traceAppend)
	if err != nil {
		return err
	}
	if traceFile != nil {
		defer func() { _ = traceFile.Close() }()
	}

	if setSummarySink != nil {
		setSummarySink(func(line string) {
			p.Send(sptMessageMsg(line))
		})
	}

	// Route orchestrator progress messages into the TUI messages pane
	orchestrator.SetNotifier(func(msg string) {
		p.Send(sptMessageMsg(msg))
	})

	// Provide a message sink for other orchestrator-driven components (e.g., entry log relay)
	// Route Spt log lines without the [spt] tag to avoid double-tagging.
	multiOrchestrator.SetMessageSink(func(msg string) {
		if strings.HasPrefix(msg, "[SPT] ") {
			p.Send(containerOutputMsg(msg)) // keep [SPT], omit [spt]
			return
		}
		p.Send(sptMessageMsg(msg))
	})

	// Set up callbacks to send messages to TUI
	multiOrchestrator.SetCallbacks(
		// Status updates from any host
		func(status *TestStatus) {
			if status != nil {
				p.Send(apiStatusMsg(*status))
			}
		},
		// Aggregated metrics from all hosts
		func(update *MultiNodeMetricsUpdate) {
			if update != nil {
				p.Send(multiNodeMetricsMsg(*update))
			}
		},
		// Container output from any host
		func(line string) {
			p.Send(containerOutputMsg(line))
		},
		// Container stderr from any host
		func(err string) {
			p.Send(containerStderrMsg(err))
		},
	)

	launchResult, cancelLaunch := startTUILaunch(runCtx, p, launchHooks, func(ctx context.Context) error {
		p.Send(sptMessageMsg("Starting distributed test across hosts..."))
		p.Send(containerStatusMsg("Starting"))
		var startErr error
		if scenarioContent != nil {
			startErr = multiOrchestrator.StartTestWithContentAndLaunchHooks(
				ctx, image, params, scenarioContent, defaultsContent, launchHooks)
		} else {
			startErr = multiOrchestrator.StartTestWithLaunchHooks(ctx, image, params, launchHooks)
		}
		if startErr == nil {
			p.Send(containerStatusMsg("Multi-Host Running"))
			p.Send(sptMessageMsg(fmt.Sprintf("Multi-host test started on %d hosts", orchestrator.GetRunningHostCount())))
		}
		return startErr
	}, func(startErr error) {
		p.Send(sptMessageMsg(fmt.Sprintf("Error starting multi-host test monitoring: %v", startErr)))
		logging.LogError("tui-multi", "failed to start test monitoring", startErr)
		p.Send(containerStatusMsg("Failed"))
	})

	// Run the program
	finalModel, programErr := p.Run()
	cancelLaunch()
	launchErr := finishTUILaunch(runCtx, programErr, launchResult, launchHooks)

	// Stop all containers and cleanup
	var stopErr error
	if orchestrator != nil && !launchHooks.SessionManaged() {
		fmt.Printf("Stopping containers on %d hosts...\n", orchestrator.GetHostCount())
		cleanupCtx, cancelCleanup := boundedDetachedContext(runCtx, constants.ContainerCleanupTimeout)
		stopErr = orchestrator.StopAllContainers(cleanupCtx)
		cancelCleanup()
		if stopErr != nil {
			fmt.Printf("Warning: Failed to stop all containers cleanly: %v\n", stopErr)
			logging.LogError("tui-multi", "failed to stop containers", stopErr)
		} else {
			fmt.Println("All containers stopped successfully.")
		}
	}

	// Cleanup scenario file if needed
	if scenarioPath != "" && !params.KeepScenario && !launchHooks.SessionManaged() {
		logging.GetLogger().Debug("Cleaning up input scenario file", "file", scenarioPath)
		_ = os.Remove(scenarioPath)
	}

	// Cleanup live view renderer
	if m, ok := finalModel.(Model); ok {
		if m.liveViewRenderer != nil {
			m.liveViewRenderer.Stop()
		}
	}

	return errors.Join(launchErr, stopErr, multiOrchestrator.CompletionError())
}

// StartTUIWithScenarioAndParamsTimeout starts the TUI (single host) and exits after autoTerminateSeconds (>0).
func StartTUIWithScenarioAndParamsTimeout(image string, scenarioPath string, params scenario.ScenarioParams, apiPort string, autoTerminateSeconds int, setSummarySink func(func(string))) error {
	return StartTUIWithScenarioAndParamsTimeoutWithTrace(image, scenarioPath, params, apiPort, autoTerminateSeconds, setSummarySink, "", false)
}

// StartTUIWithScenarioAndParamsTimeoutWithTrace starts the TUI (single host), exits after timeout, and can capture full output.
func StartTUIWithScenarioAndParamsTimeoutWithTrace(image string, scenarioPath string, params scenario.ScenarioParams, apiPort string, autoTerminateSeconds int, setSummarySink func(func(string)), tracePath string, traceAppend bool) error {
	return StartTUIWithScenarioAndParamsNetworkModeTimeoutWithTrace(image, scenarioPath, params, apiPort, constants.DefaultNetworkMode, "", autoTerminateSeconds, setSummarySink, tracePath, traceAppend)
}

// StartTUIWithScenarioAndParamsNetworkModeTimeoutWithTrace starts the TUI with a selected Docker network mode and optional timeout.
func StartTUIWithScenarioAndParamsNetworkModeTimeoutWithTrace(image string, scenarioPath string, params scenario.ScenarioParams, apiPort string, networkMode string, resultsRoot string, autoTerminateSeconds int, setSummarySink func(func(string)), tracePath string, traceAppend bool) error {
	if autoTerminateSeconds <= 0 {
		return StartTUIWithScenarioAndParamsNetworkModeWithTrace(image, scenarioPath, params, apiPort, networkMode, resultsRoot, setSummarySink, tracePath, traceAppend)
	}
	return startTUIWithScenarioAndParamsNetworkModeTimeoutTrace(context.Background(), image, scenarioPath, params, apiPort, networkMode, resultsRoot, autoTerminateSeconds, setSummarySink, tracePath, traceAppend, nil, nil, LaunchHooks{}, false)
}

// StartTUIWithScenarioContentAndParamsTimeoutWithTrace starts the TUI with caller-provided content and optional timeout.
func StartTUIWithScenarioContentAndParamsTimeoutWithTrace(image string, scenarioPath string, params scenario.ScenarioParams, apiPort string, networkMode string, resultsRoot string, autoTerminateSeconds int, setSummarySink func(func(string)), tracePath string, traceAppend bool, scenarioContent, defaultsContent []byte) error {
	if autoTerminateSeconds <= 0 {
		return StartTUIWithScenarioContentAndParamsWithTrace(image, scenarioPath, params, apiPort, networkMode, resultsRoot, setSummarySink, tracePath, traceAppend, scenarioContent, defaultsContent)
	}
	return startTUIWithScenarioAndParamsNetworkModeTimeoutTrace(context.Background(), image, scenarioPath, params, apiPort, networkMode, resultsRoot, autoTerminateSeconds, setSummarySink, tracePath, traceAppend, scenarioContent, defaultsContent, LaunchHooks{}, false)
}

func startTUIWithScenarioAndParamsNetworkModeTimeoutTrace(runCtx context.Context, image string, scenarioPath string, params scenario.ScenarioParams, apiPort string, networkMode string, resultsRoot string, autoTerminateSeconds int, setSummarySink func(func(string)), tracePath string, traceAppend bool, scenarioContent, defaultsContent []byte, launchHooks LaunchHooks, verbose bool) error {
	launchHooks = ensureLaunchState(launchHooks)
	model := InitialModel()
	model.verbose = verbose
	if params.MinimalTUI {
		model.processOutputHidden = true
		model.chartsHidden = true
	}
	model.scenarioFile = scenarioPath
	model.keepScenario = params.KeepScenario
	model.scenarioParams = &params
	model.liveViewRenderer = NewLiveViewRenderer(&params)

	dm, err := newTUIDockerManager()
	if err != nil {
		logging.LogError("tui", "failed to create Docker manager", err)
		return fmt.Errorf("failed to create Docker manager: %w", err)
	}
	defer dm.Close()

	orchestrator := NewTestOrchestrator(dm, apiPort, resultsRoot)
	orchestrator.SetNetworkMode(networkMode)
	if err := launchHooks.RegisterResourceFinalizer(
		orchestrator.FinalizeDiagnosticsAndCleanupOutcome); err != nil {
		return fmt.Errorf("register session resource finalizer: %w", err)
	}
	model.orchestrator = orchestrator
	model.dockerManager = dm

	p, traceFile, err := newTUIProgramWithTrace(model, tracePath, traceAppend)
	if err != nil {
		return err
	}
	if traceFile != nil {
		defer func() { _ = traceFile.Close() }()
	}

	if setSummarySink != nil {
		setSummarySink(func(line string) {
			p.Send(sptMessageMsg(line))
		})
	}

	orchestrator.SetCallbacks(
		func(status *TestStatus) {
			if status != nil {
				p.Send(apiStatusMsg(*status))
			}
		},
		func(update *MultiNodeMetricsUpdate) {
			if update != nil {
				p.Send(multiNodeMetricsMsg(*update))
			}
		},
		func(line string) { p.Send(containerOutputMsg(line)) },
		func(err string) { p.Send(containerStderrMsg(err)) },
	)

	launchResult, cancelLaunch := startTUILaunch(runCtx, p, launchHooks, func(ctx context.Context) error {
		p.Send(sptMessageMsg("Starting Spt in API mode..."))
		p.Send(containerStatusMsg("Starting"))
		var startErr error
		if scenarioContent != nil {
			startErr = orchestrator.StartTestWithContentAndLaunchHooks(
				ctx, image, params, scenarioContent, defaultsContent, launchHooks)
		} else {
			startErr = orchestrator.StartTestWithLaunchHooks(ctx, image, params, launchHooks)
		}
		if startErr == nil {
			p.Send(containerStatusMsg("API Running"))
			p.Send(sptMessageMsg("Test started via API"))
		}
		return startErr
	}, func(startErr error) {
		p.Send(sptMessageMsg(fmt.Sprintf("Error starting test: %v", startErr)))
		logging.LogError("tui", "failed to start test", startErr)
		p.Send(containerStatusMsg("Failed"))
	})

	// Auto-terminate timer
	go func() {
		time.Sleep(time.Duration(autoTerminateSeconds) * time.Second)
		p.Send(sptMessageMsg(fmt.Sprintf("Auto-terminate after %d seconds", autoTerminateSeconds)))
		p.Send(autoTerminateMsg{})
	}()

	finalModel, programErr := p.Run()
	cancelLaunch()
	launchErr := finishTUILaunch(runCtx, programErr, launchResult, launchHooks)

	var stopErr error
	if orchestrator != nil && launchHooks.Submitted() && !launchHooks.SessionManaged() {
		fmt.Println("Stopping Spt test...")
		if stopErr = orchestrator.StopTest(); stopErr != nil {
			fmt.Printf("Warning: Failed to stop test cleanly: %v\n", stopErr)
			logging.LogError("tui", "failed to stop test", stopErr)
		} else {
			fmt.Println("Test stopped successfully.")
		}
	}

	if scenarioPath != "" && !params.KeepScenario && !launchHooks.SessionManaged() {
		logging.GetLogger().Debug("Cleaning up input scenario file", "file", scenarioPath)
		_ = os.Remove(scenarioPath)
	}
	if m, ok := finalModel.(Model); ok {
		if m.liveViewRenderer != nil {
			m.liveViewRenderer.Stop()
		}
	}
	return errors.Join(launchErr, stopErr, orchestrator.CompletionError())
}

// StartTUIWithMultiHostOrchestratorTimeout starts the TUI (multi-host) and exits after autoTerminateSeconds (>0).
func StartTUIWithMultiHostOrchestratorTimeout(orchestrator *MultiHostOrchestrator, image string, scenarioPath string, params scenario.ScenarioParams, autoTerminateSeconds int, setSummarySink func(func(string))) error {
	return StartTUIWithMultiHostOrchestratorTimeoutWithTrace(orchestrator, image, scenarioPath, params, autoTerminateSeconds, setSummarySink, "", false)
}

// StartTUIWithMultiHostOrchestratorTimeoutWithTrace starts the multi-host TUI with timeout and optional full-output trace capture.
func StartTUIWithMultiHostOrchestratorTimeoutWithTrace(orchestrator *MultiHostOrchestrator, image string, scenarioPath string, params scenario.ScenarioParams, autoTerminateSeconds int, setSummarySink func(func(string)), tracePath string, traceAppend bool) error {
	return startTUIWithMultiHostOrchestratorTimeoutWithTrace(context.Background(), orchestrator, image, scenarioPath, params, autoTerminateSeconds, setSummarySink, tracePath, traceAppend, nil, nil, LaunchHooks{}, false)
}

// StartTUIWithMultiHostOrchestratorContentTimeoutWithTrace starts the multi-host
// TUI with caller-provided content and optional auto-terminate timeout.
func StartTUIWithMultiHostOrchestratorContentTimeoutWithTrace(orchestrator *MultiHostOrchestrator, image string, scenarioPath string, params scenario.ScenarioParams, autoTerminateSeconds int, setSummarySink func(func(string)), tracePath string, traceAppend bool, scenarioContent, defaultsContent []byte) error {
	return startTUIWithMultiHostOrchestratorTimeoutWithTrace(context.Background(), orchestrator, image, scenarioPath, params, autoTerminateSeconds, setSummarySink, tracePath, traceAppend, scenarioContent, defaultsContent, LaunchHooks{}, false)
}

func startTUIWithMultiHostOrchestratorTimeoutWithTrace(runCtx context.Context, orchestrator *MultiHostOrchestrator, image string, scenarioPath string, params scenario.ScenarioParams, autoTerminateSeconds int, setSummarySink func(func(string)), tracePath string, traceAppend bool, scenarioContent, defaultsContent []byte, launchHooks LaunchHooks, verbose bool) error {
	if autoTerminateSeconds <= 0 {
		return startTUIWithMultiHostOrchestratorWithTrace(runCtx, orchestrator, image, scenarioPath, params, setSummarySink, tracePath, traceAppend, scenarioContent, defaultsContent, launchHooks, verbose)
	}
	launchHooks = ensureLaunchState(launchHooks)
	model := InitialModel()
	model.verbose = verbose
	if params.MinimalTUI {
		model.processOutputHidden = true
		model.chartsHidden = true
	}
	model.scenarioFile = scenarioPath
	model.keepScenario = params.KeepScenario
	model.scenarioParams = &params
	model.liveViewRenderer = NewLiveViewRenderer(&params)

	readyHosts := orchestrator.GetReadyHosts()
	if len(readyHosts) == 0 {
		return fmt.Errorf("no ready hosts available for TUI startup")
	}
	model.dockerManager = readyHosts[0].DockerManager

	if err := launchHooks.RegisterResourceFinalizer(
		orchestrator.FinalizeDiagnosticsAndCleanupOutcome); err != nil {
		return fmt.Errorf("register session resource finalizer: %w", err)
	}
	multiOrchestrator := newMultiHostTUITestOrchestrator(orchestrator)
	model.orchestrator = multiOrchestrator
	model.baselineFn = multiOrchestrator.BuildBaselineUpdate

	p, traceFile, err := newTUIProgramWithTrace(model, tracePath, traceAppend)
	if err != nil {
		return err
	}
	if traceFile != nil {
		defer func() { _ = traceFile.Close() }()
	}

	if setSummarySink != nil {
		setSummarySink(func(line string) {
			p.Send(sptMessageMsg(line))
		})
	}

	orchestrator.SetNotifier(func(msg string) { p.Send(sptMessageMsg(msg)) })
	multiOrchestrator.SetMessageSink(func(msg string) {
		if strings.HasPrefix(msg, "[SPT] ") {
			p.Send(containerOutputMsg(msg))
			return
		}
		p.Send(sptMessageMsg(msg))
	})
	multiOrchestrator.SetCallbacks(
		func(status *TestStatus) {
			if status != nil {
				p.Send(apiStatusMsg(*status))
			}
		},
		func(update *MultiNodeMetricsUpdate) {
			if update != nil {
				p.Send(multiNodeMetricsMsg(*update))
			}
		},
		func(line string) { p.Send(containerOutputMsg(line)) },
		func(err string) { p.Send(containerStderrMsg(err)) },
	)

	launchResult, cancelLaunch := startTUILaunch(runCtx, p, launchHooks, func(ctx context.Context) error {
		p.Send(sptMessageMsg("Starting distributed test across hosts..."))
		p.Send(containerStatusMsg("Starting"))
		var startErr error
		if scenarioContent != nil {
			startErr = multiOrchestrator.StartTestWithContentAndLaunchHooks(
				ctx, image, params, scenarioContent, defaultsContent, launchHooks)
		} else {
			startErr = multiOrchestrator.StartTestWithLaunchHooks(ctx, image, params, launchHooks)
		}
		if startErr == nil {
			p.Send(containerStatusMsg("Multi-Host Running"))
			p.Send(sptMessageMsg(fmt.Sprintf("Multi-host test started on %d hosts", orchestrator.GetRunningHostCount())))
		}
		return startErr
	}, func(startErr error) {
		p.Send(sptMessageMsg(fmt.Sprintf("Error starting multi-host test monitoring: %v", startErr)))
		logging.LogError("tui-multi", "failed to start test monitoring", startErr)
		p.Send(containerStatusMsg("Failed"))
	})

	// Auto-terminate timer
	go func() {
		time.Sleep(time.Duration(autoTerminateSeconds) * time.Second)
		p.Send(sptMessageMsg(fmt.Sprintf("Auto-terminate after %d seconds", autoTerminateSeconds)))
		p.Send(autoTerminateMsg{})
	}()

	finalModel, programErr := p.Run()
	cancelLaunch()
	launchErr := finishTUILaunch(runCtx, programErr, launchResult, launchHooks)

	var stopErr error
	if orchestrator != nil && !launchHooks.SessionManaged() {
		fmt.Printf("Stopping containers on %d hosts...\n", orchestrator.GetHostCount())
		cleanupCtx, cancelCleanup := boundedDetachedContext(runCtx, constants.ContainerCleanupTimeout)
		stopErr = orchestrator.StopAllContainers(cleanupCtx)
		cancelCleanup()
		if stopErr != nil {
			fmt.Printf("Warning: Failed to stop all containers cleanly: %v\n", stopErr)
			logging.LogError("tui-multi", "failed to stop containers", stopErr)
		} else {
			fmt.Println("All containers stopped successfully.")
		}
	}

	if scenarioPath != "" && !params.KeepScenario && !launchHooks.SessionManaged() {
		logging.GetLogger().Debug("Cleaning up input scenario file", "file", scenarioPath)
		_ = os.Remove(scenarioPath)
	}
	if m, ok := finalModel.(Model); ok {
		if m.liveViewRenderer != nil {
			m.liveViewRenderer.Stop()
		}
	}
	return errors.Join(launchErr, stopErr, multiOrchestrator.CompletionError())
}

func newProgramWithTrace(model Model, tracePath string, traceAppend bool) (*tea.Program, *os.File, error) {
	opts := []tea.ProgramOption{tea.WithAltScreen()}
	if tracePath == "" {
		return tea.NewProgram(model, opts...), nil, nil
	}
	if err := os.MkdirAll(filepath.Dir(tracePath), 0o750); err != nil {
		return nil, nil, fmt.Errorf("failed to create trace file directory: %w", err)
	}
	flags := os.O_CREATE | os.O_WRONLY
	if traceAppend {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}
	traceFile, err := os.OpenFile(tracePath, flags, 0600) // #nosec G304 -- path validated by caller and kept local
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open trace file: %w", err)
	}
	if !traceAppend {
		command := cmdline.FormatForArtifact(os.Args)
		if command == "" {
			command = "<unknown>"
		}
		_, _ = fmt.Fprintf(
			traceFile,
			"# TUI trace file: %s\n# Generated: %s\n# Command: %s\n#\n",
			tracePath,
			time.Now().Format("2006-01-02 15:04:05"),
			command,
		)
	}
	model.traceSink = newTUITraceSink(traceFile)
	return tea.NewProgram(model, opts...), traceFile, nil
}

// StartTUIWithContainer starts the TUI and immediately launches a container
func StartTUIWithContainer(image string, cmd []string) error {
	// logging.LogTUIEvent("starting", "interactive", false, "image", image, "cmd", cmd)
	model := InitialModel()

	// Initialize live view renderer with empty params (legacy container mode)
	emptyParams := scenario.ScenarioParams{}
	model.liveViewRenderer = NewLiveViewRenderer(&emptyParams)

	// Create Docker manager
	dm, err := NewDockerManager()
	if err != nil {
		logging.LogError("tui", "failed to create Docker manager", err)
		return fmt.Errorf("failed to create Docker manager: %w", err)
	}
	defer dm.Close()

	model.dockerManager = dm

	p := tea.NewProgram(model, tea.WithAltScreen())

	// Start container and streaming in a goroutine
	go func() {
		time.Sleep(100 * time.Millisecond) // Give TUI time to initialize

		p.Send(sptMessageMsg("Starting container..."))

		containerID, err := dm.StartContainer(image, cmd)
		if err != nil {
			p.Send(sptMessageMsg(fmt.Sprintf("Error starting container: %v", err)))
			return
		}

		p.Send(containerStartedMsg{containerID: containerID})
		p.Send(sptMessageMsg(fmt.Sprintf("Container started: %s", containerID[:12])))

		// Start streaming output
		dm.StreamOutput(containerID,
			func(line string) { p.Send(containerOutputMsg(line)) },
			func(line string) { p.Send(containerStderrMsg(line)) })

		p.Send(containerStatusMsg("Stopped"))
		p.Send(sptMessageMsg("Container has stopped"))
	}()

	finalModel, err := p.Run()

	// Cleanup live view renderer and dump debug messages before cleanup
	if m, ok := finalModel.(Model); ok {
		if m.liveViewRenderer != nil {
			m.liveViewRenderer.Stop()
		}
	}

	// Cleanup container
	if dm.ContainerID() != "" {
		fmt.Println("Stopping Spt container...")
		if cleanupErr := dm.Cleanup(); cleanupErr != nil {
			fmt.Printf("Warning: Failed to cleanup container: %v\n", cleanupErr)
		} else {
			fmt.Println("Container stopped successfully.")
		}
	}

	// logging.LogTUIEvent("stopped")
	return err
}
