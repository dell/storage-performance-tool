package server

import (
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

const (
	logTailDefault = 200
	logTailMax     = 2000
	logReadMax     = 2 << 20
)

var (
	ansiPattern   = regexp.MustCompile(`\x1b\[[0-9;?]*[ -/]*[@-~]`)
	secretPattern = regexp.MustCompile(`(?i)((?:access|secret)[_-]?key(?:[_-]?id)?|password|passwd|token|authorization|credentials?)(["']?\s*[:=]\s*|\s+)("[^"]*"|'[^']*'|\S+)`)
	levelWords    = map[string]string{"TRACE": "debug", "DEBUG": "debug", "INFO": levelInfo, "WARN": levelWarn, "WARNING": levelWarn, "ERROR": levelError, "FATAL": levelError, "SEVERE": levelError}
	consoleError  = regexp.MustCompile(`\b(ERROR|FATAL|Error:|failed)\b`)
	consoleWarn   = regexp.MustCompile(`\b(WARN|WARNING|Warning:)\b`)
)

type logLine struct {
	File  string `json:"file"`
	Level string `json:"level"`
	Text  string `json:"text"`
	ts    string
}

// redactLine masks anything that looks like a credential value. SPT does not log
// keys today; this is defense in depth because log text is shown in a browser.
func redactLine(s string) string {
	return secretPattern.ReplaceAllString(s, "${1}${2}[redacted]")
}

func levelRank(level string) int {
	switch level {
	case levelError:
		return 3
	case levelWarn:
		return 2
	case levelInfo:
		return 1
	}
	return 0
}

// readTail returns the last part of a regular file, at most logReadMax bytes, split into lines.
func readTail(path string) ([]string, bool) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil, false
	}
	f, err := os.Open(path) // #nosec G304 -- path is a log file inside the dashboard state dir or a catalog run folder
	if err != nil {
		return nil, false
	}
	defer func() { _ = f.Close() }()
	cut := false
	if info.Size() > logReadMax {
		if _, err := f.Seek(info.Size()-logReadMax, io.SeekStart); err != nil {
			return nil, false
		}
		cut = true
	}
	b, err := io.ReadAll(io.LimitReader(f, logReadMax))
	if err != nil {
		return nil, false
	}
	lines := strings.Split(strings.ReplaceAll(string(b), "\r\n", "\n"), "\n")
	if cut && len(lines) > 0 {
		lines = lines[1:]
	}
	return lines, cut
}

// structuredLines parses SPT's "ts | LEVEL | logger | thread | message" log format.
// Continuation lines such as stack frames take the level of the line before them.
func structuredLines(file string, raw []string) []logLine {
	out := []logLine{}
	level, ts := levelInfo, ""
	for _, s := range raw {
		if strings.TrimSpace(s) == "" {
			continue
		}
		parts := strings.SplitN(s, " | ", 3)
		if len(parts) >= 2 {
			if l, ok := levelWords[strings.ToUpper(strings.TrimSpace(parts[1]))]; ok {
				level, ts = l, parts[0]
			}
		}
		out = append(out, logLine{File: file, Level: level, Text: redactLine(s), ts: ts})
	}
	return out
}

func consoleLines(file string, raw []string) []logLine {
	out := []logLine{}
	for _, s := range raw {
		s = strings.TrimRight(ansiPattern.ReplaceAllString(s, ""), " \t")
		if strings.TrimSpace(s) == "" {
			continue
		}
		level := levelInfo
		if consoleError.MatchString(s) {
			level = levelError
		} else if consoleWarn.MatchString(s) {
			level = levelWarn
		}
		out = append(out, logLine{File: file, Level: level, Text: redactLine(s)})
	}
	return out
}

// resultLogs reads the messages and errors logs SPT writes into a run folder. The
// errors log repeats some message lines, so exact duplicates are dropped.
func resultLogs(dirs []string) ([]logLine, bool) {
	out := []logLine{}
	seen := map[string]bool{}
	truncated := false
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		names := []string{}
		for _, e := range entries {
			n := e.Name()
			if e.Type().IsRegular() && (strings.HasSuffix(n, "."+constants.ResultsArtifactSuffixMessages) || strings.HasSuffix(n, "."+constants.ResultsArtifactSuffixErrors)) {
				names = append(names, n)
			}
		}
		// messages.log first: it is the complete stream; errors.log mostly repeats it.
		sort.Slice(names, func(i, j int) bool {
			mi, mj := strings.HasSuffix(names[i], constants.ResultsArtifactSuffixMessages), strings.HasSuffix(names[j], constants.ResultsArtifactSuffixMessages)
			if mi != mj {
				return mi
			}
			return names[i] < names[j]
		})
		for _, n := range names {
			raw, cut := readTail(filepath.Join(dir, n))
			truncated = truncated || cut
			for _, l := range structuredLines(n, raw) {
				if seen[l.Text] {
					continue
				}
				seen[l.Text] = true
				out = append(out, l)
			}
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].ts < out[j].ts })
	return out, truncated
}

func filterLogs(lines []logLine, level string, tail int) []logLine {
	floor := 0
	switch level {
	case levelWarn:
		floor = 2
	case levelError:
		floor = 3
	}
	out := []logLine{}
	for _, l := range lines {
		if levelRank(l.Level) >= floor {
			out = append(out, l)
		}
	}
	if len(out) > tail {
		out = out[len(out)-tail:]
	}
	return out
}

func logQuery(r *http.Request) (string, int, bool) {
	level := r.URL.Query().Get("level")
	if level == "" {
		level = "all"
	}
	if level != "all" && level != levelWarn && level != levelError {
		return "", 0, false
	}
	tail := logTailDefault
	if raw := r.URL.Query().Get("tail"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > logTailMax {
			return "", 0, false
		}
		tail = n
	}
	return level, tail, true
}

// runFolders returns the dashboard run directory and its immediate child run folders.
func runFolders(dir string) []string {
	out := []string{dir}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return out
	}
	for _, e := range entries {
		if e.IsDir() && e.Type()&os.ModeSymlink == 0 {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	return out
}

// handleRunLog serves a dashboard run's logs. source=console is the CLI output the
// dashboard captured (interim, until SPT exposes engine logs over its API);
// source=results is the engine's messages/errors logs in the results folder.
func (a *app) handleRunLog(w http.ResponseWriter, r *http.Request) {
	if !a.loopbackHost(r) {
		writeJSON(w, 403, map[string]any{keyError: msgLoopbackOnly})
		return
	}
	runID := r.PathValue("runID")
	if !runPattern.MatchString(runID) || filepath.Base(runID) != runID || !strings.HasPrefix(runID, "mt-dashboard-") {
		writeJSON(w, 404, map[string]any{keyError: msgSavedRunNotFound})
		return
	}
	level, tail, ok := logQuery(r)
	source := r.URL.Query().Get("source")
	if source == "" {
		source = logSourceConsole
	}
	if !ok || (source != logSourceConsole && source != "results") {
		writeJSON(w, 400, map[string]any{keyError: "Unsupported log options."})
		return
	}
	var lines []logLine
	truncated := false
	if source == logSourceConsole {
		raw, cut := readTail(filepath.Join(a.stateDir, runID+".log"))
		if raw == nil {
			writeJSON(w, 404, map[string]any{keyError: "No console log was captured for this run."})
			return
		}
		lines, truncated = consoleLines(logSourceConsole, raw), cut
	} else {
		lines, truncated = resultLogs(runFolders(filepath.Join(a.results, runID)))
	}
	writeJSON(w, 200, map[string]any{keyRunID: runID, "source": source, "level": level, "interim": source == logSourceConsole, keyTruncated: truncated, "lines": filterLogs(lines, level, tail)})
}

// handleCatalogLog serves the engine logs stored in any catalog run folder.
func (a *app) handleCatalogLog(w http.ResponseWriter, r *http.Request) {
	if !a.loopbackHost(r) {
		writeJSON(w, 403, map[string]any{keyError: msgLoopbackOnly})
		return
	}
	level, tail, ok := logQuery(r)
	if !ok {
		writeJSON(w, 400, map[string]any{keyError: "Unsupported log options."})
		return
	}
	run, found := a.findCatalogRun(r.URL.Query().Get("id"))
	if !found {
		writeJSON(w, 404, map[string]any{keyError: msgCatalogRunNotFound})
		return
	}
	lines, truncated := resultLogs([]string{run.dir})
	writeJSON(w, 200, map[string]any{keyRunID: run.ID, "source": "results", "level": level, "interim": false, keyTruncated: truncated, "lines": filterLogs(lines, level, tail)})
}
