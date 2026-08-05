package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/dell/storage-performance-tool/cli/internal/results/summary"
)

type summaryTraceWriter struct {
	output   io.Writer
	trace    *os.File
	traceErr error
}

func newSummaryTraceWriter(output io.Writer, tracePath string) (*summaryTraceWriter, error) {
	if output == nil {
		output = io.Discard
	}
	trace, err := os.OpenFile(tracePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600) // #nosec G304 -- validated local trace path
	if err != nil {
		return nil, err
	}
	return &summaryTraceWriter{output: output, trace: trace}, nil
}

func (w *summaryTraceWriter) Write(p []byte) (int, error) {
	n, err := w.output.Write(p)
	if err != nil {
		return n, err
	}
	if n != len(p) {
		return n, io.ErrShortWrite
	}
	if w.traceErr == nil {
		traceN, traceErr := w.trace.Write(p)
		if traceErr != nil {
			w.traceErr = traceErr
		} else if traceN != len(p) {
			w.traceErr = io.ErrShortWrite
		}
	}
	return len(p), nil
}

func (w *summaryTraceWriter) Flush() {
	if flusher, ok := w.output.(interface{ Flush() }); ok {
		flusher.Flush()
	}
	if w.traceErr == nil {
		w.traceErr = w.trace.Sync()
	}
}

func (w *summaryTraceWriter) Close() error {
	w.Flush()
	return w.trace.Close()
}

func (w *summaryTraceWriter) Err() error {
	return w.traceErr
}

func usesCompactSummaryOutput(out io.Writer) bool {
	switch typed := out.(type) {
	case *summaryMessageWriter:
		return true
	case *summaryTraceWriter:
		return usesCompactSummaryOutput(typed.output)
	default:
		return false
	}
}

// generateRunSummary loads run artifacts from runDir, produces a summary report file,
// and writes a console-friendly snippet to out. Non-fatal errors are logged and
// returned so callers can surface warnings without aborting the workflow.
func generateRunSummary(ctx context.Context, runDir string, out io.Writer) error {
	loader := summary.NewLoader()
	runData, err := loader.Load(ctx, runDir)
	if err != nil {
		return fmt.Errorf("load results: %w", err)
	}
	agg, err := summary.Aggregate(runData)
	if err != nil {
		return fmt.Errorf("aggregate summary: %w", err)
	}
	fullRenderer := summary.NewRenderer(summary.RenderOptions{MaxWidth: 110})
	fullReport := fullRenderer.FullReport(agg)
	var snippet string
	if usesCompactSummaryOutput(out) {
		compactRenderer := summary.NewRenderer(summary.RenderOptions{MaxWidth: 72})
		snippet = compactRenderer.CompactSnippet(agg)
	} else {
		snippetRenderer := summary.NewRenderer(summary.RenderOptions{MaxWidth: 100})
		snippet = snippetRenderer.ConsoleSnippet(agg)
	}
	if out != nil {
		if _, writeErr := io.WriteString(out, snippet); writeErr != nil {
			logging.LogError("auto-results", "write summary snippet", writeErr)
		}
	}
	fileName := fmt.Sprintf(constants.ResultsSummaryFilePattern, agg.RunID)
	filePath := filepath.Join(runDir, fileName)
	if err := writeAtomic(filePath, []byte(fullReport), 0o644); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	if out != nil {
		if _, writeErr := fmt.Fprintf(out, "Auto-results: summary saved to %s\n", filePath); writeErr != nil {
			logging.LogError("auto-results", "write summary path", writeErr)
		}
	}
	if flusher, ok := out.(interface{ Flush() }); ok {
		flusher.Flush()
	}
	return nil
}

func writeAtomic(path string, data []byte, perm os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".summary-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("write temp file: %w", err)
	}
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("chmod temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close temp file: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename temp file: %w", err)
	}
	return nil
}
