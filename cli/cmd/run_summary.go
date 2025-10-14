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
	if _, isBuffered := out.(*summaryMessageWriter); isBuffered {
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
