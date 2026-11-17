// Package logging provides utility functions for consistent logging throughout spt.
package logging

import (
	"io"
	"log/slog"
	"sync/atomic"
)

// logger holds the application-wide logger instance. It is atomic so that
// SetLogger may run while other goroutines log (tests swap it often).
var logger atomic.Pointer[slog.Logger]

// SetLogger sets the global logger instance. A nil logger restores the
// no-op default.
func SetLogger(l *slog.Logger) {
	logger.Store(l)
}

// GetLogger returns the configured logger instance.
// If the logger hasn't been initialized, it returns a no-op logger.
func GetLogger() *slog.Logger {
	if l := logger.Load(); l != nil {
		return l
	}
	// Return a no-op logger (discard output) if not initialized
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// WithComponent adds a component field to the logger for context.
func WithComponent(component string) *slog.Logger {
	return GetLogger().With("component", component)
}

// LogContainerEvent logs Docker container lifecycle events.
func LogContainerEvent(event string, containerID string, details ...any) {
	logger := WithComponent("docker")
	allDetails := append([]any{"container_id", containerID, "event", event}, details...)
	logger.Info("container event", allDetails...)
}

// LogMetricsParsing logs metrics parsing events at debug level.
func LogMetricsParsing(message string, details ...any) {
	logger := WithComponent("metrics")
	logger.Debug(message, details...)
}

// LogTUIEvent logs TUI-related events.
func LogTUIEvent(event string, details ...any) {
	logger := WithComponent("tui")
	logger.Debug("TUI event", append([]any{"event", event}, details...)...)
}

// LogError logs error conditions with context.
func LogError(component string, message string, err error, details ...any) {
	logger := WithComponent(component)
	allDetails := append([]any{"error", err}, details...)
	logger.Error(message, allDetails...)
}

// LogInfo logs informational messages with context.
func LogInfo(component string, message string, details ...any) {
	logger := WithComponent(component)
	logger.Info(message, details...)
}

// LogDebug logs debug messages with context.
func LogDebug(component string, message string, details ...any) {
	logger := WithComponent(component)
	logger.Debug(message, details...)
}

// LogWarn logs warning messages with context.
func LogWarn(component string, message string, details ...any) {
	logger := WithComponent(component)
	logger.Warn(message, details...)
}
