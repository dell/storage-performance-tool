/*
Copyright © 2025 Dell Technologies
*/
//revive:disable:package-comments
// Package constants defines shared constants used across the application.
package constants

// Canonical Spt run states (shared project-wide).
const (
	StateIdle         = "IDLE"
	StateRunning      = "RUNNING"
	StateStarting     = "STARTING"
	StateInitializing = "INITIALIZING"
	StateCompleted    = "COMPLETED"
	StateFailed       = "FAILED"
	StateStopped      = "STOPPED"
)

// UI-only states (not emitted by Spt, used for client UX)
const (
	UIStateDetected = "DETECTED"
	UIStateUnknown  = "UNKNOWN"
)
