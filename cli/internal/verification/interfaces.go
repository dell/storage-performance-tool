/*
Copyright © 2025 Dell Technologies
*/

package verification

import (
	"context"
	"net/http"
	"time"
)

// NetworkChecker defines the interface for network connectivity testing
type NetworkChecker interface {
	// IsPortOpen tests if a port is accessible on the given host
	IsPortOpen(host string, port int, timeout time.Duration) bool

	// HTTPGet performs an HTTP GET request and returns the response
	HTTPGet(ctx context.Context, url string) (*http.Response, error)
}

// TimeProvider defines the interface for time-related operations (for deterministic testing)
type TimeProvider interface {
	// Now returns the current time
	Now() time.Time

	// Since returns the duration since the given time
	Since(t time.Time) time.Duration
}

// Real implementations are defined in implementations.go
