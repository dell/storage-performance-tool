/*
Copyright © 2025 Dell Technologies
*/

package verification

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"
)

// RealNetworkChecker implements NetworkChecker using actual network operations
type RealNetworkChecker struct{}

// IsPortOpen reports whether a TCP connection to host:port can be
// established within the provided timeout.
func (r *RealNetworkChecker) IsPortOpen(host string, port int, timeout time.Duration) bool {
	address := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	conn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		return false
	}
	defer func() { _ = conn.Close() }()
	return true
}

// HTTPGet performs an HTTP GET with context and a reasonable client timeout.
func (r *RealNetworkChecker) HTTPGet(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	return client.Do(req)
}

// RealTimeProvider implements TimeProvider using actual time functions
type RealTimeProvider struct{}

// Now returns the current time.
func (r *RealTimeProvider) Now() time.Time {
	return time.Now()
}

// Since returns the elapsed duration since t.
func (r *RealTimeProvider) Since(t time.Time) time.Duration {
	return time.Since(t)
}
