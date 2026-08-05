/*
Copyright © 2026 Dell Technologies
*/

package tui

import (
	"context"
	"time"
)

func normalizeContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// boundedDetachedContext preserves context values while giving rollback and
// mandatory cleanup an independent deadline after normal work is canceled.
func boundedDetachedContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	parent = normalizeContext(parent)
	if timeout <= 0 {
		return context.WithCancel(context.WithoutCancel(parent))
	}
	return context.WithTimeout(context.WithoutCancel(parent), timeout)
}
