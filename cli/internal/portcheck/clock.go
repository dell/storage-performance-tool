package portcheck

import "time"

// Clock abstracts time-related operations to enable fast tests.
type Clock interface {
	Now() time.Time
	Sleep(d time.Duration)
	After(d time.Duration) <-chan time.Time
}

// RealClock uses the real wall clock backed by the time package.
type RealClock struct{}

// Now returns the current time from the system clock.
func (r *RealClock) Now() time.Time { return time.Now() }

// Sleep sleeps for the given duration using time.Sleep.
func (r *RealClock) Sleep(d time.Duration) { time.Sleep(d) }

// After delegates to time.After to deliver a timer channel.
func (r *RealClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// FakeClock is a testing clock that unblocks waits immediately.
// It is useful for tests where real sleeping is undesirable.
type FakeClock struct {
	now time.Time
}

// NewFakeClock returns a FakeClock initialized to the Unix epoch.
func NewFakeClock() *FakeClock { return &FakeClock{now: time.Unix(0, 0)} }

// Now returns the fixed time stored in the fake clock.
func (f *FakeClock) Now() time.Time { return f.now }

// Sleep is a no-op in FakeClock
func (f *FakeClock) Sleep(_ time.Duration) {}

// After returns an already-fired channel to unblock waits immediately
func (f *FakeClock) After(_ time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	ch <- f.now
	return ch
}
