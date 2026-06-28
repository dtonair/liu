package engine

import "time"

// Clock abstracts time so the engine's retry backoff and timers can be tested
// deterministically with a fake clock.
type Clock interface {
	Now() time.Time
}

// SystemClock is the production Clock backed by the OS clock (UTC).
type SystemClock struct{}

// Now returns the current UTC time.
func (SystemClock) Now() time.Time { return time.Now().UTC() }

// FakeClock is a manually-advanced Clock for tests.
type FakeClock struct{ t time.Time }

// NewFakeClock returns a FakeClock anchored at t.
func NewFakeClock(t time.Time) *FakeClock { return &FakeClock{t: t.UTC()} }

// Now returns the fake clock's current time.
func (f *FakeClock) Now() time.Time { return f.t }

// Advance moves the fake clock forward by d.
func (f *FakeClock) Advance(d time.Duration) { f.t = f.t.Add(d) }
