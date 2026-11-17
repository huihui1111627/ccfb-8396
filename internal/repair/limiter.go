package repair

import (
	"context"
	"io"
	"sync"
	"time"
)

// Limiter throttles repair IO so a rebuild cannot saturate the disks that
// foreground reads and writes share. It combines:
//
//   - a token bucket capped at bytesPerSecond across all rebuilds, and
//   - an optional yield check that blocks while foreground IO is in flight.
type Limiter struct {
	mu     sync.Mutex
	bps    int64
	tokens float64
	last   time.Time
	yield  func() bool // returns true while foreground load is active
	now    func() time.Time
}

// NewLimiter builds a limiter. bps <= 0 disables rate limiting; yield may be
// nil to disable foreground-aware pausing.
func NewLimiter(bps int64, yield func() bool) *Limiter {
	return &Limiter{
		bps:    bps,
		tokens: float64(bps),
		last:   time.Now(),
		yield:  yield,
		now:    time.Now,
	}
}

// SetRate changes the byte-per-second budget at runtime (console tuning).
func (l *Limiter) SetRate(bps int64) {
	l.mu.Lock()
	l.bps = bps
	l.mu.Unlock()
}

// Rate returns the current budget.
func (l *Limiter) Rate() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.bps
}

// Wait blocks until n bytes are budgeted and any foreground burst settles.
func (l *Limiter) Wait(ctx context.Context, n int) error {
	// First, let foreground traffic take priority.
	if l.yield != nil {
		for l.yield() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
	l.mu.Lock()
	bps := l.bps
	l.mu.Unlock()
	if bps <= 0 {
		return nil // unlimited
	}
	for {
		l.mu.Lock()
		now := l.now()
		elapsed := now.Sub(l.last).Seconds()
		l.tokens += elapsed * float64(bps)
		if max := float64(bps); l.tokens > max {
			l.tokens = max
		}
		l.last = now
		if l.tokens >= float64(n) {
			l.tokens -= float64(n)
			l.mu.Unlock()
			return nil
		}
		need := float64(n) - l.tokens
		l.mu.Unlock()
		wait := time.Duration(need / float64(bps) * float64(time.Second))
		if wait < 5*time.Millisecond {
			wait = 5 * time.Millisecond
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

// throttledReader wraps a source reader, acquiring limiter budget per read.
type throttledReader struct {
	r   io.Reader
	lim *Limiter
	ctx context.Context
}

func (t *throttledReader) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if n > 0 {
		if werr := t.lim.Wait(t.ctx, n); werr != nil {
			return n, werr
		}
	}
	return n, err
}
