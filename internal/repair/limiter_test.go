package repair_test

import (
	"context"
	"testing"
	"time"

	"imstore/internal/repair"
)

func TestLimiterThrottlesToRate(t *testing.T) {
	// 1 MiB/s budget; consuming 4 MiB should take roughly 3 seconds.
	const bps int64 = 1 << 20
	l := repair.NewLimiter(bps, nil)
	ctx := context.Background()

	start := time.Now()
	for i := 0; i < 4; i++ {
		if err := l.Wait(ctx, 1<<20); err != nil {
			t.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	if elapsed < 2500*time.Millisecond {
		t.Fatalf("4 MiB at 1 MiB/s took %v, expected ~3s (throttle not enforced)", elapsed)
	}
	t.Logf("throttled 4 MiB in %v at %d B/s", elapsed, bps)
}

func TestLimiterUnlimitedWhenZero(t *testing.T) {
	l := repair.NewLimiter(0, nil)
	ctx := context.Background()
	start := time.Now()
	for i := 0; i < 10; i++ {
		if err := l.Wait(ctx, 1<<20); err != nil {
			t.Fatal(err)
		}
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("zero rate should mean unlimited")
	}
}

func TestLimiterYieldsToForegroundLoad(t *testing.T) {
	busy := true
	// Yield function reports foreground activity until released.
	l := repair.NewLimiter(0, func() bool { return busy })
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- l.Wait(ctx, 1) }()

	select {
	case <-done:
		t.Fatal("Wait returned while foreground load was active")
	case <-time.After(100 * time.Millisecond):
	}

	busy = false // foreground traffic settles
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Wait after yield release: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Wait did not return after foreground load cleared")
	}
}

func TestLimiterRuntimeRateChange(t *testing.T) {
	l := repair.NewLimiter(1<<20, nil)
	if l.Rate() != 1<<20 {
		t.Fatalf("initial rate = %d", l.Rate())
	}
	l.SetRate(0) // unlimited
	if l.Rate() != 0 {
		t.Fatalf("rate after change = %d", l.Rate())
	}
}
