package repair_test

import (
	"testing"
	"time"

	"erasurestore/internal/repair"
)

func TestLimiterThrottles(t *testing.T) {
	// 1 MiB/s，消费 2 MiB 应至少耗时约 1 秒（扣除初始突发后仍明显受限速）
	l := repair.NewLimiter(1 << 20)
	start := time.Now()
	for i := 0; i < 8; i++ {
		if err := l.Wait(256 * 1024); err != nil {
			t.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	if elapsed < 700*time.Millisecond {
		t.Fatalf("限速未生效：消费 2MiB 仅耗时 %v", elapsed)
	}
}

func TestLimiterUnlimited(t *testing.T) {
	l := repair.NewLimiter(0)
	start := time.Now()
	for i := 0; i < 100; i++ {
		if err := l.Wait(64 * 1024); err != nil {
			t.Fatal(err)
		}
	}
	if time.Since(start) > 200*time.Millisecond {
		t.Fatal("rate=0 应当不限速")
	}
}

func TestLimiterDynamicRate(t *testing.T) {
	l := repair.NewLimiter(1 << 20)
	l.SetRate(0)
	if l.Rate() != 0 {
		t.Fatal("动态调速失败")
	}
	start := time.Now()
	_ = l.Wait(4 << 20)
	if time.Since(start) > 200*time.Millisecond {
		t.Fatal("调速为 0 后应当不再限速")
	}
}
