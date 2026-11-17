// Package repair 负责分片缺失/损坏后的后台重建，支持限速、暂停与手动触发。
package repair

import (
	"context"
	"sync"
	"time"
)

// Limiter 令牌桶限速器：按固定速率补充字节配额。
// rate<=0 表示不限速。
type Limiter struct {
	mu       sync.Mutex
	rate     int64 // 字节/秒
	tokens   int64
	maxBurst int64
	last     time.Time
}

// NewLimiter 创建限速器。
func NewLimiter(rate int64) *Limiter {
	l := &Limiter{rate: rate, maxBurst: max64(rate, 1<<20), last: time.Now()}
	l.tokens = l.maxBurst
	return l
}

// SetRate 动态调整速率（0=不限速）。
func (l *Limiter) SetRate(rate int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rate = rate
	if rate > 0 {
		l.maxBurst = max64(rate, 1<<20)
	}
}

// Rate 当前速率。
func (l *Limiter) Rate() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rate
}

// Wait 消费 n 字节配额，必要时阻塞。
func (l *Limiter) Wait(n int) error {
	for {
		l.mu.Lock()
		if l.rate <= 0 {
			l.mu.Unlock()
			return nil
		}
		now := time.Now()
		elapsed := now.Sub(l.last).Seconds()
		l.tokens = min64(l.maxBurst, l.tokens+int64(elapsed*float64(l.rate)))
		l.last = now
		if l.tokens >= int64(n) {
			l.tokens -= int64(n)
			l.mu.Unlock()
			return nil
		}
		need := int64(n) - l.tokens
		wait := time.Duration(float64(need) / float64(l.rate) * float64(time.Second))
		l.mu.Unlock()
		if wait > 100*time.Millisecond {
			wait = 100 * time.Millisecond
		}
		select {
		case <-time.After(wait):
		case <-context.Background().Done():
			return context.Canceled
		}
	}
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
