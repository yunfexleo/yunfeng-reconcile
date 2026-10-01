package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// Severity 是告警级别（2.3 节）。
type Severity string

const (
	Info     Severity = "info"
	Warning  Severity = "warning"
	Critical Severity = "critical"
)

// Event 是一条告警。Key 用于去抖：同一 Key 在激活期内不重复发送，
// 故障恢复后由调用方 Resolve 解除。Key 为空表示每次都发（不复用）。
type Event struct {
	Key      string
	Title    string
	Text     string
	Severity Severity
}

const (
	maxAttempts  = 3                // 单次发送的最大尝试次数（含重试）
	activeTTL    = 15 * time.Minute // 同 Key 告警的去抖窗口
	maxRetryWait = 30 * time.Second // 429 retryAfter 的上限，避免被恶意/异常值拖死
)

// Alerter 向告警 webhook 发送告警。
// 处理三种不可靠性：429 限流（按 retryAfter 退避重试）、5xx/超时（指数退避重试）、
// 值班可临时静默。进程重启后去抖状态丢失是已知取舍：重启期间同类告警会重发一次，可接受。
type Alerter struct {
	endpoint string
	client   *http.Client
	now      func() time.Time

	mu            sync.Mutex
	active        map[string]time.Time // key -> 最近一次发送时间
	silencedUntil time.Time
}

func New(endpoint string) *Alerter {
	return &Alerter{
		endpoint: endpoint,
		client:   &http.Client{Timeout: 5 * time.Second},
		now:      time.Now,
		active:   map[string]time.Time{},
	}
}

// SilenceFor 静默未来 d 时长内的所有告警（运维临时止血用）。
func (a *Alerter) SilenceFor(d time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.silencedUntil = a.now().Add(d)
}

// SilenceRemaining 返回静默期剩余时长；未在静默期返回 0。
func (a *Alerter) SilenceRemaining() time.Duration {
	a.mu.Lock()
	defer a.mu.Unlock()
	if rem := time.Until(a.silencedUntil); rem > 0 {
		return rem
	}
	return 0
}

// Resolve 解除某 Key 的激活状态：故障恢复后同 Key 的告警可以再次发送。
func (a *Alerter) Resolve(key string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.active, key)
}

func (a *Alerter) suppressed(key string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.now().Before(a.silencedUntil) {
		return true
	}
	if key == "" {
		return false
	}
	if last, ok := a.active[key]; ok && a.now().Sub(last) < activeTTL {
		return true // 同 Key 仍在激活期，去抖
	}
	a.active[key] = a.now()
	return false
}

// Send 发送一条告警，429/5xx/超时自动退避重试（最多 3 次）。
// 静默期或去抖命中时直接丢弃并返回 nil；重试耗尽返回错误。
func (a *Alerter) Send(ctx context.Context, ev Event) error {
	if a.suppressed(ev.Key) {
		return nil
	}
	payload, err := json.Marshal(map[string]string{
		"title":    ev.Title,
		"text":     ev.Text,
		"severity": string(ev.Severity),
	})
	if err != nil {
		return err
	}

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(1<<uint(attempt-1)) * 500 * time.Millisecond // 500ms, 1s
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
		retryAfter, err := a.post(ctx, payload)
		if err == nil {
			return nil
		}
		lastErr = err
		if retryAfter > 0 {
			if retryAfter > maxRetryWait {
				retryAfter = maxRetryWait
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retryAfter):
			}
		}
	}
	// 全部失败：解除去抖标记，让后续调度可以重发（避免故障期后永远沉默）。
	a.Resolve(ev.Key)
	return fmt.Errorf("alert send failed after %d attempts: %w", maxAttempts, lastErr)
}

// post 执行一次请求；返回 (retryAfter, error)。
// 200 返回 nil error；429 返回服务端要求的等待时长；5xx/网络错误作为可重试错误。
func (a *Alerter) post(ctx context.Context, payload []byte) (time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.endpoint, bytes.NewReader(payload))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.client.Do(req)
	if err != nil {
		return 0, err // 超时/连接失败：交给退避重试
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	if resp.StatusCode == http.StatusOK {
		return 0, nil
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		var rl struct {
			RetryAfterSeconds int `json:"retryAfterSeconds"`
		}
		if json.Unmarshal(body, &rl) == nil && rl.RetryAfterSeconds > 0 {
			return time.Duration(rl.RetryAfterSeconds) * time.Second, fmt.Errorf("webhook rate limited")
		}
		return 0, fmt.Errorf("webhook rate limited")
	}
	return 0, fmt.Errorf("webhook status %d", resp.StatusCode)
}
