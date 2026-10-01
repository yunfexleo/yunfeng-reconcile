package alert

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// 测试 3：webhook 告警 —— 429 限流自动重试、同 Key 去抖、静默期抑制。
func TestAlerter_429RetryDedupAndSilence(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		io.Copy(io.Discard, r.Body)
		if n == 1 {
			// 第一次调用模拟限流：要求 0 秒后重试。
			w.WriteHeader(http.StatusTooManyRequests)
			json.NewEncoder(w).Encode(map[string]int{"retryAfterSeconds": 0})
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	a := New(srv.URL)
	ctx := context.Background()

	// 1) 首次发送遇到 429 → 按 retryAfter 重试 → 最终送达（共 2 次 HTTP 调用）。
	if err := a.Send(ctx, Event{Key: "biz-down", Title: "业务系统不可用", Severity: Critical}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("calls = %d, want 2 (one 429 + one success)", got)
	}

	// 2) 同一 Key 在去抖窗口内再次发送 → 不再产生 HTTP 调用。
	if err := a.Send(ctx, Event{Key: "biz-down", Title: "业务系统不可用", Severity: Critical}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("dedup failed: calls = %d, want 2", got)
	}

	// 3) 静默期内一切告警被抑制。
	a.SilenceFor(time.Hour)
	if err := a.Send(ctx, Event{Key: "new-alert", Title: "发送方异常", Severity: Warning}); err != nil {
		t.Fatalf("Send during silence: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("silence failed: calls = %d, want 2", got)
	}

	// 4) 静默结束 + Resolve 解除激活 → 告警恢复投递。
	a.SilenceFor(-time.Hour) // 立即解除静默
	a.Resolve("biz-down")
	if err := a.Send(ctx, Event{Key: "biz-down", Title: "业务系统不可用", Severity: Critical}); err != nil {
		t.Fatalf("Send after resolve: %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("calls = %d, want 3 after resolve", got)
	}
}
