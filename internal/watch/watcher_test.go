package watch

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"stability-service/internal/alert"
)

// 测试 6：巡检引擎 —— flap 防抖、恢复通知、静默抑制。
func TestWatcher_FlapDetectionRecoveryAndSilence(t *testing.T) {
	ctx := context.Background()

	// 业务系统：mode=0 时全部 500（故障），mode=1 时全部正常。
	var mode atomic.Int32
	biz := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if mode.Load() == 0 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/internal/health":
			json.NewEncoder(w).Encode(map[string]any{"version": "mock", "db": "ok", "provider": "ok"})
		case "/internal/stats":
			json.NewEncoder(w).Encode(map[string]any{"pendingCount": 0, "oldestPendingAgeSeconds": 0})
		case "/internal/senders":
			json.NewEncoder(w).Encode(map[string]any{"senders": []any{}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer biz.Close()

	var alertCalls atomic.Int32
	wh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		alertCalls.Add(1)
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer wh.Close()

	a := alert.New(wh.URL)
	w := New(biz.URL, a, Config{FailThreshold: 3, RecoverThreshold: 2})

	// 故障期：失败 2 次不告警（未达阈值），第 3 次才告警。
	w.CheckOnce(ctx)
	w.CheckOnce(ctx)
	if n := alertCalls.Load(); n != 0 {
		t.Fatalf("alerted after only 2 failures: calls=%d", n)
	}
	w.CheckOnce(ctx)
	if n := alertCalls.Load(); n != 1 {
		t.Fatalf("no alert at 3rd consecutive failure: calls=%d", n)
	}

	// 短暂恢复 1 次（模拟重启间隙端口可用）→ 不得发送恢复通知（flap 防抖）。
	mode.Store(1)
	w.CheckOnce(ctx)
	if n := alertCalls.Load(); n != 1 {
		t.Fatalf("recovery announced too early (single success is a flap): calls=%d", n)
	}
	// 连续恢复第 2 次 → 发送恢复通知。
	w.CheckOnce(ctx)
	if n := alertCalls.Load(); n != 2 {
		t.Fatalf("no recovery notice after 2 consecutive successes: calls=%d", n)
	}

	// 静默期：业务系统再次故障，但告警被抑制。
	a.SilenceFor(time.Hour)
	mode.Store(0)
	w.CheckOnce(ctx)
	w.CheckOnce(ctx)
	w.CheckOnce(ctx)
	if n := alertCalls.Load(); n != 2 {
		t.Fatalf("silence failed: calls=%d, want 2", n)
	}
}

// 测试 7：发送方异常分级与节点聚合。
func TestWatcher_SenderAnomaliesAndNodeAggregation(t *testing.T) {
	ctx := context.Background()

	biz := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/internal/health":
			json.NewEncoder(w).Encode(map[string]any{"db": "ok", "provider": "ok"})
		case "/internal/stats":
			json.NewEncoder(w).Encode(map[string]any{"pendingCount": 0, "oldestPendingAgeSeconds": 0})
		case "/internal/senders":
			// node-0 上 6 个 disconnected（≥阈值 5 → 应聚合成一条节点级告警）；
			// 另有一个 throttled（warning）和一个 suspended（critical）。
			var senders []map[string]any
			for i := 0; i < 6; i++ {
				senders = append(senders, map[string]any{
					"senderId": "n0-s" + string(rune('0'+i)), "status": "disconnected", "nodeId": "node-0"})
			}
			senders = append(senders,
				map[string]any{"senderId": "s-throttled", "status": "throttled", "nodeId": "node-1"},
				map[string]any{"senderId": "s-suspended", "status": "suspended", "nodeId": "node-2"})
			json.NewEncoder(w).Encode(map[string]any{"senders": senders})
		default:
			http.NotFound(w, r)
		}
	}))
	defer biz.Close()

	var titles []string
	var mu sync.Mutex
	wh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Title string `json:"title"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		titles = append(titles, body.Title)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer wh.Close()

	w := New(biz.URL, alert.New(wh.URL), Config{FailThreshold: 1, NodeDownMinSenders: 5})
	w.CheckOnce(ctx)

	mu.Lock()
	defer mu.Unlock()
	if len(titles) != 3 { // 1 节点级 + throttled + suspended，不是 8 条
		t.Fatalf("alerts = %v, want exactly 3 aggregated", titles)
	}
	var node, throttled, suspended bool
	for _, title := range titles {
		node = node || title == "发送节点失联（大量发送方断开）"
		throttled = throttled || title == "发送方被限流（通常可自行恢复）"
		suspended = suspended || title == "发送方通道异常（suspended/revoked，不可自行恢复）"
	}
	if !node || !throttled || !suspended {
		t.Fatalf("missing expected alerts: %v", titles)
	}
}
