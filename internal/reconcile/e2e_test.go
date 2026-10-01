package reconcile

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"stability-service/internal/mockbiz"
	"stability-service/internal/mockprov"
)

// 快速配置：服务商 10ms 内发出、10ms 内可查、无时钟偏差。
func fastProv(cfg mockprov.Config) *mockprov.Server {
	cfg.MaxDecideMS = 10
	cfg.MaxVisibleMS = 10
	cfg.SkewMS = 1
	cfg.DropRate = 0
	return mockprov.New(cfg)
}

// 测试 4（端到端）：提交 → 增量同步 → 状态闭合 → 对账一致。
// 业务系统被配置为【不自己推进】，闭合完全由我们的 Syncer 通过 resolve 完成。
func TestEndToEnd_SubmitSyncResolveReconcile(t *testing.T) {
	ctx := context.Background()

	prov := fastProv(mockprov.Config{})
	provURL := httptest.NewServer(prov.Handler())
	defer provURL.Close()

	// Advance 时延设为极大：biz 永远不把 accepted 推进到终态，
	// 于是「记录被闭合为 sent」这件事只能是我们的 Syncer 干的。
	biz := mockbiz.New(mockbiz.Config{
		ProviderURL:  provURL.URL,
		Senders:      2,
		SeedRecords:  0,
		AdvanceMinMS: 1 << 30,
		AdvanceMaxMS: 1 << 30,
	})
	bizURL := httptest.NewServer(biz.Handler())
	defer bizURL.Close()

	store := NewMemStore()
	syncer := NewSyncer(NewBizClient(bizURL.URL), NewProviderClient(provURL.URL), store)
	syncer.Horizon = 200 * time.Millisecond     // 测试加速：200ms 后可判定
	syncer.ScanMinAge = 25 * time.Millisecond   // 匹配 mock 的物理发出时延（10ms+偏差）

	// 提交两条；之后把服务商调成「全部放弃」，再提交一条 → 应闭合为 failed。
	biz.Submit("sender-0", "a@example.com")
	biz.Submit("sender-0", "b@example.com")
	prov.SetDropRate(1)
	biz.Submit("sender-0", "c@example.com")

	// 轮询同步，直到 3 条记录全部闭合。
	deadline := time.Now().Add(15 * time.Second)
	var recs []Record
	for {
		if err := syncer.SyncOnce(ctx); err != nil {
			t.Fatalf("SyncOnce: %v", err)
		}
		recs, _ = store.RecordsOf(ctx, "sender-0",
			time.Now().Add(-time.Minute), time.Now().Add(time.Minute))
		closed := 0
		for _, r := range recs {
			if r.Status == StatusSent || r.Status == StatusFailed {
				closed++
			}
		}
		if closed == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("records not closed within 15s: %+v", recs)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// 2 条 sent（带 providerMsgId），1 条 failed（无消息）。
	sent, failed := 0, 0
	for _, r := range recs {
		switch r.Status {
		case StatusSent:
			sent++
			if r.ProviderMsgID == "" {
				t.Fatalf("sent record without providerMsgId: %+v", r)
			}
		case StatusFailed:
			failed++
		}
	}
	if sent != 2 || failed != 1 {
		t.Fatalf("sent=%d failed=%d, want 2/1; recs=%+v", sent, failed, recs)
	}

	// 对账：记录与服务商消息匹配 → consistent（无采集缺口）。
	msgs, err := store.MessagesOf(ctx, "sender-0",
		time.Now().Add(-time.Minute), time.Now().Add(time.Minute))
	if err != nil || len(msgs) != 2 {
		t.Fatalf("messages = %d, err=%v, want 2", len(msgs), err)
	}
	result := ComputeState(false, Match(recs, msgs))
	if result.State != StateConsistent {
		t.Fatalf("state = %s, want consistent; issues=%+v", result.State, result.Issues)
	}

	// /api/reconciliation 走 HTTP 全链路（窗口右端点已过可见性时延 → 不 incomplete）。
	form := url.Values{"senderId": {"sender-0"},
		"from": {time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)},
		"to":   {time.Now().Add(-time.Millisecond * 300).UTC().Format(time.RFC3339Nano)}}
	req := httptest.NewRequest("GET", "/api/reconciliation?"+form.Encode(), nil)
	rec := httptest.NewRecorder()
	syncer.ServeReconciliation(rec, req)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var httpResult Result
	json.Unmarshal(rec.Body.Bytes(), &httpResult)
	if httpResult.State == StateIncomplete {
		t.Fatalf("unexpected incomplete: %+v", httpResult)
	}
}

// 测试 5（端到端）：故障注入 —— 503 → incomplete；100 条静默截断被自适应窗口击穿；429 自动重试。
func TestEndToEnd_FaultInjection(t *testing.T) {
	ctx := context.Background()

	// 1) 服务商每次请求都 503 → 对账只能回答 incomplete（不能谎报 consistent）。
	downProv := fastProv(mockprov.Config{Fail503Every: 1})
	downURL := httptest.NewServer(downProv.Handler())
	defer downURL.Close()

	biz := mockbiz.New(mockbiz.Config{ProviderURL: downURL.URL, Senders: 1, SeedRecords: 0})
	bizURL := httptest.NewServer(biz.Handler())
	defer bizURL.Close()

	syncer := NewSyncer(NewBizClient(bizURL.URL), NewProviderClient(downURL.URL), NewMemStore())
	q := url.Values{"senderId": {"sender-0"},
		"from": {time.Now().Add(-2 * time.Hour).UTC().Format(time.RFC3339)}, // 窗口在可见性时延之前
		"to":   {time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339)}}
	rec := httptest.NewRecorder()
	syncer.ServeReconciliation(rec, httptest.NewRequest("GET", "/api/reconciliation?"+q.Encode(), nil))
	var result Result
	json.Unmarshal(rec.Body.Bytes(), &result)
	if result.State != StateIncomplete {
		t.Fatalf("state = %s, want incomplete under provider 503", result.State)
	}

	// 2) 静默截断：同一毫秒涌入 150 条消息（单页上限 100），自适应窗口必须全量取回。
	prov := fastProv(mockprov.Config{})
	provURL := httptest.NewServer(prov.Handler())
	defer provURL.Close()
	biz2 := mockbiz.New(mockbiz.Config{ProviderURL: provURL.URL, Senders: 1, SeedRecords: 0})
	for i := 0; i < 150; i++ {
		biz2.Submit("sender-0", fmt.Sprintf("burst-%d@example.com", i))
	}
	time.Sleep(50 * time.Millisecond) // 等全部发出并可见

	pc := NewProviderClient(provURL.URL)
	msgs, err := pc.FetchWindow(ctx, "sender-0",
		time.Now().Add(-time.Minute), time.Now().Add(time.Second))
	if err != nil {
		t.Fatalf("FetchWindow: %v", err)
	}
	if len(msgs) != 150 {
		t.Fatalf("adaptive window lost messages: got %d, want 150", len(msgs))
	}

	// 3) 429：每 2 次请求限流一次，客户端必须按 retryAfter 退避后成功。
	prov429 := fastProv(mockprov.Config{Fail429Every: 2})
	prov429URL := httptest.NewServer(prov429.Handler())
	defer prov429URL.Close()
	biz3 := mockbiz.New(mockbiz.Config{ProviderURL: prov429URL.URL, Senders: 1, SeedRecords: 0})
	biz3.Submit("sender-0", "x@example.com")
	time.Sleep(50 * time.Millisecond)
	pc429 := NewProviderClient(prov429URL.URL)
	got, err := pc429.FetchWindow(ctx, "sender-0",
		time.Now().Add(-time.Minute), time.Now().Add(time.Second))
	if err != nil {
		t.Fatalf("FetchWindow with 429: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("after 429 retry: got %d messages, want 1", len(got))
	}
}
