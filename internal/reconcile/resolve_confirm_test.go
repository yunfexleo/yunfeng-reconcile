package reconcile

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"stability-service/internal/mockbiz"
	"stability-service/internal/mockprov"
)

// 测试 8：判 failed 前的 live 复核 —— 防止「扫描缺口 + 判定」叠加把已发出的记录误改。
func TestEndToEnd_ResolveFailedRequiresLiveConfirm(t *testing.T) {
	ctx := context.Background()

	// 场景：服务商有消息，但本地存储因采集缺口（这里直接不扫）没有 —
	// 旧实现会把记录误判为 failed；正确行为是复核后发现消息，闭合为 sent。
	prov := fastProv(mockprov.Config{})
	provURL := httptest.NewServer(prov.Handler())
	defer provURL.Close()

	biz := mockbiz.New(mockbiz.Config{
		ProviderURL: provURL.URL, Senders: 1, SeedRecords: 0,
		AdvanceMinMS: 1 << 30, AdvanceMaxMS: 1 << 30, // biz 不自己推进
	})
	bizURL := httptest.NewServer(biz.Handler())
	defer bizURL.Close()

	biz.Submit("sender-0", "gap@example.com")
	time.Sleep(50 * time.Millisecond) // 等服务商发出并可见

	// 记录直接入存储（不经过扫描 —— 模拟"存储缺消息"的采集缺口）。
	recs, err := NewBizClient(bizURL.URL).Records(ctx, time.Time{}, 10)
	if err != nil || len(recs) != 1 {
		t.Fatalf("fetch records: n=%d err=%v", len(recs), err)
	}

	store := NewMemStore()
	store.UpsertRecord(ctx, recs[0])
	syncer := NewSyncer(NewBizClient(bizURL.URL), NewProviderClient(provURL.URL), store)
	syncer.Horizon = 10 * time.Millisecond // 提交即越过判定时延，直接进入判定分支

	if err := syncer.ResolveOnce(ctx); err != nil {
		t.Fatalf("ResolveOnce: %v", err)
	}
	got, _ := store.RecordsOf(ctx, "sender-0", time.Time{}, time.Now().Add(time.Minute))
	if len(got) != 1 {
		t.Fatalf("records = %d, want 1", len(got))
	}
	if got[0].Status != StatusSent || got[0].ProviderMsgID == "" {
		t.Fatalf("存储缺消息时不得直接判 failed：got %+v", got[0])
	}
}

// 测试 9：服务商持续 503 时宁可不闭合，也不猜 —— 记录必须保持 accepted。
func TestEndToEnd_ResolveBlockedWhenProviderDown(t *testing.T) {
	ctx := context.Background()

	downProv := fastProv(mockprov.Config{Fail503Every: 1}) // 每次请求都 503
	downURL := httptest.NewServer(downProv.Handler())
	defer downURL.Close()

	biz := mockbiz.New(mockbiz.Config{
		ProviderURL: downURL.URL, Senders: 1, SeedRecords: 0,
		AdvanceMinMS: 1 << 30, AdvanceMaxMS: 1 << 30,
	})
	bizURL := httptest.NewServer(biz.Handler())
	defer bizURL.Close()

	biz.Submit("sender-0", "down@example.com")
	time.Sleep(50 * time.Millisecond)
	recs, _ := NewBizClient(bizURL.URL).Records(ctx, time.Time{}, 10)

	store := NewMemStore()
	store.UpsertRecord(ctx, recs[0])
	syncer := NewSyncer(NewBizClient(bizURL.URL), NewProviderClient(downURL.URL), store)
	syncer.Horizon = 10 * time.Millisecond // 已越过判定时延，但服务商 503：宁可不闭合

	if err := syncer.ResolveOnce(ctx); err != nil {
		t.Fatalf("ResolveOnce: %v", err)
	}
	got, _ := store.RecordsOf(ctx, "sender-0", time.Time{}, time.Now().Add(time.Minute))
	if got[0].Status != StatusAccepted {
		t.Fatalf("服务商不可用时不得闭合记录：got %s", got[0].Status)
	}
}
