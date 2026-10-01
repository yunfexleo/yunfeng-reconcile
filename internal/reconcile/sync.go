package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

// SyncLag 是增量游标相对当前时间的安全滞后。
// 题面：updatedAt 在事务【开始时】取值，事务最晚 30s 后才提交 ——
// 直接把游标推进到"刚查到的最大 updatedAt"会永久漏掉晚提交的记录。
// 滞后 60s（= 30s 最长提交时延 × 2 余量），只把游标推进到滞后线以内。
const SyncLag = 60 * time.Second

// ScanDepth 是每次同步回溯拉取服务商消息的时长。
const ScanDepth = 10 * time.Minute

// ProviderDecideMax 是「提交后最多 30s 内发出 + 5s 时钟偏差」的物理时延上界。
// 扫描只覆盖 sentAt 早于 now-ProviderDecideMax 的消息：
// 更近期的消息可能还没发出或不可见，交给后续重叠轮次补扫（幂等去重）。
const ProviderDecideMax = 35 * time.Second

// Syncer 持续同步业务系统记录 + 服务商消息，并闭合可确定的记录状态。
type Syncer struct {
	Biz   *BizClient
	Prov  *ProviderClient
	Store Store
	Now   func() time.Time     // 可注入时钟，测试中固定
	// Horizon 是「提交后多久可确定结果」的判定时延，默认 DetermineHorizon(95s)；
	// 测试注入更小值以加速端到端验证。
	Horizon time.Duration
	// ScanMinAge 是扫描服务商消息的最近边界（now-ScanMinAge），默认 ProviderDecideMax(35s)；
	// 必须与 mock/真实服务商的物理发出时延匹配，测试注入更小值。
	ScanMinAge time.Duration
}

func NewSyncer(biz *BizClient, prov *ProviderClient, store Store) *Syncer {
	return &Syncer{Biz: biz, Prov: prov, Store: store, Now: time.Now}
}

func (s *Syncer) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *Syncer) horizon() time.Duration {
	if s.Horizon > 0 {
		return s.Horizon
	}
	return DetermineHorizon
}

func (s *Syncer) scanMinAge() time.Duration {
	if s.ScanMinAge > 0 {
		return s.ScanMinAge
	}
	return ProviderDecideMax
}

// SyncOnce 执行一轮增量同步。可随时被杀重启：全部进度落在 Store（PostgreSQL）里。
func (s *Syncer) SyncOnce(ctx context.Context) error {
	horizon := s.now().Add(-SyncLag)
	since, ok, err := s.Store.Cursor(ctx)
	if err != nil {
		return err
	}
	if !ok {
		since = time.Time{}
	}

	// 翻页拉取。游标只推进到「滞后线内最后一条记录的 updatedAt」，
	// 绝不推进到滞后线之外 —— 滞后线外的记录即使拉到也不占位。
	var boundary time.Time
	for {
		recs, err := s.Biz.Records(ctx, since, 500)
		if err != nil {
			return err
		}
		for _, r := range recs {
			if err := s.Store.UpsertRecord(ctx, r); err != nil {
				return err
			}
			s.scanProvider(ctx, r.SenderID) // best-effort，失败不阻断主流程
			if !r.UpdatedAt.After(horizon) {
				boundary = r.UpdatedAt // 滞后线内的最后一条，可作为安全游标
			}
		}
		if len(recs) < 500 {
			break
		}
		last := recs[len(recs)-1].UpdatedAt
		if last.After(horizon) {
			break // 已越过滞后线，剩下的下轮再拉
		}
		since = last // 同毫秒整桶重拉，Store 按 id 幂等
	}
	if !boundary.IsZero() {
		if err := s.Store.SetCursor(ctx, boundary); err != nil {
			return err
		}
	}
	return s.resolveDeterminable(ctx)
}

// scanProvider 补拉该发送方「已过可见性时延」的一段服务商消息。
// 窗口两端留 ClockSkew 重叠，按 providerMsgId 幂等去重，重复拉取无害。
func (s *Syncer) scanProvider(ctx context.Context, senderID string) {
	now := s.now()
	from := now.Add(-ScanDepth - ClockSkew)
	to := now.Add(-s.scanMinAge())
	if !to.After(from) {
		return
	}
	msgs, err := s.Prov.FetchWindow(ctx, senderID, from, to)
	if err != nil {
		return // 429/503/ revoked：下轮再扫，查询侧会反映为 incomplete
	}
	_ = s.Store.AddMessages(ctx, senderID, msgs)
}

// resolveDeterminable 把已能确定结果的 accepted/unknown 记录更正过来。
func (s *Syncer) resolveDeterminable(ctx context.Context) error {
	now := s.now()
	pending, err := s.Store.PendingDetermination(ctx, now, s.horizon())
	if err != nil {
		return err
	}
	for _, rec := range pending {
		// 以最近一次 submittedAt 为中心取 ±时延窗口内的服务商消息。
		nearby, err := s.Store.MessagesOf(ctx, rec.SenderID,
			rec.SubmittedAt.Add(-ClockSkew), rec.SubmittedAt.Add(s.horizon()))
		if err != nil {
			return err
		}
		toStatus, providerMsgID := Determine(rec, nearby)
		if err := s.Biz.Resolve(ctx, rec.ID, rec.Status, toStatus, providerMsgID); err != nil {
			continue // 网络抖动/409：下轮重试，幂等
		}
		_ = s.Store.UpdateStatus(ctx, rec.ID, toStatus, providerMsgID)
	}
	return nil
}

// ServeReconciliation 处理 GET /api/reconciliation?senderId=&from=&to=。
// 结论口径：
//   - to 越过可见性时延线 → 数据未采齐 → incomplete；
//   - 服务商 429/503/超时/发送方被注销 → incomplete；
//   - 其余按 Match 的 issues 判定 consistent / inconsistent。
func (s *Syncer) ServeReconciliation(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	senderID := q.Get("senderId")
	from, err1 := time.Parse(time.RFC3339Nano, q.Get("from"))
	to, err2 := time.Parse(time.RFC3339Nano, q.Get("to"))
	if senderID == "" || err1 != nil || err2 != nil || !to.After(from) {
		http.Error(w, `{"error":"bad request: senderId, from, to (ISO 8601) required"}`, http.StatusBadRequest)
		return
	}

	hasGap := false
	// 查询窗口的右端点还在「发出后 60s 才能查到」的时延内，服务商数据必然不全。
	if !to.Before(s.now().Add(-s.horizon())) {
		hasGap = true
	}

	msgs, err := s.Prov.FetchWindow(ctx, senderID,
		from.Add(-ClockSkew), to.Add(s.horizon()))
	switch {
	case errors.Is(err, ErrSenderRevoked):
		hasGap = true
		msgs = nil
	case err != nil:
		hasGap = true
		msgs = nil
	}

	records, err := s.Store.RecordsOf(ctx, senderID, from.UTC(), to.UTC())
	if err != nil {
		http.Error(w, `{"error":"store unavailable"}`, http.StatusInternalServerError)
		return
	}
	result := ComputeState(hasGap, Match(records, msgs))

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}
