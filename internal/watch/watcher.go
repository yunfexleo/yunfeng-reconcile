// Package watch 是巡检与告警引擎（笔试题 3.2 节）：定时探测业务系统，
// 故障时通过 alert.Alerter 发告警。
//
// 核心设计：
//   - flap 防抖：连续失败 N 次才告警，连续成功 M 次才判恢复。
//     题目明说"两次重启之间端口可能短暂可用"——单次探测成功绝不能翻转状态；
//   - 单点连通性只报一条（biz-health），stats/senders 拉取失败不重复告警；
//   - 发送方异常按节点聚合：一个节点上大量 sender disconnected 说明是节点故障，
//     合并成一条 critical，避免 2000 个 sender 刷出 2000 条告警；
//   - 静默由 alert.Alerter 统一处理（POST /api/silence）。
package watch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"stability-service/internal/alert"
)

type Config struct {
	Interval              time.Duration // 探测周期（默认 10s）
	FailThreshold         int           // 连续失败多少次才告警（默认 3）
	RecoverThreshold      int           // 连续成功多少次才判恢复（默认 2）
	PendingCountThreshold int           // pending 超过该条数告警（默认 1000）
	PendingAgeThreshold   time.Duration // 最早 pending 超过该时长告警（默认 5min）
	NodeDownMinSenders    int           // 同一节点 disconnected 达到该数量判定节点故障（默认 5）
}

func (c Config) withDefaults() Config {
	if c.Interval <= 0 {
		c.Interval = 10 * time.Second
	}
	if c.FailThreshold <= 0 {
		c.FailThreshold = 3
	}
	if c.RecoverThreshold <= 0 {
		c.RecoverThreshold = 2
	}
	if c.PendingCountThreshold <= 0 {
		c.PendingCountThreshold = 1000
	}
	if c.PendingAgeThreshold <= 0 {
		c.PendingAgeThreshold = 5 * time.Minute
	}
	if c.NodeDownMinSenders <= 0 {
		c.NodeDownMinSenders = 5
	}
	return c
}

// Watcher 周期性巡检业务系统并发出告警。
type Watcher struct {
	bizURL  string
	alerter *alert.Alerter
	client  *http.Client
	cfg     Config

	mu     sync.Mutex
	states map[string]*state // 每个告警键的 flap 状态
}

type state struct {
	fails     int
	successes int
	alerted   bool
}

func New(bizURL string, a *alert.Alerter, cfg Config) *Watcher {
	return &Watcher{
		bizURL:  bizURL,
		alerter: a,
		client:  &http.Client{Timeout: 3 * time.Second},
		cfg:     cfg.withDefaults(),
		states:  map[string]*state{},
	}
}

// Run 阻塞运行巡检循环，ctx 取消即退出。
func (w *Watcher) Run(ctx context.Context) {
	tick := time.NewTicker(w.cfg.Interval)
	defer tick.Stop()
	for {
		w.CheckOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// CheckOnce 执行一轮全部探测。导出以便测试确定性驱动。
func (w *Watcher) CheckOnce(ctx context.Context) {
	w.checkHealth(ctx)
	w.checkStats(ctx)
	w.checkSenders(ctx)
}

// transition 更新某告警键的 flap 状态：
// 首次越过失败阈值返回 fired=true；首次越过恢复阈值返回 recovered=true。
func (w *Watcher) transition(key string, ok bool) (fired, recovered bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	st := w.states[key]
	if st == nil {
		st = &state{}
		w.states[key] = st
	}
	if ok {
		st.fails = 0
		st.successes++
		if st.alerted && st.successes >= w.cfg.RecoverThreshold {
			st.alerted = false
			return false, true
		}
		return false, false
	}
	st.successes = 0
	st.fails++
	if !st.alerted && st.fails >= w.cfg.FailThreshold {
		st.alerted = true
		return true, false
	}
	return false, false
}

// describe 由告警键推导标题与级别。
func describe(key string) (title string, sev alert.Severity) {
	switch key {
	case "biz-health":
		return "业务系统不可用", alert.Critical
	case "biz-db":
		return "业务系统数据库异常", alert.Warning
	case "biz-provider":
		return "业务系统访问服务商异常", alert.Warning
	case "biz-backlog":
		return "消息积压：pending 记录过多或等待过久", alert.Warning
	}
	if strings.HasPrefix(key, "node-") && strings.HasSuffix(key, "-down") {
		return "发送节点失联（大量发送方断开）", alert.Critical
	}
	if strings.HasPrefix(key, "sender-") {
		switch {
		case strings.HasSuffix(key, "-throttled"):
			return "发送方被限流（通常可自行恢复）", alert.Warning
		case strings.HasSuffix(key, "-disconnected"):
			return "发送方连接断开", alert.Warning
		default:
			return "发送方通道异常（suspended/revoked，不可自行恢复）", alert.Critical
		}
	}
	return "巡检异常", alert.Warning
}

// fire / recover 统一出口：恢复前先解除 Alerter 的去抖标记，否则恢复消息会被吞。
func (w *Watcher) fire(key, text string) {
	title, sev := describe(key)
	_ = w.alerter.Send(context.Background(), alert.Event{
		Key: key, Title: title, Text: text, Severity: sev,
	})
}

func (w *Watcher) recover(key string) {
	title, _ := describe(key)
	w.alerter.Resolve(key) // 解除去抖，允许发送恢复通知
	_ = w.alerter.Send(context.Background(), alert.Event{
		Key: key, Title: title + "（已恢复）", Severity: alert.Info,
	})
}

func (w *Watcher) get(ctx context.Context, path string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, w.bizURL+path, nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return body, resp.StatusCode, err
}

// checkHealth 连通性 + 依赖状态。连通性故障只报这一条，不连锁刷屏。
func (w *Watcher) checkHealth(ctx context.Context) {
	body, status, err := w.get(ctx, "/internal/health")
	if err != nil || status != http.StatusOK {
		if fired, _ := w.transition("biz-health", false); fired {
			w.fire("biz-health", fmt.Sprintf("探测失败: err=%v status=%d", err, status))
		}
		return
	}
	if _, recovered := w.transition("biz-health", true); recovered {
		w.recover("biz-health")
	}
	var h struct {
		DB       string `json:"db"`
		Provider string `json:"provider"`
	}
	if json.Unmarshal(body, &h) != nil {
		return
	}
	w.checkSub("biz-db", h.DB == "ok" || h.DB == "")
	w.checkSub("biz-provider", h.Provider == "ok" || h.Provider == "")
}

func (w *Watcher) checkSub(key string, ok bool) {
	fired, recovered := w.transition(key, ok)
	switch {
	case fired:
		w.fire(key, "health 接口报告依赖异常")
	case recovered:
		w.recover(key)
	}
}

// checkStats 消息发不出去的信号：pending 堆积或最老 pending 等待过久。
func (w *Watcher) checkStats(ctx context.Context) {
	body, status, err := w.get(ctx, "/internal/stats")
	if err != nil || status != http.StatusOK {
		return // 连通性由 health 检查负责
	}
	var st struct {
		PendingCount          int `json:"pendingCount"`
		OldestPendingAgeSeconds int `json:"oldestPendingAgeSeconds"`
	}
	if json.Unmarshal(body, &st) != nil {
		return
	}
	ok := st.PendingCount < w.cfg.PendingCountThreshold &&
		time.Duration(st.OldestPendingAgeSeconds)*time.Second < w.cfg.PendingAgeThreshold
	fired, recovered := w.transition("biz-backlog", ok)
	switch {
	case fired:
		w.fire("biz-backlog", fmt.Sprintf("pendingCount=%d oldestPendingAge=%ds",
			st.PendingCount, st.OldestPendingAgeSeconds))
	case recovered:
		w.recover("biz-backlog")
	}
}

// checkSenders 发送方异常：suspended/revoked 是 critical（不可恢复），
// throttled 是 warning（通常几分钟自愈），disconnected 按节点聚合。
func (w *Watcher) checkSenders(ctx context.Context) {
	body, status, err := w.get(ctx, "/internal/senders")
	if err != nil || status != http.StatusOK {
		return
	}
	var parsed struct {
		Senders []struct {
			SenderID string `json:"senderId"`
			Status   string `json:"status"`
			NodeID   string `json:"nodeId"`
		} `json:"senders"`
	}
	if json.Unmarshal(body, &parsed) != nil {
		return
	}

	nodeDown := map[string][]string{} // nodeId -> disconnected sender ids
	anomalous := map[string]bool{}    // key -> true
	for _, s := range parsed.Senders {
		switch s.Status {
		case "suspended", "revoked":
			anomalous["sender-"+s.SenderID+"-down"] = true
		case "throttled":
			anomalous["sender-"+s.SenderID+"-throttled"] = true
		case "disconnected":
			nodeDown[s.NodeID] = append(nodeDown[s.NodeID], s.SenderID)
		}
	}
	for nodeID, ids := range nodeDown {
		if len(ids) >= w.cfg.NodeDownMinSenders {
			anomalous["node-"+nodeID+"-down"] = true // 聚合为一条节点级告警
		} else {
			for _, id := range ids {
				anomalous["sender-"+id+"-disconnected"] = true
			}
		}
	}

	// 先恢复：已告警但本轮不再异常的键
	w.mu.Lock()
	known := make([]string, 0, len(w.states))
	for key := range w.states {
		known = append(known, key)
	}
	w.mu.Unlock()
	for _, key := range known {
		if key == "biz-health" || key == "biz-db" || key == "biz-provider" || key == "biz-backlog" {
			continue
		}
		if !anomalous[key] {
			if _, recovered := w.transition(key, true); recovered {
				w.recover(key)
			}
		}
	}
	// 再触发
	for key := range anomalous {
		if fired, _ := w.transition(key, false); fired {
			w.fire(key, "巡检发现发送方/节点状态异常")
		}
	}
}
