// Package mockbiz 模拟业务系统内部接口（笔试题 2.1 节）。
//
// 真实模拟的关键行为：
//   - 状态机 pending → accepted|failed|unknown → sent|failed，后台自行推进 accepted；
//   - updatedAt 取修改时刻，LateCommit 模式下取「事务开始值」且 30s 后才可见
//     （考验你的服务滞后游标是否正确）；
//   - 同一毫秒可能有多条记录；
//   - resolve 接口严格校验 fromStatus，状态不符返回 409 STATUS_CHANGED；
//   - 提交记录时真实调用 mock 服务商的 /internal/submit（两 mock 之间真实联动）；
//   - 故障注入：周期性 503、请求 hang 30s。
package mockbiz

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"
)

type Config struct {
	ProviderURL  string // mock 服务商地址（提交消息用）
	Senders      int    // 发送方数量（默认 20）
	SeedRecords  int    // 启动时预置的发送记录数（默认 200）
	Fail503Every int    // 每 N 次请求返回一次 503（0=关闭）
	Slow         bool   // 所有请求 hang 30s（模拟"进程在、端口通、请求不返回"）
	LateCommit   bool   // updatedAt 取事务开始值，30s 后才可查（考验滞后游标）
	AdvanceMinMS int    // 后台推进 accepted→终态的最小时延（默认 1000）
	AdvanceMaxMS int    // 最大时延（默认 30000，即题面"提交后最多 30s 内发出或放弃"）
}

type record struct {
	ID            string
	SenderID      string
	Recipient     string
	RequestID     string
	Status        string
	ProviderMsgID string
	SubmittedAt   time.Time
	UpdatedAt     time.Time
	VisibleAt     time.Time // LateCommit 模式：记录在该时刻之后才可见
	Live          bool      // 通过 Submit 提交的记录：由后台推进到终态
}

type sender struct {
	SenderID        string `json:"senderId"`
	Status          string `json:"status"`
	NodeID          string `json:"nodeId"`
	StatusChangedAt string `json:"statusChangedAt"`
}

type Server struct {
	cfg Config

	mu        sync.Mutex
	records   map[string]*record
	senders   []sender
	startedAt time.Time
	counts    map[string]int // 本次进程启动以来进入 sent/failed/unknown 的条数
	seq       int
	reqCount  int

	client *http.Client
}

func New(cfg Config) *Server {
	if cfg.Senders <= 0 {
		cfg.Senders = 20
	}
	if cfg.SeedRecords < 0 { // -1 = 使用默认值
		cfg.SeedRecords = 200
	}
	if cfg.AdvanceMinMS == 0 {
		cfg.AdvanceMinMS = 1000
	}
	if cfg.AdvanceMaxMS == 0 {
		cfg.AdvanceMaxMS = 30000
	}
	s := &Server{
		cfg:       cfg,
		records:   map[string]*record{},
		startedAt: time.Now().UTC(),
		counts:    map[string]int{},
		client:    &http.Client{Timeout: 5 * time.Second},
	}
	s.seed()
	return s
}

// seed 预置发送方与历史记录。每 50 条记录共享同一 updatedAt（同一毫秒多条）。
func (s *Server) seed() {
	now := time.Now().UTC()
	for i := 0; i < s.cfg.Senders; i++ {
		st := "connected"
		if i == 7 {
			st = "throttled"
		} else if i == 13 {
			st = "suspended"
		}
		s.senders = append(s.senders, sender{
			SenderID:        fmt.Sprintf("sender-%d", i),
			Status:          st,
			NodeID:          fmt.Sprintf("node-%d", i%3),
			StatusChangedAt: now.Format(time.RFC3339),
		})
	}
	for i := 0; i < s.cfg.SeedRecords; i++ {
		updatedAt := now.Add(-time.Hour).Add(time.Duration(i/50) * time.Millisecond)
		rec := &record{
			ID:          fmt.Sprintf("seed-%d", i),
			SenderID:    fmt.Sprintf("sender-%d", i%s.cfg.Senders),
			Recipient:   fmt.Sprintf("user-%d@example.com", i),
			RequestID:   fmt.Sprintf("req-seed-%d", i),
			UpdatedAt:   updatedAt,
			SubmittedAt: updatedAt,
		}
		switch i % 5 {
		case 0:
			rec.Status = "pending"
			rec.SubmittedAt = time.Time{}
		case 1:
			rec.Status = "accepted"
		case 2:
			rec.Status = "sent"
			rec.ProviderMsgID = fmt.Sprintf("pm-seed-%d", i)
		case 3:
			rec.Status = "failed"
		default:
			rec.Status = "unknown"
		}
		s.records[rec.ID] = rec
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/send-records", s.handleRecords)
	mux.HandleFunc("GET /internal/send-records/", s.handleRecordByID)
	mux.HandleFunc("POST /internal/send-records/", s.handleResolve)
	mux.HandleFunc("GET /internal/senders", s.handleSenders)
	mux.HandleFunc("GET /internal/health", s.handleHealth)
	mux.HandleFunc("GET /internal/stats", s.handleStats)
	mux.HandleFunc("POST /internal/simulate/submit", s.handleSimulateSubmit)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.injectFault(w, r) {
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func (s *Server) injectFault(w http.ResponseWriter, r *http.Request) bool {
	if s.cfg.Slow {
		time.Sleep(30 * time.Second) // 模拟"请求一直不返回"
		return false
	}
	s.mu.Lock()
	s.reqCount++
	n := s.reqCount
	s.mu.Unlock()
	if s.cfg.Fail503Every > 0 && n%s.cfg.Fail503Every == 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		return true
	}
	return false
}

// Submit 创建一条 accepted 记录并真实提交给 mock 服务商。
// 返回 requestId。之后服务商决定发出（带 providerMsgId）或放弃。
func (s *Server) Submit(senderID, recipient string) string {
	s.mu.Lock()
	s.seq++
	rec := &record{
		ID:          fmt.Sprintf("live-%d", s.seq),
		SenderID:    senderID,
		Recipient:   recipient,
		RequestID:   fmt.Sprintf("req-live-%d", s.seq),
		Status:      "accepted",
		SubmittedAt: time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
		Live:        true,
	}
	s.records[rec.ID] = rec
	reqID := rec.RequestID
	s.mu.Unlock()

	// 提交给服务商；它可能延迟发出、延迟可查，或直接放弃。
	// 业务系统侧不保留提交结果（这正是 unknown 状态的来源），推进时回查询确认。
	payload, _ := json.Marshal(map[string]string{
		"senderId": senderID, "requestId": reqID, "recipient": recipient,
	})
	http.Post(s.cfg.ProviderURL+"/internal/submit", "application/json", bytes.NewReader(payload))
	return reqID
}

// StartBackground 启动后台推进：accepted 记录在 1~30s 后自行推进为
// sent（服务商已发出）或 failed（服务商放弃），模拟题面 2.1 节的行为。
func (s *Server) StartBackground(ctx context.Context) {
	go func() {
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				s.advance()
			}
		}
	}()
}

// advance 推进 accepted 记录。规则：
//   - 通过 Submit 产生的记录持有服务商返回的 providerMsgId（发出）或空（放弃）；
//   - 时延取 1~30s 随机，模拟「每次提交后最多 30 秒内发出或放弃」。
func (s *Server) advance() {
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rec := range s.records {
		if rec.Status != "accepted" || !rec.Live { // 预置的 accepted 种子不推进
			continue
		}
		delay := time.Duration(rand.Intn(s.cfg.AdvanceMaxMS-s.cfg.AdvanceMinMS+1)+s.cfg.AdvanceMinMS) * time.Millisecond
		if now.Sub(rec.SubmittedAt) < delay {
			continue
		}
		// 向服务商确认这条提交的实际结果。
		msgID := s.askProvider(rec.SenderID, rec.RequestID)
		if msgID != "" {
			s.applyUpdate(rec, "sent", msgID, now)
		} else {
			s.applyUpdate(rec, "failed", "", now)
		}
	}
}

// askProvider 查服务商侧该 requestId 最近一条可见消息（提交已由 Submit 真实发出）。
func (s *Server) askProvider(senderID, requestID string) string {
	now := time.Now().UTC()
	q := url.Values{}
	q.Set("from", strconv.FormatInt(now.Add(-2*time.Minute).UnixMilli(), 10))
	q.Set("to", strconv.FormatInt(now.Add(time.Minute).UnixMilli(), 10))
	q.Set("limit", "100")
	resp, err := s.client.Get(s.cfg.ProviderURL + "/senders/" + senderID + "/messages?" + q.Encode())
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	var body struct {
		Messages []struct {
			ProviderMsgID string  `json:"providerMsgId"`
			RequestID     *string `json:"requestId"`
		} `json:"messages"`
	}
	if json.NewDecoder(resp.Body).Decode(&body) != nil {
		return ""
	}
	for i := len(body.Messages) - 1; i >= 0; i-- { // 取最近一条（重提交会多条）
		if body.Messages[i].RequestID != nil && *body.Messages[i].RequestID == requestID {
			return body.Messages[i].ProviderMsgID
		}
	}
	return ""
}

// applyUpdate 修改记录；LateCommit 模式下 updatedAt 取"事务开始值"且 30s 后才可见。
func (s *Server) applyUpdate(rec *record, status, providerMsgID string, now time.Time) {
	updatedAt := now
	if s.cfg.LateCommit {
		rec.VisibleAt = now.Add(30 * time.Second)
	}
	rec.Status = status
	rec.ProviderMsgID = providerMsgID
	rec.UpdatedAt = updatedAt
	s.counts[status]++
}

func (s *Server) handleRecords(w http.ResponseWriter, r *http.Request) {
	since := parseISO(r.URL.Query().Get("updatedSince"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 500 {
		limit = 500
	}
	now := time.Now().UTC()
	s.mu.Lock()
	var out []*record
	for _, rec := range s.records {
		if rec.VisibleAt.After(now) { // LateCommit：晚提交的记录此刻还查不到
			continue
		}
		if rec.UpdatedAt.After(since) || rec.UpdatedAt.Equal(since) {
			out = append(out, rec)
		}
	}
	s.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if !out[i].UpdatedAt.Equal(out[j].UpdatedAt) {
			return out[i].UpdatedAt.Before(out[j].UpdatedAt)
		}
		return out[i].ID < out[j].ID
	})
	if len(out) > limit {
		out = out[:limit]
	}
	records := make([]map[string]any, 0, len(out))
	for _, rec := range out {
		records = append(records, rec.toJSON())
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"records": records})
}

func (s *Server) handleRecordByID(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Path[len("/internal/send-records/"):]
	s.mu.Lock()
	rec, ok := s.records[id]
	s.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"record": rec.toJSON()})
}

// handleResolve 对应 2.1 节 resolve 接口。
func (s *Server) handleResolve(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Path[len("/internal/send-records/"):]
	id = id[:len(id)-len("/resolve")]
	var body struct {
		FromStatus    string `json:"fromStatus"`
		ToStatus      string `json:"toStatus"`
		ProviderMsgID string `json:"providerMsgId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
		return
	}
	if body.FromStatus != "accepted" && body.FromStatus != "unknown" {
		http.Error(w, `{"error":"fromStatus must be accepted or unknown"}`, http.StatusBadRequest)
		return
	}
	if body.ToStatus != "sent" && body.ToStatus != "failed" {
		http.Error(w, `{"error":"toStatus must be sent or failed"}`, http.StatusBadRequest)
		return
	}
	if body.ToStatus == "sent" && body.ProviderMsgID == "" {
		http.Error(w, `{"error":"providerMsgId required when toStatus=sent"}`, http.StatusBadRequest)
		return
	}
	now := time.Now().UTC()
	s.mu.Lock()
	rec, ok := s.records[id]
	if !ok {
		s.mu.Unlock()
		http.NotFound(w, r)
		return
	}
	if rec.Status != body.FromStatus {
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict) // 409 STATUS_CHANGED，不做修改
		json.NewEncoder(w).Encode(map[string]any{"record": rec.toJSON()})
		return
	}
	s.applyUpdate(rec, body.ToStatus, body.ProviderMsgID, now)
	resp := rec.toJSON()
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"record": resp})
}

func (s *Server) handleSenders(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	out := make([]sender, len(s.senders))
	copy(out, s.senders)
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"senders": out})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"version":   "2.14.0-mock",
		"startedAt": s.startedAt.Format(time.RFC3339),
		"db":        "ok",
		"provider":  "ok",
	})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	now := time.Now().UTC()
	s.mu.Lock()
	pending, oldest := 0, 0
	for _, rec := range s.records {
		if rec.Status == "pending" {
			pending++
			if age := int(now.Sub(rec.UpdatedAt).Seconds()); age > oldest {
				oldest = age
			}
		}
	}
	body := map[string]any{
		"startedAt":            s.startedAt.Format(time.RFC3339),
		"sent":                 s.counts["sent"],
		"failed":               s.counts["failed"],
		"unknown":              s.counts["unknown"],
		"pendingCount":         pending,
		"oldestPendingAgeSeconds": oldest,
	}
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(body)
}

// handleSimulateSubmit 供 curl/演示用：POST /internal/simulate/submit {senderId, recipient}。
func (s *Server) handleSimulateSubmit(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SenderID  string `json:"senderId"`
		Recipient string `json:"recipient"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.SenderID == "" {
		http.Error(w, `{"error":"senderId required"}`, http.StatusBadRequest)
		return
	}
	reqID := s.Submit(body.SenderID, body.Recipient)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"requestId": reqID})
}

func (rec *record) toJSON() map[string]any {
	format := func(t time.Time) string {
		if t.IsZero() {
			return ""
		}
		return t.UTC().Format("2006-01-02T15:04:05.000Z")
	}
	submitted := format(rec.SubmittedAt)
	return map[string]any{
		"id":            rec.ID,
		"senderId":      rec.SenderID,
		"recipient":     rec.Recipient,
		"requestId":     rec.RequestID,
		"status":        rec.Status,
		"providerMsgId": rec.ProviderMsgID,
		"submittedAt":   submitted,
		"createdAt":     format(rec.UpdatedAt),
		"updatedAt":     format(rec.UpdatedAt),
		"version":       1,
	}
}

func parseISO(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}
	}
	return t.UTC()
}
