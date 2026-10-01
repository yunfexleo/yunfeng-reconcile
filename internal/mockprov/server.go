// Package mockprov 模拟服务商查询接口（笔试题 2.2 节）。
//
// 真实模拟的关键行为：
//   - 提交后 0~MaxDecideMS 毫秒内才「发出」，再 0~MaxVisibleMS 毫秒内才「可查」；
//   - 不按 requestId 去重：同一 requestId 提交两次发出两条；
//   - 单次查询最多返回 100 条，超出的部分【静默截断】；
//   - sentAt 带 ±SkewMS 的时钟偏差；
//   - 429（带 retryAfterSeconds）/ 503 / 410 SENDER_REVOKED（注销后历史消息消失）。
package mockprov

import (
	"encoding/json"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Config struct {
	Fail429Every int      // 每 N 次请求返回一次 429（0=关闭）
	Fail503Every int      // 每 N 次请求返回一次 503（0=关闭）
	Revoked      []string // 启动即被注销的 sender（对其查询永远 410）
	MaxDecideMS  int      // 提交后多少毫秒内发出（默认 30000，即题面 30s 上限）
	MaxVisibleMS int      // 发出后多少毫秒内可查（默认 60000，即题面 60s 上限）
	SkewMS       int      // 时钟偏差 ±N 毫秒（默认 5000）
	DropRate     float64  // 放弃概率 0~1（默认 0.1；-1 表示用默认值）
}

type message struct {
	providerMsgID string
	requestID     *string
	recipient     string
	sentAt        time.Time
	visibleAt     time.Time // 发出后还要等 visibility 延迟才查得到
}

type Server struct {
	cfg Config

	mu       sync.Mutex
	msgs     map[string][]*message
	revoked  map[string]bool
	seq      int
	reqCount int
}

func New(cfg Config) *Server {
	if cfg.MaxDecideMS == 0 {
		cfg.MaxDecideMS = 30000
	}
	if cfg.MaxVisibleMS == 0 {
		cfg.MaxVisibleMS = 60000
	}
	if cfg.SkewMS == 0 {
		cfg.SkewMS = 5000
	}
	if cfg.DropRate < 0 {
		cfg.DropRate = 0.1
	}
	s := &Server{cfg: cfg, msgs: map[string][]*message{}, revoked: map[string]bool{}}
	for _, id := range cfg.Revoked {
		s.revoked[id] = true
	}
	return s
}

// SetDropRate 运行时调整放弃概率（测试用：1.0 = 全部放弃）。
func (s *Server) SetDropRate(rate float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cfg.DropRate = rate
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/submit", s.handleSubmit)
	mux.HandleFunc("GET /senders/", s.handleMessages) // /senders/{id}/messages
	mux.HandleFunc("POST /internal/revoke", s.handleRevoke)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.injectFault(w) {
			return
		}
		mux.ServeHTTP(w, r)
	})
}

// injectFault 按配置周期性返回 429 / 503。返回 true 表示已写出响应。
func (s *Server) injectFault(w http.ResponseWriter) bool {
	s.mu.Lock()
	s.reqCount++
	n := s.reqCount
	s.mu.Unlock()
	if s.cfg.Fail429Every > 0 && n%s.cfg.Fail429Every == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		json.NewEncoder(w).Encode(map[string]int{"retryAfterSeconds": 1})
		return true
	}
	if s.cfg.Fail503Every > 0 && n%s.cfg.Fail503Every == 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		return true
	}
	return false
}

// handleSubmit 模拟业务系统提交消息（题目中这个调用发生在两个 mock 之间）。
// 重要：同一个 requestId 提交两次会产生两条消息（不去重）。
func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SenderID  string `json:"senderId"`
		RequestID string `json:"requestId"`
		Recipient string `json:"recipient"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.SenderID == "" {
		http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	if s.revoked[body.SenderID] {
		s.mu.Unlock()
		w.WriteHeader(http.StatusGone)
		return
	}
	resp := map[string]string{"providerMsgId": ""}
	if rand.Float64() >= s.cfg.DropRate {
		s.seq++
		id := "pm-" + strconv.Itoa(s.seq)
		sentAt := time.Now().UTC().Add(randMillis(s.cfg.MaxDecideMS))
		sentAt = sentAt.Add(randSkew(s.cfg.SkewMS))
		msg := &message{
			providerMsgID: id,
			requestID:     &body.RequestID,
			recipient:     body.Recipient,
			sentAt:        sentAt,
			visibleAt:     sentAt.Add(randMillis(s.cfg.MaxVisibleMS)),
		}
		s.msgs[body.SenderID] = append(s.msgs[body.SenderID], msg)
		resp["providerMsgId"] = id
	}
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleMessages 对应 2.2 节查询接口：from <= sentAt < to，升序，最多 100 条，静默截断。
func (s *Server) handleMessages(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/senders/")
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 || parts[1] != "messages" {
		http.NotFound(w, r)
		return
	}
	senderID := parts[0]

	s.mu.Lock()
	if s.revoked[senderID] {
		s.mu.Unlock()
		w.WriteHeader(http.StatusGone) // SENDER_REVOKED：历史消息从此查不到
		return
	}
	s.mu.Unlock()

	from, _ := strconv.ParseInt(r.URL.Query().Get("from"), 10, 64)
	to, _ := strconv.ParseInt(r.URL.Query().Get("to"), 10, 64)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 100 {
		limit = 100
	}

	now := time.Now().UTC()
	s.mu.Lock()
	var matched []*message
	for _, m := range s.msgs[senderID] {
		if m.visibleAt.After(now) { // 还没到可查时间
			continue
		}
		if m.sentAt.UnixMilli() >= from && m.sentAt.UnixMilli() < to {
			matched = append(matched, m)
		}
	}
	s.mu.Unlock()

	sort.Slice(matched, func(i, j int) bool {
		if matched[i].sentAt != matched[j].sentAt {
			return matched[i].sentAt.Before(matched[j].sentAt)
		}
		return matched[i].providerMsgID < matched[j].providerMsgID
	})
	if len(matched) > limit { // 静默截断：调用方必须自己检测
		matched = matched[:limit]
	}

	out := make([]map[string]any, 0, len(matched))
	for _, m := range matched {
		out = append(out, map[string]any{
			"providerMsgId": m.providerMsgID,
			"requestId":     m.requestID,
			"recipient":     m.recipient,
			"sentAt":        m.sentAt.UnixMilli(),
		})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"messages": out})
}

// handleRevoke 注销发送方通道：此后查询一律 410，历史消息立即不可查。
func (s *Server) handleRevoke(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SenderID string `json:"senderId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.SenderID == "" {
		http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	s.revoked[body.SenderID] = true
	delete(s.msgs, body.SenderID)
	s.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func randMillis(max int) time.Duration {
	return time.Duration(rand.Intn(max+1)) * time.Millisecond
}

// randSkew 返回 [-max, +max] 毫秒的时钟偏差。
func randSkew(max int) time.Duration {
	return time.Duration(rand.Intn(2*max+1)-max) * time.Millisecond
}
