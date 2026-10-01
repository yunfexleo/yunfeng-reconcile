package reconcile

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Store 是同步进度的持久化抽象。进程随时可能被强杀：
// 所有进度（记录、消息、游标）都必须落库，重启后从断点续跑。
// MemStore 用于本地开发/测试，SQLStore 是 PostgreSQL 实现。
type Store interface {
	UpsertRecord(ctx context.Context, r Record) error
	UpdateStatus(ctx context.Context, id, status, providerMsgID string) error
	// Cursor 返回增量同步游标；未设置时 ok=false。
	Cursor(ctx context.Context) (t time.Time, ok bool, err error)
	SetCursor(ctx context.Context, t time.Time) error
	// AddMessages 按 providerMsgId 幂等去重（窗口重叠重扫时不会产生重复）。
	AddMessages(ctx context.Context, senderID string, msgs []ProviderMessage) error
	RecordsOf(ctx context.Context, senderID string, from, to time.Time) ([]Record, error)
	MessagesOf(ctx context.Context, senderID string, from, to time.Time) ([]ProviderMessage, error)
	// PendingDetermination 返回所有「已越过判定时延、可确定结果」的 accepted/unknown 记录。
	PendingDetermination(ctx context.Context, now time.Time, horizon time.Duration) ([]Record, error)
}

// MemStore 是内存实现。
type MemStore struct {
	mu        sync.RWMutex
	records   map[string]Record
	msgs      map[string]map[string]ProviderMessage // senderId -> providerMsgId -> msg
	cursor    time.Time
	cursorSet bool
}

func NewMemStore() *MemStore {
	return &MemStore{
		records: map[string]Record{},
		msgs:    map[string]map[string]ProviderMessage{},
	}
}

var _ Store = (*MemStore)(nil)

func (s *MemStore) UpsertRecord(_ context.Context, r Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[r.ID] = r
	return nil
}

func (s *MemStore) UpdateStatus(_ context.Context, id, status, providerMsgID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.records[id]; ok {
		r.Status = status
		r.ProviderMsgID = providerMsgID
		s.records[id] = r
	}
	return nil
}

func (s *MemStore) Cursor(context.Context) (time.Time, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cursor, s.cursorSet, nil
}

func (s *MemStore) SetCursor(_ context.Context, t time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cursor, s.cursorSet = t, true
	return nil
}

func (s *MemStore) AddMessages(_ context.Context, senderID string, msgs []ProviderMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.msgs[senderID] == nil {
		s.msgs[senderID] = map[string]ProviderMessage{}
	}
	for _, m := range msgs {
		if _, dup := s.msgs[senderID][m.ProviderMsgID]; dup {
			continue
		}
		s.msgs[senderID][m.ProviderMsgID] = m
	}
	return nil
}

func (s *MemStore) RecordsOf(_ context.Context, senderID string, from, to time.Time) ([]Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []Record
	for _, r := range s.records {
		if r.SenderID != senderID {
			continue
		}
		// 以最近一次提交时间归属窗口；pending 无提交时间，落在更新时间内。
		t := r.SubmittedAt
		if t.IsZero() {
			t = r.UpdatedAt
		}
		if !t.Before(from) && t.Before(to) {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.Before(out[j].UpdatedAt) })
	return out, nil
}

func (s *MemStore) MessagesOf(_ context.Context, senderID string, from, to time.Time) ([]ProviderMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []ProviderMessage
	for _, m := range s.msgs[senderID] {
		if !m.SentAt.Before(from) && m.SentAt.Before(to) {
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SentAt.Before(out[j].SentAt) })
	return out, nil
}

func (s *MemStore) PendingDetermination(_ context.Context, now time.Time, horizon time.Duration) ([]Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []Record
	for _, r := range s.records {
		if Determinable(r, now, horizon) {
			out = append(out, r)
		}
	}
	return out, nil
}
