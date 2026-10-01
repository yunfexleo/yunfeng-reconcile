package reconcile

import (
	"context"
	"database/sql"
	"time"
)

// SQLStore 是 Store 的 PostgreSQL 实现（database/sql + pgx 驱动）。
// 所有写操作幂等：进程被杀重启后重放同一轮同步不会产生重复数据。
type SQLStore struct {
	db *sql.DB
}

func NewSQLStore(db *sql.DB) *SQLStore {
	return &SQLStore{db: db}
}

var _ Store = (*SQLStore)(nil)

// EnsureSchema 建表（幂等，可每次启动时调用）。
func (s *SQLStore) EnsureSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS send_records (
			id              TEXT PRIMARY KEY,
			sender_id       TEXT NOT NULL,
			request_id      TEXT NOT NULL DEFAULT '',
			status          TEXT NOT NULL,
			provider_msg_id TEXT NOT NULL DEFAULT '',
			submitted_at    TIMESTAMPTZ,
			updated_at      TIMESTAMPTZ NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_send_records_window
			ON send_records (sender_id, COALESCE(submitted_at, updated_at))`,
		`CREATE TABLE IF NOT EXISTS provider_messages (
			sender_id       TEXT NOT NULL,
			provider_msg_id TEXT NOT NULL,
			request_id      TEXT,
			recipient       TEXT NOT NULL DEFAULT '',
			sent_at         TIMESTAMPTZ NOT NULL,
			PRIMARY KEY (sender_id, provider_msg_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_provider_messages_sent
			ON provider_messages (sender_id, sent_at)`,
		`CREATE TABLE IF NOT EXISTS sync_state (
			key    TEXT PRIMARY KEY,
			cursor TIMESTAMPTZ NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLStore) UpsertRecord(ctx context.Context, r Record) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO send_records (id, sender_id, request_id, status, provider_msg_id, submitted_at, updated_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7)
		 ON CONFLICT (id) DO UPDATE SET
			sender_id = EXCLUDED.sender_id,
			request_id = EXCLUDED.request_id,
			status = EXCLUDED.status,
			provider_msg_id = EXCLUDED.provider_msg_id,
			submitted_at = EXCLUDED.submitted_at,
			updated_at = EXCLUDED.updated_at`,
		r.ID, r.SenderID, r.RequestID, r.Status, r.ProviderMsgID,
		nullTime(r.SubmittedAt), r.UpdatedAt,
	)
	return err
}

func (s *SQLStore) UpdateStatus(ctx context.Context, id, status, providerMsgID string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE send_records SET status = $2, provider_msg_id = $3 WHERE id = $1`,
		id, status, providerMsgID)
	return err
}

func (s *SQLStore) Cursor(ctx context.Context) (time.Time, bool, error) {
	var t time.Time
	err := s.db.QueryRowContext(ctx,
		`SELECT cursor FROM sync_state WHERE key = 'records'`).Scan(&t)
	if err == sql.ErrNoRows {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	return t, true, nil
}

func (s *SQLStore) SetCursor(ctx context.Context, t time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO sync_state (key, cursor) VALUES ('records', $1)
		 ON CONFLICT (key) DO UPDATE SET cursor = EXCLUDED.cursor`, t)
	return err
}

func (s *SQLStore) AddMessages(ctx context.Context, senderID string, msgs []ProviderMessage) error {
	if len(msgs) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO provider_messages (sender_id, provider_msg_id, request_id, recipient, sent_at)
		 VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, m := range msgs {
		var reqID sql.NullString
		if m.RequestID != nil {
			reqID = sql.NullString{String: *m.RequestID, Valid: true}
		}
		if _, err := stmt.ExecContext(ctx, senderID, m.ProviderMsgID, reqID, m.Recipient, m.SentAt); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *SQLStore) RecordsOf(ctx context.Context, senderID string, from, to time.Time) ([]Record, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, sender_id, request_id, status, provider_msg_id, submitted_at, updated_at
		 FROM send_records
		 WHERE sender_id = $1
		   AND COALESCE(submitted_at, updated_at) >= $2
		   AND COALESCE(submitted_at, updated_at) <  $3
		 ORDER BY updated_at`,
		senderID, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Record
	for rows.Next() {
		var r Record
		var submitted sql.NullTime
		if err := rows.Scan(&r.ID, &r.SenderID, &r.RequestID, &r.Status, &r.ProviderMsgID, &submitted, &r.UpdatedAt); err != nil {
			return nil, err
		}
		if submitted.Valid {
			r.SubmittedAt = submitted.Time
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *SQLStore) MessagesOf(ctx context.Context, senderID string, from, to time.Time) ([]ProviderMessage, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT provider_msg_id, request_id, recipient, sent_at
		 FROM provider_messages
		 WHERE sender_id = $1 AND sent_at >= $2 AND sent_at < $3
		 ORDER BY sent_at`,
		senderID, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ProviderMessage
	for rows.Next() {
		var m ProviderMessage
		var reqID sql.NullString
		if err := rows.Scan(&m.ProviderMsgID, &reqID, &m.Recipient, &m.SentAt); err != nil {
			return nil, err
		}
		if reqID.Valid {
			m.RequestID = &reqID.String
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// PendingDetermination 直接下推到 SQL：状态为 accepted/unknown 且 submitted_at
// 早于 (now - DetermineHorizon) 的记录，其结果在服务商侧已经可以确定。
func (s *SQLStore) PendingDetermination(ctx context.Context, now time.Time) ([]Record, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, sender_id, request_id, status, provider_msg_id, submitted_at, updated_at
		 FROM send_records
		 WHERE status IN ('accepted', 'unknown')
		   AND submitted_at IS NOT NULL
		   AND submitted_at <= $1`,
		now.Add(-DetermineHorizon))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Record
	for rows.Next() {
		var r Record
		if err := rows.Scan(&r.ID, &r.SenderID, &r.RequestID, &r.Status, &r.ProviderMsgID, &r.SubmittedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}
