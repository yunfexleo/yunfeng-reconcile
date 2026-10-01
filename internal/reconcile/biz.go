package reconcile

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// BizClient 是业务系统内部接口（2.1 节）的客户端。
type BizClient struct {
	BaseURL string
	HTTP    *http.Client
}

func NewBizClient(baseURL string) *BizClient {
	return &BizClient{BaseURL: baseURL, HTTP: &http.Client{Timeout: 10 * time.Second}}
}

type rawRecord struct {
	ID            string `json:"id"`
	SenderID      string `json:"senderId"`
	RequestID     string `json:"requestId"`
	Status        string `json:"status"`
	ProviderMsgID string `json:"providerMsgId"`
	SubmittedAt   string `json:"submittedAt"`
	UpdatedAt     string `json:"updatedAt"`
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

// Records 拉取 updatedAt >= since 的记录（升序，服务端 limit 上限 500）。
// 同一毫秒可能有多条，游标停在毫秒边界上会整桶重拉，由 Store 按 id 幂等去重。
func (c *BizClient) Records(ctx context.Context, since time.Time, limit int) ([]Record, error) {
	q := url.Values{}
	if !since.IsZero() {
		q.Set("updatedSince", since.UTC().Format("2006-01-02T15:04:05.000Z"))
	}
	q.Set("limit", strconv.Itoa(limit))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.BaseURL+"/internal/send-records?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("biz records: status %d", resp.StatusCode)
	}
	var body struct {
		Records []rawRecord `json:"records"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, err
	}
	out := make([]Record, 0, len(body.Records))
	for _, rr := range body.Records {
		out = append(out, Record{
			ID:            rr.ID,
			SenderID:      rr.SenderID,
			RequestID:     rr.RequestID,
			Status:        rr.Status,
			ProviderMsgID: rr.ProviderMsgID,
			SubmittedAt:   parseISO(rr.SubmittedAt),
			UpdatedAt:     parseISO(rr.UpdatedAt),
		})
	}
	return out, nil
}

// Resolve 调用 2.1 节的 resolve 接口，把记录从 fromStatus 更正到 toStatus。
// 409 STATUS_CHANGED 表示业务系统自己已推进过该记录，视为成功（以它的为准）。
func (c *BizClient) Resolve(ctx context.Context, id, fromStatus, toStatus, providerMsgID string) error {
	payload := map[string]string{
		"fromStatus": fromStatus,
		"toStatus":   toStatus,
	}
	if providerMsgID != "" {
		payload["providerMsgId"] = providerMsgID
	}
	buf, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.BaseURL+"/internal/send-records/"+url.PathEscape(id)+"/resolve", bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusConflict {
		return nil
	}
	return fmt.Errorf("biz resolve %s: status %d", id, resp.StatusCode)
}
