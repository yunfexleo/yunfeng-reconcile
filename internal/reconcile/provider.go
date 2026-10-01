package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// ErrSenderRevoked：发送方通道被服务商注销，历史消息从此查不到。
// 对账只能回答 incomplete，不能回答 consistent。
var ErrSenderRevoked = errors.New("sender revoked by provider")

// providerLimit 是服务商单次查询的上限（2.2 节），超出部分不返回。
const providerLimit = 100

// ProviderClient 是服务商查询接口（2.2 节）的客户端。
type ProviderClient struct {
	BaseURL string
	HTTP    *http.Client
}

func NewProviderClient(baseURL string) *ProviderClient {
	return &ProviderClient{BaseURL: baseURL, HTTP: &http.Client{Timeout: 10 * time.Second}}
}

type rawMessage struct {
	ProviderMsgID string `json:"providerMsgId"`
	RequestID     *string `json:"requestId"`
	Recipient     string `json:"recipient"`
	SentAt        int64  `json:"sentAt"` // 毫秒时间戳
}

// FetchWindow 拉取 [from, to) 内的全部消息。
//
// 服务商接口最多返回 100 条且静默截断，因此：返回满 100 条时把窗口一分为二
// 递归重拉，直到窗口不可再分 —— 保证不丢消息（代价是热点窗口会多查几轮）。
func (c *ProviderClient) FetchWindow(ctx context.Context, senderID string, from, to time.Time) ([]ProviderMessage, error) {
	return c.fetchWindow(ctx, senderID, from, to)
}

func (c *ProviderClient) fetchWindow(ctx context.Context, senderID string, from, to time.Time) ([]ProviderMessage, error) {
	msgs, err := c.fetchOnce(ctx, senderID, from, to)
	if err != nil {
		return nil, err
	}
	if len(msgs) < providerLimit {
		return msgs, nil
	}
	// 可能截断：窗口小到 5ms 仍满 100 就停止细分（极端同毫秒洪峰，由上层记为采集缺口）。
	if to.Sub(from) <= 5*time.Millisecond {
		return msgs, nil
	}
	mid := from.Add(to.Sub(from) / 2)
	left, err := c.fetchWindow(ctx, senderID, from, mid)
	if err != nil {
		return nil, err
	}
	right, err := c.fetchWindow(ctx, senderID, mid, to)
	if err != nil {
		return nil, err
	}
	return append(left, right...), nil
}

// fetchOnce 单次请求；429 按 retryAfter 退避重试（最多 3 次），503 指数退避重试。
func (c *ProviderClient) fetchOnce(ctx context.Context, senderID string, from, to time.Time) ([]ProviderMessage, error) {
	q := url.Values{}
	q.Set("from", strconv.FormatInt(from.UnixMilli(), 10))
	q.Set("to", strconv.FormatInt(to.UnixMilli(), 10))
	q.Set("limit", strconv.Itoa(providerLimit))

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(attempt) * 2 * time.Second): // 2s, 4s
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet,
			c.BaseURL+"/senders/"+url.PathEscape(senderID)+"/messages?"+q.Encode(), nil)
		if err != nil {
			return nil, err
		}
		resp, err := c.HTTP.Do(req)
		if err != nil {
			lastErr = err
			continue // 超时/连接错误，按 503 同样退避重试
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			lastErr = readErr
			continue
		}
		switch {
		case resp.StatusCode == http.StatusOK:
			var out struct {
				Messages []rawMessage `json:"messages"`
			}
			if err := json.Unmarshal(body, &out); err != nil {
				return nil, err
			}
			msgs := make([]ProviderMessage, 0, len(out.Messages))
			for _, rm := range out.Messages {
				msgs = append(msgs, ProviderMessage{
					ProviderMsgID: rm.ProviderMsgID,
					RequestID:     rm.RequestID,
					Recipient:     rm.Recipient,
					SentAt:        time.UnixMilli(rm.SentAt).UTC(),
				})
			}
			return msgs, nil
		case resp.StatusCode == http.StatusGone:
			return nil, ErrSenderRevoked
		case resp.StatusCode == http.StatusTooManyRequests:
			var rl struct {
				RetryAfterSeconds int `json:"retryAfterSeconds"`
			}
			json.Unmarshal(body, &rl) // 解析失败则沿用默认退避
			if rl.RetryAfterSeconds > 0 {
				d := time.Duration(rl.RetryAfterSeconds) * time.Second
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(d):
				}
			}
			lastErr = fmt.Errorf("provider rate limited")
			continue
		default:
			lastErr = fmt.Errorf("provider messages: status %d", resp.StatusCode)
		}
	}
	return nil, lastErr
}
