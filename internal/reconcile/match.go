package reconcile

import "time"

// Match 把窗口内的发送记录与服务商消息做多重集合匹配。
//
// 关键语义：
//   - 服务商不按 requestId 去重，unknown 记录可能被重新提交并发出多条，
//     所以按 requestId 分组、每条记录消费一条消息，剩余同 requestId 的消息
//     归为「重提交重复」（预期行为，不是不一致）；
//   - requestId 为 null 的消息是发送方绕过业务系统直发，单独报告；
//   - 窗口内没有对应记录的 requestId 归为 PROVIDER_MSG_WITHOUT_RECORD
//     （可能是记录已滑出同步窗口，值班需人工确认）。
func Match(records []Record, msgs []ProviderMessage) []Issue {
	var issues []Issue

	// 1. 服务商消息按 requestId 分组（null 单独收集）。
	byRequestID := map[string][]ProviderMessage{}
	var directSends []ProviderMessage
	for _, m := range msgs {
		if m.RequestID == nil {
			directSends = append(directSends, m)
			continue
		}
		byRequestID[*m.RequestID] = append(byRequestID[*m.RequestID], m)
	}

	// 2. 每条记录消费一条同 requestId 的消息。
	knownRequestID := map[string]bool{}
	for _, rec := range records {
		knownRequestID[rec.RequestID] = true
		bucket := byRequestID[rec.RequestID]
		switch rec.Status {
		case StatusSent:
			if len(bucket) == 0 {
				issues = append(issues, Issue{
					Type:      IssueSentButNoProviderMsg,
					RecordID:  rec.ID,
					RequestID: rec.RequestID,
					Detail:    "record is sent but provider has no message with this requestId",
				})
				continue
			}
			byRequestID[rec.RequestID] = bucket[1:] // 消费一条
		case StatusFailed:
			if len(bucket) > 0 {
				byRequestID[rec.RequestID] = bucket[1:]
				issues = append(issues, Issue{
					Type:          IssueFailedButProviderHasMsg,
					RecordID:      rec.ID,
					RequestID:     rec.RequestID,
					ProviderMsgID: bucket[0].ProviderMsgID,
					Detail:        "record is failed but provider actually sent a message",
				})
			}
		}
		// accepted / unknown：结果尚未（或刚刚）确定，不参与缺失判定。
	}

	// 3. 剩余消息按是否有对应记录分类。
	for requestID, leftover := range byRequestID {
		for _, m := range leftover {
			iss := Issue{
				RequestID:     requestID,
				ProviderMsgID: m.ProviderMsgID,
			}
			if knownRequestID[requestID] {
				iss.Type = IssueDuplicateSend
				iss.Detail = "duplicate message from resubmission of an unknown record (expected)"
			} else {
				iss.Type = IssueMsgWithoutRecord
				iss.Detail = "provider message has no matching send record in window"
			}
			issues = append(issues, iss)
		}
	}

	// 4. 直发消息单独报告。
	for _, m := range directSends {
		issues = append(issues, Issue{
			Type:          IssueDirectSend,
			ProviderMsgID: m.ProviderMsgID,
			Detail:        "message sent directly by sender, bypassing the business system",
		})
	}
	return issues
}

// Determinable 判断一条 accepted/unknown 记录此刻能否根据服务商侧情况确定结果。
//
// 必须用【最近一次】submittedAt 判定：unknown 记录 10 分钟内可能被重新提交，
// 每次重提都会刷新 submittedAt —— 用旧的提交时间去判定，会和一条活着的记录赛跑。
func Determinable(rec Record, now time.Time) bool {
	if rec.Status != StatusAccepted && rec.Status != StatusUnknown {
		return false
	}
	if rec.SubmittedAt.IsZero() {
		return false
	}
	return now.Sub(rec.SubmittedAt) >= DetermineHorizon
}

// Determine 返回该记录应更正到的状态：
// 窗口内查到同 requestId 的消息 → sent（带上 providerMsgId），否则 → failed。
// 调用方必须保证传入的是该记录 submittedAt 附近 ±ClockSkew..DetermineHorizon 的消息。
func Determine(rec Record, nearby []ProviderMessage) (toStatus, providerMsgID string) {
	for _, m := range nearby {
		if m.RequestID != nil && *m.RequestID == rec.RequestID {
			return StatusSent, m.ProviderMsgID
		}
	}
	return StatusFailed, ""
}
