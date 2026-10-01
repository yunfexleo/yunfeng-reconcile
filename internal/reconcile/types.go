package reconcile

import "time"

// 业务系统发送记录的状态。
const (
	StatusPending   = "pending"
	StatusAccepted  = "accepted"
	StatusSent      = "sent"
	StatusFailed    = "failed"
	StatusUnknown   = "unknown"
)

// 对账结论状态。
const (
	StateConsistent   = "consistent"
	StateInconsistent = "inconsistent"
	StateIncomplete   = "incomplete"
)

// 不一致/异常明细的分类。
const (
	// 业务系统记为 sent，但服务商查不到对应消息 —— 真不一致。
	IssueSentButNoProviderMsg = "SENT_BUT_NO_PROVIDER_MSG"
	// 业务系统记为 failed，但服务商实际发出了 —— 真不一致。
	IssueFailedButProviderHasMsg = "FAILED_BUT_PROVIDER_HAS_MSG"
	// 服务商有消息，但窗口内没有对应的发送记录 —— 真不一致。
	IssueMsgWithoutRecord = "PROVIDER_MSG_WITHOUT_RECORD"
	// unknown 记录被重新提交导致的重复消息 —— 预期行为， informational。
	IssueDuplicateSend = "RESUBMISSION_DUPLICATE"
	// requestId 为 null：发送方绕过业务系统直发 —— 单独报告，不算对账不一致。
	IssueDirectSend = "PROVIDER_DIRECT_SEND"
)

// 时序边界（题目给定）：
//   - 提交后最多 30s，服务商把消息发出或放弃；
//   - 发出后最多 60s 才能在查询接口查到；
//   - 两侧时钟最多相差 5s。
//
// 因此：submittedAt 超过 95s 的记录，其结果在服务商侧已经可以确定。
const DetermineHorizon = 95 * time.Second

// ClockSkew 用于给服务商时间窗前后各留 5s 重叠，避免时钟偏差造成漏查。
const ClockSkew = 5 * time.Second

// Record 是业务系统发送记录的本地投影（字段与 2.1 节一致）。
type Record struct {
	ID            string
	SenderID      string
	RequestID     string
	Status        string
	ProviderMsgID string
	SubmittedAt   time.Time // pending 时为零值
	UpdatedAt     time.Time
}

// ProviderMessage 是服务商实际发出的消息（2.2 节）。
// RequestID 为 nil 表示发送方绕过业务系统直发。
type ProviderMessage struct {
	ProviderMsgID string
	RequestID     *string
	Recipient     string
	SentAt        time.Time
}

// Issue 是一条不一致/异常明细。
type Issue struct {
	Type          string `json:"type"`
	RecordID      string `json:"recordId,omitempty"`
	RequestID     string `json:"requestId,omitempty"`
	ProviderMsgID string `json:"providerMsgId,omitempty"`
	Detail        string `json:"detail"`
}

// Result 是 /api/reconciliation 的应答。
type Result struct {
	State  string  `json:"state"`
	Issues []Issue `json:"issues"`
}

// affectsState 报告该 issue 是否应该把结论从 consistent 打成 inconsistent。
// 重复提交（重试语义内）和发送方直发都不算对账不一致。
func (i Issue) affectsState() bool {
	switch i.Type {
	case IssueDuplicateSend, IssueDirectSend:
		return false
	}
	return true
}

// ComputeState 由「数据是否采齐 + issues」得出最终结论。
// 只要存在采集缺口（窗口未过可见性时延、429/503、发送方被注销），
// 就只能回答 incomplete —— 缺口 ≠ 一致。
func ComputeState(hasGap bool, issues []Issue) Result {
	r := Result{State: StateConsistent, Issues: issues}
	if hasGap {
		r.State = StateIncomplete
		return r
	}
	for _, iss := range issues {
		if iss.affectsState() {
			r.State = StateInconsistent
			return r
		}
	}
	return r
}
