package reconcile

import (
	"testing"
	"time"
)

func strptr(s string) *string { return &s }

func hasIssue(issues []Issue, typ, requestID string) bool {
	for _, iss := range issues {
		if iss.Type == typ && (requestID == "" || iss.RequestID == requestID) {
			return true
		}
	}
	return false
}

// 测试 1：对账匹配的核心语义 —— 重提交重复不算不一致、查无记录/状态矛盾才算。
func TestMatch_DuplicatesUnmatchedAndDirectSend(t *testing.T) {
	records := []Record{
		{ID: "r-sent", RequestID: "q1", Status: StatusSent},   // 服务商有两条 q1（unknown 重提交过一次）→ 不报错
		{ID: "r-failed", RequestID: "q2", Status: StatusFailed}, // 服务商确实没发 → 不报错
		{ID: "r-lost", RequestID: "q6", Status: StatusSent},     // 记为 sent 但服务商查无 → 不一致
		{ID: "r-bad", RequestID: "q3", Status: StatusFailed},    // 记为 failed 但服务商发了 → 不一致
	}
	msgs := []ProviderMessage{
		{ProviderMsgID: "m1", RequestID: strptr("q1"), SentAt: time.Now()},
		{ProviderMsgID: "m2", RequestID: strptr("q1"), SentAt: time.Now()}, // 重提交产生的重复
		{ProviderMsgID: "m3", RequestID: strptr("q3"), SentAt: time.Now()}, // 与 r-bad 矛盾
		{ProviderMsgID: "m4", RequestID: strptr("q4"), SentAt: time.Now()}, // 无任何记录对应
		{ProviderMsgID: "m5", RequestID: nil, SentAt: time.Now()},          // 发送方直发
	}

	issues := Match(records, msgs)
	result := ComputeState(false, issues)

	// 断言 1：存在且仅存在两类真不一致。
	var lost, bad, unmatched bool
	for _, iss := range issues {
		switch iss.Type {
		case IssueSentButNoProviderMsg:
			lost = lost || iss.RecordID == "r-lost"
		case IssueFailedButProviderHasMsg:
			bad = bad || iss.RecordID == "r-bad"
		case IssueMsgWithoutRecord:
			unmatched = unmatched || iss.RequestID == "q4"
		}
	}
	if !lost || !bad || !unmatched {
		t.Fatalf("missing expected inconsistencies: lost=%v bad=%v unmatched=%v, issues=%+v", lost, bad, unmatched, issues)
	}
	if result.State != StateInconsistent {
		t.Fatalf("state = %s, want %s", result.State, StateInconsistent)
	}

	// 断言 2：重提交重复与直发被单独报告，且【不】把结论打成 inconsistent。
	if !hasIssue(issues, IssueDuplicateSend, "q1") {
		t.Fatalf("expected RESUBMISSION_DUPLICATE for q1, issues=%+v", issues)
	}
	if !hasIssue(issues, IssueDirectSend, "") {
		t.Fatalf("expected PROVIDER_DIRECT_SEND, issues=%+v", issues)
	}
	// 只有 informational issue（无真不一致）时，结论必须是 consistent。
	onlyInfos := []Issue{
		{Type: IssueDuplicateSend, RequestID: "q1"},
		{Type: IssueDirectSend},
	}
	if got := ComputeState(false, onlyInfos); got.State != StateConsistent {
		t.Fatalf("info issues must not change state; got %s", got.State)
	}

	// 断言 3：没有采集缺口时，完全一致 → consistent。
	ok := ComputeState(false, Match(
		[]Record{{ID: "r1", RequestID: "q1", Status: StatusSent}},
		[]ProviderMessage{{ProviderMsgID: "m1", RequestID: strptr("q1"), SentAt: time.Now()}},
	))
	if ok.State != StateConsistent {
		t.Fatalf("state = %s, want %s", ok.State, StateConsistent)
	}

	// 断言 4：有采集缺口（429/503/窗口未过可见性时延）时，任何问题都只能回答 incomplete。
	gap := ComputeState(true, issues)
	if gap.State != StateIncomplete {
		t.Fatalf("state = %s, want %s (gap must dominate)", gap.State, StateIncomplete)
	}
}

// 测试 2：状态判定 —— 判定时延用最近一次 submittedAt，且尊重 95s 可见性窗口。
func TestDeterminableAndDetermine_UsesLatestSubmittedAt(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

	// 刚提交 30s：服务商最多还要 30s 才决定发不发、发出后最多 60s 才能查到 → 不能判定。
	fresh := Record{ID: "r1", RequestID: "q1", Status: StatusAccepted, SubmittedAt: now.Add(-30 * time.Second)}
	if Determinable(fresh, now, DetermineHorizon) {
		t.Fatal("record submitted 30s ago must not be determinable")
	}

	// 已越过 95s 判定时延：服务商有消息 → sent 并带回 providerMsgId。
	decided := Record{ID: "r2", RequestID: "q2", Status: StatusUnknown, SubmittedAt: now.Add(-120 * time.Second)}
	if !Determinable(decided, now, DetermineHorizon) {
		t.Fatal("record submitted 120s ago must be determinable")
	}
	toStatus, msgID := Determine(decided, []ProviderMessage{
		{ProviderMsgID: "m9", RequestID: strptr("q2"), SentAt: decided.SubmittedAt.Add(10 * time.Second)},
	})
	if toStatus != StatusSent || msgID != "m9" {
		t.Fatalf("Determine = (%s, %s), want (sent, m9)", toStatus, msgID)
	}

	// 越过时延但服务商查无 → failed。
	toStatus, msgID = Determine(decided, nil)
	if toStatus != StatusFailed || msgID != "" {
		t.Fatalf("Determine = (%s, %s), want (failed, empty)", toStatus, msgID)
	}

	// 关键陷阱：unknown 记录 10 分钟内会被重新提交并刷新 submittedAt。
	// 必须按【最新】submittedAt 判断，否则会跟一条活着的记录赛跑、把还在途中的提交误判为 failed。
	resubmitted := Record{ID: "r3", RequestID: "q3", Status: StatusUnknown, SubmittedAt: now.Add(-20 * time.Second)}
	if Determinable(resubmitted, now, DetermineHorizon) {
		t.Fatal("resubmitted 20s ago: must wait for the latest submission, not the original one")
	}
}
