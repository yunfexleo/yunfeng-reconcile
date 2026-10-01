// mock 业务系统入口。环境变量：
//
//	MOCK_BIZ_PORT            监听端口（默认 9001）
//	MOCK_BIZ_PROV_URL        mock 服务商地址（默认 http://localhost:9002）
//	MOCK_BIZ_SENDERS         发送方数量（默认 20）
//	MOCK_BIZ_SEED_RECORDS    预置记录数（默认 200）
//	MOCK_BIZ_503_EVERY       每 N 次请求返回一次 503（0=关闭）
//	MOCK_BIZ_SLOW            =true 时所有请求 hang 30s
//	MOCK_BIZ_LATE_COMMIT     =true 时 updatedAt 取事务开始值、30s 后才可见
//	MOCK_BIZ_AUTO_SUBMIT_MS  每 N 毫秒自动提交一条新消息（0=关闭）
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	"stability-service/internal/mockbiz"
)

func main() {
	cfg := mockbiz.Config{
		ProviderURL:  getenv("MOCK_BIZ_PROV_URL", "http://localhost:9002"),
		Slow:         os.Getenv("MOCK_BIZ_SLOW") == "true",
		LateCommit:   os.Getenv("MOCK_BIZ_LATE_COMMIT") == "true",
		SeedRecords:  -1, // 环境变量未设置时用默认值 200
	}
	cfg.Senders, _ = strconv.Atoi(os.Getenv("MOCK_BIZ_SENDERS"))
	if v := os.Getenv("MOCK_BIZ_SEED_RECORDS"); v != "" {
		cfg.SeedRecords, _ = strconv.Atoi(v)
	}
	cfg.Fail503Every, _ = strconv.Atoi(os.Getenv("MOCK_BIZ_503_EVERY"))
	port := getenv("MOCK_BIZ_PORT", "9001")

	srv := mockbiz.New(cfg)
	senders := cfg.Senders
	if senders <= 0 {
		senders = 20 // 与 mockbiz.New 的默认值保持一致（New 内部按值应用默认）
	}
	ctx := context.Background()
	srv.StartBackground(ctx)

	if every, _ := strconv.Atoi(os.Getenv("MOCK_BIZ_AUTO_SUBMIT_MS")); every > 0 {
		go func() {
			tick := time.NewTicker(time.Duration(every) * time.Millisecond)
			for range tick.C {
				sender := fmt.Sprintf("sender-%d", rand.Intn(senders))
				srv.Submit(sender, fmt.Sprintf("auto-%d@example.com", rand.Int()))
			}
		}()
	}

	log.Printf("mock biz listening on :%s (slow=%v late_commit=%v 503_every=%d)",
		port, cfg.Slow, cfg.LateCommit, cfg.Fail503Every)
	log.Fatal(http.ListenAndServe(":"+port, srv.Handler()))
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
