// mock 服务商入口。环境变量：
//
//	MOCK_PROV_PORT       监听端口（默认 9002）
//	MOCK_PROV_429_EVERY  每 N 次请求返回一次 429（0=关闭）
//	MOCK_PROV_503_EVERY  每 N 次请求返回一次 503（0=关闭）
//	MOCK_PROV_REVOKED    启动即注销的 sender，逗号分隔（对其查询永远 410）
//	MOCK_PROV_DROP_RATE  提交后放弃概率 0~1（默认 0.1）
package main

import (
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"stability-service/internal/mockprov"
)

func main() {
	cfg := mockprov.Config{DropRate: -1} // 环境变量未设置时用默认放弃率 0.1
	cfg.Fail429Every, _ = strconv.Atoi(os.Getenv("MOCK_PROV_429_EVERY"))
	cfg.Fail503Every, _ = strconv.Atoi(os.Getenv("MOCK_PROV_503_EVERY"))
	if v := os.Getenv("MOCK_PROV_DROP_RATE"); v != "" {
		cfg.DropRate, _ = strconv.ParseFloat(v, 64)
	}
	if v := os.Getenv("MOCK_PROV_REVOKED"); v != "" {
		cfg.Revoked = strings.Split(v, ",")
	}
	port := os.Getenv("MOCK_PROV_PORT")
	if port == "" {
		port = "9002"
	}
	log.Printf("mock provider listening on :%s (429_every=%d 503_every=%d revoked=%v)",
		port, cfg.Fail429Every, cfg.Fail503Every, cfg.Revoked)
	log.Fatal(http.ListenAndServe(":"+port, mockprov.New(cfg).Handler()))
}
