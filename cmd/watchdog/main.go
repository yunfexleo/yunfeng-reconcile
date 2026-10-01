// 看门狗（dead man's switch）：独立于稳定性服务运行，定时探测它的 /health。
// 你的服务自己挂了时它发不出任何告警，所以必须有外部进程知道 —— 就是本进程。
//
//	WATCHDOG_TARGET       被探测服务的地址（默认 http://localhost:8080）
//	WATCHDOG_WEBHOOK      告警 webhook（默认 http://localhost:9003）
//	WATCHDOG_INTERVAL_MS  探测间隔（默认 15000）
//	WATCHDOG_THRESHOLD    连续失败多少次告警（默认 2）
package main

import (
	"context"
	"net/http"
	"os"
	"strconv"
	"time"

	"stability-service/internal/alert"
)

func main() {
	target := getenv("WATCHDOG_TARGET", "http://localhost:8080")
	webhook := getenv("WATCHDOG_WEBHOOK", "http://localhost:9003")
	interval, _ := strconv.Atoi(getenv("WATCHDOG_INTERVAL_MS", "15000"))
	threshold, _ := strconv.Atoi(getenv("WATCHDOG_THRESHOLD", "2"))

	a := alert.New(webhook)
	client := &http.Client{Timeout: 3 * time.Second}
	fails := 0
	alerted := false

	for {
		_, err := client.Get(target + "/health")
		ok := err == nil
		if ok {
			fails = 0
			if alerted {
				alerted = false
				a.Resolve("stability-service-down")
				_ = a.Send(context.Background(), alert.Event{
					Key:      "stability-service-down",
					Title:    "稳定性服务已恢复",
					Severity: alert.Info,
				})
			}
		} else {
			fails++
			if !alerted && fails >= threshold {
				alerted = true
				_ = a.Send(context.Background(), alert.Event{
					Key:      "stability-service-down",
					Title:    "稳定性服务失联",
					Text:     "看门狗连续探测失败: " + err.Error(),
					Severity: alert.Critical,
				})
			}
		}
		time.Sleep(time.Duration(interval) * time.Millisecond)
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
