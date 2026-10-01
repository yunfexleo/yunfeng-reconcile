// mock 告警 webhook 入口：收到的告警打印到 stdout 并追加到 alerts.log。
//
//	MOCK_ALERT_PORT      监听端口（默认 9003）
//	MOCK_ALERT_429_EVERY 每 N 次请求返回一次 429（0=关闭）
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

func main() {
	every429, _ := strconv.Atoi(os.Getenv("MOCK_ALERT_429_EVERY"))
	port := os.Getenv("MOCK_ALERT_PORT")
	if port == "" {
		port = "9003"
	}
	logFile, err := os.OpenFile("alerts.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	var n atomic.Int32
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if every429 > 0 && int(n.Add(1))%every429 == 0 {
			w.WriteHeader(http.StatusTooManyRequests)
			json.NewEncoder(w).Encode(map[string]int{"retryAfterSeconds": 1})
			return
		}
		body, _ := io.ReadAll(r.Body)
		line := fmt.Sprintf("%s %s", time.Now().UTC().Format(time.RFC3339), body)
		fmt.Println("[alert]", line)
		fmt.Fprintln(logFile, line)
		w.WriteHeader(http.StatusOK)
	})
	log.Printf("mock alert webhook listening on :%s (log=alerts.log 429_every=%d)", port, every429)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
