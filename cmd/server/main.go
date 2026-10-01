package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"
)

// 最简骨架：仅提供 /health，验证 PORT 环境变量配置与编译运行。
// 对账与巡检逻辑在此之上逐步扩展。
func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	startedAt := time.Now().UTC()

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"status":    "ok",
			"startedAt": startedAt.Format(time.RFC3339),
		})
	})

	log.Printf("listening on :%s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}
