package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL 驱动（database/sql 接口）

	"stability-service/internal/alert"
	"stability-service/internal/reconcile"
	"stability-service/internal/watch"
)

// 环境变量（题目约定）：PORT / DATABASE_URL / BIZ_URL / PROVIDER_URL / ALERT_WEBHOOK_URL。
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// openStore 按 DATABASE_URL 连接 PostgreSQL 并建表；未配置时退化为内存存储（本地开发用）。
func openStore(ctx context.Context) reconcile.Store {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Println("DATABASE_URL not set, using in-memory store (data lost on restart)")
		return reconcile.NewMemStore()
	}
	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		log.Fatalf("ping database: %v", err)
	}
	store := reconcile.NewSQLStore(db)
	if err := store.EnsureSchema(ctx); err != nil {
		log.Fatalf("ensure schema: %v", err)
	}
	log.Println("using PostgreSQL store")
	return store
}

func main() {
	port := env("PORT", "8080")
	bizURL := os.Getenv("BIZ_URL")
	provURL := os.Getenv("PROVIDER_URL")
	webhookURL := os.Getenv("ALERT_WEBHOOK_URL")
	if bizURL == "" || provURL == "" || webhookURL == "" {
		log.Fatal("BIZ_URL / PROVIDER_URL / ALERT_WEBHOOK_URL are required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	biz := reconcile.NewBizClient(bizURL)
	prov := reconcile.NewProviderClient(provURL)
	alerter := alert.New(webhookURL)
	store := openStore(ctx)
	syncer := reconcile.NewSyncer(biz, prov, store)

	// 巡检引擎：探测业务系统健康/stats/发送方状态，flap 防抖后告警。
	watcher := watch.New(bizURL, alerter, watch.Config{})
	go watcher.Run(ctx)

	// 同步主循环：进程随时可能被强杀，进度全部落在 Store 游标里，重启后续跑。
	go func() {
		tick := time.NewTicker(5 * time.Second)
		defer tick.Stop()
		for {
			if err := syncer.SyncOnce(ctx); err != nil {
				// 业务系统不可达/超时/503：告警（去抖保证不刷屏），下轮重试。
				_ = alerter.Send(ctx, alert.Event{
					Key:      "biz-unreachable",
					Title:    "业务系统不可达",
					Text:     "同步循环访问业务系统失败: " + err.Error(),
					Severity: alert.Critical,
				})
			}
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
	}()

	startedAt := time.Now().UTC()
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"status":    "ok",
			"startedAt": startedAt.Format(time.RFC3339),
			"db":        "ok",
		})
	})
	mux.HandleFunc("/api/reconciliation", syncer.ServeReconciliation)
	// 运维临时静默告警：POST /api/silence {"minutes": 30}；GET 查询剩余静默时间。
	mux.HandleFunc("/api/silence", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			var body struct {
				Minutes int `json:"minutes"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Minutes <= 0 {
				http.Error(w, `{"error":"minutes required"}`, http.StatusBadRequest)
				return
			}
			alerter.SilenceFor(time.Duration(body.Minutes) * time.Minute)
		}
		json.NewEncoder(w).Encode(map[string]any{
			"silencedSecondsRemaining": int(alerter.SilenceRemaining().Seconds()),
		})
	})

	log.Printf("listening on :%s", port)
	srv := &http.Server{Addr: ":" + port, Handler: mux}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
