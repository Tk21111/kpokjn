package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"kpokjn/internal/alpaca"
	"kpokjn/internal/backfill"
	"kpokjn/internal/config"
	"kpokjn/internal/data"
	"kpokjn/internal/logx"
)

func main() {
	// Initialize logger first
	logx.Init("logs")
	defer logx.Logger().Sync()

	logx.Info("=== Fetching Engine Starting ===")

	// Load configuration (M13)
	cfg := config.Load()
	if err := cfg.Validate(); err != nil {
		logx.Fatalf("Config validation failed: %v", err)
	}

	logx.Infof("Config loaded: tickers=%d workers=%d db=%s",
		len(cfg.Tickers), cfg.WorkerCount, cfg.DBPath)
	logx.Infof("Tickers: %v", cfg.Tickers)

	// Initialize SQLite writer (M3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	writer, err := data.NewWriter(ctx, cfg.DBPath)
	if err != nil {
		logx.Fatalf("Failed to initialize SQLite writer: %v", err)
	}
	defer writer.Close()

	// Initialize Alpaca client
	client := alpaca.NewClient(cfg)

	// Run backfill (M10) — BLOCKING
	startTime := time.Now()
	worker := backfill.NewWorker(cfg, client, writer)
	if err := worker.Run(); err != nil {
		logx.Fatalf("Backfill failed: %v", err)
	}
	elapsed := time.Since(startTime)

	logx.Infof("=== Fetching Complete ===")
	logx.Infof("Elapsed: %v", elapsed)

	// Print summary
	printSummary(writer, cfg.Tickers)

	fmt.Println("\nDone. Run again to verify idempotency (should skip already-backfilled tickers).")
}

func printSummary(writer *data.Writer, tickers []string) {
	fmt.Println("\n=== Backfill Summary ===")
	fmt.Printf("%-8s %10s %12s\n", "TICKER", "BARS", "STATUS")
	fmt.Println("-----------------------------------")

	for _, ticker := range tickers {
		row := writer.QueryRow("SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM ohlcv WHERE ticker = ?", ticker)
		var count int
		var minTs, maxTs int64
		if err := row.Scan(&count, &minTs, &maxTs); err != nil {
			fmt.Printf("%-8s %10s %12s\n", ticker, "?", "ERROR")
			continue
		}

		status := "OK"
		if count == 0 {
			status = "EMPTY"
		}

		minStr := time.Unix(minTs, 0).Format("2006-01-02")
		maxStr := time.Unix(maxTs, 0).Format("2006-01-02")

		fmt.Printf("%-8s %10d %12s  (%s → %s)\n", ticker, count, status, minStr, maxStr)
	}

	// Check bootstrap_state
	fmt.Println("\n=== Bootstrap State ===")
	rows, err := writer.Query("SELECT ticker, backfill_done, oldest_ts FROM bootstrap_state ORDER BY ticker")
	if err != nil {
		logx.Warnf("Failed to query bootstrap_state: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var ticker string
		var done int
		var oldestTs int64
		if err := rows.Scan(&ticker, &done, &oldestTs); err != nil {
			continue
		}
		status := "PENDING"
		if done == 1 {
			status = "DONE"
		}
		oldest := time.Unix(oldestTs, 0).Format("2006-01-02")
		fmt.Printf("  %-8s  backfill=%-5s  oldest=%s\n", ticker, status, oldest)
	}
}

// Unused but keeps imports clean
var _ = os.Args
