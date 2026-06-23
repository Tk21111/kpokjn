package main

import (
	"fmt"
	"os"
	"time"

	"kpokjn/internal/alpaca"
	"kpokjn/internal/config"
	"kpokjn/internal/indicator"
	"kpokjn/internal/logx"

	"github.com/joho/godotenv"
)

func main() {
	// Initialize logger before anything else
	_ = godotenv.Load(".env")

	logx.Init()
	defer logx.Logger().Sync()

	logx.Infof("=== Trading Indicator Notifier starting ===")

	cfg := config.Load()

	// Validate required config
	if cfg.AlpacaAPIKey == "" {
		logx.Fatal("ALPACA_API_KEY is not set. Add it to your .env file or environment.")
	}
	if cfg.AlpacaSecret == "" {
		logx.Fatal("ALPACA_SECRET_KEY is not set. Add it to your .env file or environment.")
	}
	logx.Infof("Alpaca API configured (base URL: %s)", cfg.AlpacaBaseURL)

	client := alpaca.NewClient(cfg)

	ticker := "MSFT"
	limit := 250

	logx.Infof("Fetching %d daily bars for %s ...", limit, ticker)

	startFetch := time.Now()
	bars, err := client.GetBars(ticker, limit)
	if err != nil {
		logx.Fatalf("Failed to fetch bars for %s: %v", ticker, err)
	}
	elapsed := time.Since(startFetch)

	logx.Infof("Fetched %d bars in %v", len(bars), elapsed)

	if len(bars) == 0 {
		logx.Fatalf("No bars returned for %s", ticker)
	}

	last := bars[len(bars)-1]
	logx.Infof("Last bar: time=%s O=%.2f H=%.2f L=%.2f C=%.2f V=%d",
		last.Timestamp.Format("2006-01-02 15:04"),
		last.Open, last.High, last.Low, last.Close, last.Volume,
	)

	// Extract close prices
	closes := make([]float64, len(bars))
	for i, b := range bars {
		closes[i] = b.Close
	}

	// Run SMA crossover indicator (fast=10, slow=50)
	result := indicator.SmaCrossover(closes, 10, 50)

	logx.Infof("Indicator result: %s", result)

	// Console output for visibility
	fmt.Println()
	fmt.Println("=== Indicator Output ===")
	fmt.Printf("Ticker:      %s\n", ticker)
	fmt.Printf("Bars loaded: %d\n", len(bars))
	fmt.Printf("Result:      %s\n", result)

	if result.Signal != "HOLD" {
		logx.Infof(">>> SIGNAL DETECTED: %s for %s at price=%.2f <<<", result.Signal, ticker, result.Price)
		fmt.Printf("\n>>> ALERT: %s signal detected for %s! <<<\n", result.Signal, ticker)
	} else {
		logx.Info("No crossover signal at this time.")
		fmt.Println("\n>>> No crossover signal at this time.")
	}

	logx.Info("=== Done ===")
	os.Exit(0)
}
