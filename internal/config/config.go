package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"kpokjn/internal/logx"

	"github.com/joho/godotenv"
)

// TickerOverride holds per-ticker configuration overrides.
type TickerOverride struct {
	Lookback      int            `json:"lookback"`
	CooldownHours int            `json:"cooldown_hours"`
	Priority      string         `json:"priority"`
	FormulaID     string         `json:"formula_id"`
	Params        map[string]any `json:"params"`
}

// Config holds all runtime configuration for the trading engine.
type Config struct {
	// Alpaca
	AlpacaAPIKey  string
	AlpacaSecret  string
	AlpacaBaseURL string

	// Discord
	DiscordSignalWebhook string
	DiscordErrorWebhook  string

	// Database
	DBPath string

	// Tickers
	Tickers            []string
	TickerOverrides    map[string]TickerOverride
	TickerTimeFallback time.Time

	// Workers
	WorkerCount    int
	EvalTimeoutSec int

	// Signal dedup
	SignalCooldownHours int

	// Backfill
	BackfillRateLimitPerSecond int

	// Logging
	LogDir string
}

// Load reads configuration from .env and environment variables.
func Load() *Config {
	if err := godotenv.Load(".env"); err != nil {
		logx.Warnf("No .env file found, using system env vars: %v", err)
	} else {
		logx.Info("Loaded environment from .env")
	}

	return &Config{
		AlpacaAPIKey:               os.Getenv("ALPACA_API_KEY"),
		AlpacaSecret:               os.Getenv("ALPACA_SECRET_KEY"),
		AlpacaBaseURL:              getEnv("ALPACA_BASE_URL", "https://data.alpaca.markets/v2"),
		DiscordSignalWebhook:       os.Getenv("DISCORD_SIGNAL_WEBHOOK"),
		DiscordErrorWebhook:        os.Getenv("DISCORD_ERROR_WEBHOOK"),
		DBPath:                     getEnv("DB_PATH", "data/engine.db"),
		Tickers:                    parseTickers(getEnv("TICKERS", "")),
		TickerTimeFallback:         time.Date(2023, time.January, 1, 0, 0, 0, 0, time.UTC),
		WorkerCount:                getIntEnv("PY_WORKER_COUNT", 4),
		EvalTimeoutSec:             getIntEnv("EVAL_TIMEOUT_SEC", 30),
		SignalCooldownHours:        getIntEnv("SIGNAL_COOLDOWN_HOURS", 4),
		BackfillRateLimitPerSecond: getIntEnv("BACKFILL_RATE_LIMIT", 2),
		LogDir:                     getEnv("LOG_DIR", "logs"),
	}
}

// Validate checks that all required configuration is present.
func (c *Config) Validate() error {
	if c.AlpacaAPIKey == "" {
		return fmt.Errorf("ALPACA_API_KEY is required")
	}
	if c.AlpacaSecret == "" {
		return fmt.Errorf("ALPACA_SECRET_KEY is required")
	}
	if len(c.Tickers) == 0 {
		return fmt.Errorf("TICKERS is required (comma-separated list)")
	}
	if c.DiscordSignalWebhook == "" {
		logx.Warn("DISCORD_SIGNAL_WEBHOOK not set — signal delivery will be disabled")
	}
	if c.DiscordErrorWebhook == "" {
		logx.Warn("DISCORD_ERROR_WEBHOOK not set — error delivery will be disabled")
	}
	return nil
}

// TickerConfig returns the effective config for a ticker, merging overrides.
func (c *Config) TickerConfig(ticker string) (lookback int, cooldownHours int, formulaID string, params map[string]any) {
	lookback = 720 // default: ~1 month of hourly candles
	cooldownHours = c.SignalCooldownHours
	formulaID = ""
	params = nil

	if override, ok := c.TickerOverrides[ticker]; ok {
		if override.Lookback > 0 {
			lookback = override.Lookback
		}
		if override.CooldownHours > 0 {
			cooldownHours = override.CooldownHours
		}
		if override.FormulaID != "" {
			formulaID = override.FormulaID
		}
		if override.Params != nil {
			params = override.Params
		}
	}

	return
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getIntEnv(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
		logx.Warnf("Invalid int for %s=%q, using default %d", key, v, fallback)
	}
	return fallback
}

func parseTickers(s string) []string {
	if s == "" {
		return nil
	}
	var tickers []string
	for _, t := range strings.Split(s, ",") {
		t = strings.TrimSpace(t)
		if t != "" {
			tickers = append(tickers, strings.ToUpper(t))
		}
	}
	return tickers
}
