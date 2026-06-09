package config

import (
	"os"

	"github.com/joho/godotenv"
	"kpokjn/internal/logx"
)

type Config struct {
	AlpacaAPIKey  string
	AlpacaSecret  string
	AlpacaBaseURL string
	DiscordWebhook string
}

func Load() *Config {
	// Load .env file if present (silently ignore if missing)
	if err := godotenv.Load(".env"); err != nil {
		logx.Warnf("No .env file found, using system env vars: %v", err)
	} else {
		logx.Info("Loaded environment from .env")
	}

	return &Config{
		AlpacaAPIKey:   os.Getenv("ALPACA_API_KEY"),
		AlpacaSecret:   os.Getenv("ALPACA_SECRET_KEY"),
		AlpacaBaseURL:  getEnv("ALPACA_BASE_URL", "https://data.alpaca.markets/v2"),
		DiscordWebhook: os.Getenv("DISCORD_WEBHOOK_URL"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
