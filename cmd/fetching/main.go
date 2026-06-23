package main

import (
	"context"
	"fmt"
	"kpokjn/domain"
	"kpokjn/internal/api"
	"kpokjn/internal/config"
	"kpokjn/internal/data"
	"kpokjn/internal/logx"
	"net/http"
	"os"

	"github.com/joho/godotenv"
)

func main() {

	_ = godotenv.Load(".env")

	logx.Init()
	ctx := context.Background()
	writer, err := data.NewWriter(ctx, "data.db")
	if err != nil {
		panic(err)
	}
	manager := api.NewApiManager(ctx, writer, 10)
	fmt.Println("manager create")
	producer := manager.NewProducer()
	go producer.Run()
	fmt.Println("producer run")
	producer1 := manager.NewProducer()
	go producer1.Run()
	fmt.Println("producer1 run")

	cfg := &domain.Client{
		Cfg: &config.Config{
			AlpacaAPIKey:  os.Getenv("ALPACA_API_KEY"),
			AlpacaSecret:  os.Getenv("ALPACA_SECRET_KEY"),
			AlpacaBaseURL: "https://data.alpaca.markets/v2",
		},
		Client: &http.Client{},
	}
	consumeer := manager.NewConsumer(cfg, 10, 200)
	consumeer.Run()
	fmt.Println("consummer run")
}

// Unused but keeps imports clean
var _ = os.Args
