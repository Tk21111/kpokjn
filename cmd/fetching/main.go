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
	// producer1 := manager.NewProducer([]s)
	// go producer1.Run()
	// fmt.Println("producer1 run")

	cfg := &domain.Client{
		Cfg:    config.Load(),
		Client: &http.Client{},
	}
	manager := api.NewApiManager(ctx, writer, cfg.Cfg, 10)
	fmt.Println("manager create")
	producer := manager.NewProducer([]string{"TSLA"})
	go producer.Run()
	fmt.Println("producer run")
	consumeer := manager.NewConsumer(cfg, 10, 200)
	consumeer.Run()
	fmt.Println("consummer run")
}

// Unused but keeps imports clean
var _ = os.Args
