package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"kpokjn/domain"
	"kpokjn/internal/alpaca"
	"kpokjn/internal/data"
	"kpokjn/internal/logx"
)

func FetchAndWrite(client *domain.Client, writer *data.Writer, cfg *domain.ApiJob, onPageToken func(*domain.ApiJob, string), onResult func([]domain.Bar, *domain.ApiJob)) error {

	result, pageToken, error := alpaca.GetAllBars(client, cfg)
	if error != nil {
		return fmt.Errorf("fetch api fail")
	}

	if len(result) > 0 {
		//process
		if time.Since(result[len(result)-1].Timestamp) < time.Hour {
			onResult(result, cfg)
		}

		//save to db
		for _, bar := range result {
			writer.Submit(
				`INSERT INTO ohlcv (ticker, timestamp, open, high, low, close, volume) 
				VALUES (?, ?, ?, ?, ?, ?, ?) 
				ON CONFLICT(ticker, timestamp) DO UPDATE SET 
					open=excluded.open, 
					high=excluded.high, 
					low=excluded.low, 
					close=excluded.close, 
					volume=excluded.volume
				`,
				cfg.Ticker, bar.Timestamp.Unix(), bar.Open, bar.High, bar.Low, bar.Close, bar.Volume,
			)
		}
	}

	if pageToken != "" {
		logx.Info("Fetching nextPageToken")
		onPageToken(cfg, pageToken)
	}

	return nil

}

// Ensure imports are used
var _ = json.Decoder{}
var _ = http.StatusOK
