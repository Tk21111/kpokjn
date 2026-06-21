package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"kpokjn/domain"
	"kpokjn/internal/alpaca"
	"kpokjn/internal/data"
)

func FetchAndWrite(client *domain.Client, writer *data.Writer, cfg *domain.ApiJob, onPageToken func(string)) error {

	result, pageToken, error := alpaca.GetAllBars(client, cfg)
	if error != nil {
		return fmt.Errorf("fetch api fail")
	}

	if len(result) > 0 {
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
				cfg.Ticker, bar.Timestamp, bar.Open, bar.Close, bar.Low, bar.Close, bar.Volume,
			)
		}
	}

	if pageToken != "" {
		onPageToken(pageToken)
	}

	return nil

}

// Ensure imports are used
var _ = json.Decoder{}
var _ = http.StatusOK
