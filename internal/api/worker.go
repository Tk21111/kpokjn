package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"kpokjn/domain"
	"kpokjn/internal/alpaca"
	"kpokjn/internal/data"
)

// OHLCV represents a single candlestick bar stored in SQLite.

// NewWorker creates a new backfill worker.
func NewWorker(cfg *domain.ApiJob, client *alpaca.Client, writer *data.Writer) *domain.Worker {
	return &domain.Worker{
		Cfg:    cfg,
		Client: client,
		Writer: writer,
	}
}

func FetchAndWrite(client *alpaca.Client, writer *data.Writer, cfg *domain.ApiJob, onPageToken func(string)) error {

	result, pageToken, error := client.GetAllBars(cfg.Ticker, cfg.TimeFrame, cfg.Start, cfg.End, cfg.Limit, cfg.NextPageToken)
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
