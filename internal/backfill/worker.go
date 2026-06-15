package backfill

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"kpokjn/internal/alpaca"
	"kpokjn/internal/config"
	"kpokjn/internal/data"
	"kpokjn/internal/logx"
)

// OHLCV represents a single candlestick bar stored in SQLite.
type OHLCV struct {
	Ticker    string
	Timestamp int64
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
}

// Worker is the backfill manager (M10).
// It fetches historical OHLCV data from Alpaca and writes it to SQLite
// via the single-writer goroutine (M3).
type Worker struct {
	cfg    *config.Config
	client *alpaca.Client
	writer *data.Writer
}

// NewWorker creates a new backfill worker.
func NewWorker(cfg *config.Config, client *alpaca.Client, writer *data.Writer) *Worker {
	return &Worker{
		cfg:    cfg,
		client: client,
		writer: writer,
	}
}

// Run executes the backfill for all tickers that need it.
// It checks bootstrap_state and only fetches data for tickers with backfill_done=0.
// This is a BLOCKING call — must complete before the pipeline starts.
func (w *Worker) Run() error {
	logx.Info("Backfill Worker: starting...")

	tickers := w.cfg.Tickers
	if len(tickers) == 0 {
		return fmt.Errorf("no tickers configured")
	}

	needsBackfill := 0
	for _, ticker := range tickers {
		done, err := w.isBackfillDone(ticker)
		if err != nil {
			return fmt.Errorf("check bootstrap_state for %s: %w", ticker, err)
		}
		if !done {
			needsBackfill++
		}
	}

	if needsBackfill == 0 {
		logx.Info("Backfill Worker: all tickers already backfilled, skipping")
		return nil
	}

	logx.Infof("Backfill Worker: %d / %d tickers need backfill", needsBackfill, len(tickers))

	var failed []string
	for _, ticker := range tickers {
		done, err := w.isBackfillDone(ticker)
		if err != nil {
			logx.Errorf("Backfill Worker: failed to check %s: %v", ticker, err)
			failed = append(failed, ticker)
			continue
		}
		if done {
			logx.Debugf("Backfill Worker: %s already backfilled, skipping", ticker)
			continue
		}

		if err := w.backfillTicker(ticker); err != nil {
			logx.Errorf("Backfill Worker: failed to backfill %s: %v", ticker, err)
			failed = append(failed, ticker)
		}
	}

	if len(failed) > 0 {
		return fmt.Errorf("backfill failed for %d tickers: %v", len(failed), failed)
	}

	logx.Info("Backfill Worker: all tickers backfilled successfully")
	return nil
}

// backfillTicker fetches 2 years of hourly OHLCV data for a single ticker
// and writes it to SQLite in batches.
func (w *Worker) backfillTicker(ticker string) error {
	logx.Infof("Backfill Worker: fetching 2yr hourly data for %s ...", ticker)

	end := time.Now().UTC()
	start := end.Add(-2 * 365 * 24 * time.Hour)

	// Rate limiter: Alpaca free tier is 200 req/min.
	// We use the configured rate limit (default 2/sec for backfill).
	rateLimit := time.Duration(float64(time.Second) / float64(w.cfg.BackfillRateLimitPerSecond))
	rateTicker := time.NewTicker(rateLimit)
	defer rateTicker.Stop()

	allBars := make([]OHLCV, 0, 17520) // 2yr * 365 * 24 = 17520 hourly bars
	cursor := end
	totalFetched := 0
	pages := 0

	for cursor.After(start) {
		<-rateTicker.C

		bars, nextToken, err := w.client.GetBarsPaged(ticker, "1Hour", cursor, 10000)
		if err != nil {
			logx.Warnf("Backfill Worker: fetch error for %s at %s: %v (will retry)", ticker, cursor.Format(time.RFC3339), err)
			time.Sleep(2 * time.Second)
			continue
		}

		pages++

		if len(bars) == 0 {
			logx.Debugf("Backfill Worker: no more data for %s before %s", ticker, cursor.Format(time.RFC3339))
			break
		}

		for _, b := range bars {
			if b.Timestamp.Unix() >= start.Unix() {
				allBars = append(allBars, OHLCV{
					Ticker:    ticker,
					Timestamp: b.Timestamp.Unix(),
					Open:      b.Open,
					High:      b.High,
					Low:       b.Low,
					Close:     b.Close,
					Volume:    float64(b.Volume),
				})
			}
		}

		totalFetched += len(bars)
		logx.Debugf("Backfill Worker: %s page %d — fetched %d bars (total: %d)", ticker, pages, len(bars), totalFetched)

		// If we got a next page token, use it for pagination.
		// Otherwise, use the oldest bar timestamp as cursor.
		if nextToken != "" {
			cursor = time.Unix(extractOldestTs(bars), 0).UTC()
		} else {
			cursor = time.Unix(extractOldestTs(bars), 0).UTC()
		}

		// If we got fewer bars than the page limit, we've reached the beginning
		if len(bars) < 10000 {
			break
		}
	}

	if len(allBars) == 0 {
		logx.Warnf("Backfill Worker: no bars returned for %s, marking as done anyway", ticker)
	}

	// Write to SQLite in batches of 500
	batchSize := 500
	batches := len(allBars) / batchSize
	if len(allBars)%batchSize != 0 {
		batches++
	}

	logx.Infof("Backfill Worker: writing %d bars for %s in %d batches", len(allBars), ticker, batches)

	for i := 0; i < len(allBars); i += batchSize {
		endIdx := i + batchSize
		if endIdx > len(allBars) {
			endIdx = len(allBars)
		}
		chunk := allBars[i:endIdx]

		reqs := make([]data.WriteRequest, 0, len(chunk)+1)
		for _, bar := range chunk {
			reqs = append(reqs, data.WriteRequest{
				SQL: `INSERT OR REPLACE INTO ohlcv (ticker, timestamp, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)`,
				Args: []any{
					bar.Ticker,
					bar.Timestamp,
					bar.Open,
					bar.High,
					bar.Low,
					bar.Close,
					bar.Volume,
				},
			})
		}
		// Flush sentinel
		reqs = append(reqs, data.WriteRequest{BatchFlush: true})

		if err := w.writer.WriteBatch(reqs); err != nil {
			return fmt.Errorf("write batch %d-%d for %s: %w", i, endIdx, ticker, err)
		}

		logx.Debugf("Backfill Worker: %s written batch %d/%d (%d bars)", ticker, i/batchSize+1, batches, len(chunk))
	}

	// Mark backfill as done
	if err := w.markBackfillDone(ticker, start.Unix()); err != nil {
		return fmt.Errorf("mark backfill done for %s: %w", ticker, err)
	}

	logx.Infof("Backfill Worker: %s complete — %d bars written (%d pages)", ticker, len(allBars), pages)
	return nil
}

// extractOldestTs returns the oldest timestamp from a slice of bars.
func extractOldestTs(bars []alpaca.Bar) int64 {
	var oldest int64
	for i, b := range bars {
		ts := b.Timestamp.Unix()
		if i == 0 || ts < oldest {
			oldest = ts
		}
	}
	return oldest
}

// isBackfillDone checks bootstrap_state for a ticker.
func (w *Worker) isBackfillDone(ticker string) (bool, error) {
	row := w.writer.QueryRow("SELECT backfill_done FROM bootstrap_state WHERE ticker = ?", ticker)
	var done int
	err := row.Scan(&done)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return done == 1, nil
}

// markBackfillDone sets backfill_done=1 for a ticker in bootstrap_state.
func (w *Worker) markBackfillDone(ticker string, oldestTs int64) error {
	return w.writer.Write(
		`INSERT OR REPLACE INTO bootstrap_state (ticker, backfill_done, oldest_ts) VALUES (?, 1, ?)`,
		ticker, oldestTs,
	)
}

// Ensure imports are used
var _ = json.Decoder{}
var _ = http.StatusOK
