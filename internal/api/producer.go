package api

import (
	"database/sql"
	"kpokjn/domain"
	"kpokjn/internal/data"
	"kpokjn/internal/logx"
	"sync"
	"time"

	"github.com/google/uuid"
)

// stupid producer
type ApiProducer struct {
	mu      sync.Mutex
	Tickers map[string]*domain.Ticker

	MaxDuration int //sec
	SubmitFunc  func(job *domain.ApiJob)
	FeedbackCh  chan *domain.ApiResult
}

func GetLastFetch(w *data.Writer, ticker string, fallbackDays int) time.Time {
	var maxTimestamp sql.NullInt64

	err := w.QueryRow("SELECT MAX(timestamp) FROM ohlcv WHERE ticker = ?", ticker).Scan(&maxTimestamp)
	if err != nil && err != sql.ErrNoRows {
		logx.Errorf("failed to fetch max timestamp for %s: %v", ticker, err)
	}

	if maxTimestamp.Valid {
		return time.Unix(maxTimestamp.Int64, 0)
	}

	return time.Now().AddDate(0, 0, -fallbackDays)
}

// tmp
func (APM *ApiManager) NewProducer(tickers []string) *ApiProducer {

	//check dup
	//get tickre and lastFetch from sql
	producer := &ApiProducer{
		Tickers: map[string]*domain.Ticker{
			"TSLA": {
				Ticker:    "TSLA",
				LastFetch: time.Now().Add(-time.Minute * 60 * 24 * 300),
			},
		},
		MaxDuration: 60 * 5,
		SubmitFunc: func(job *domain.ApiJob) {
			APM.Submit(job, 5)
		},
		FeedbackCh: make(chan *domain.ApiResult),
	}

	return producer
}

func (Ap *ApiProducer) Run() {

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			go func() {
				Ap.mu.Lock()
				defer Ap.mu.Unlock()

				for _, v := range Ap.Tickers {
					if time.Since(v.LastFetch) > time.Duration(Ap.MaxDuration)*time.Second {
						go Ap.SubmitFunc(
							&domain.ApiJob{
								ID:        uuid.New().String(),
								Start:     v.LastFetch,
								End:       time.Now().Add(-time.Minute * 60),
								Ticker:    v.Ticker,
								TimeFrame: "15min",
								Priority:  1,
								Feedback:  Ap.FeedbackCh,
								Limit:     200,
							},
						)
					}
				}
			}()
		case result := <-Ap.FeedbackCh:
			Ap.mu.Lock()
			Ap.Tickers[result.ID].LastFetch = time.Now()

			Ap.mu.Unlock()
		}
	}
}
