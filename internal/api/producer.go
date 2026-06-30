package api

import (
	"database/sql"
	"fmt"
	"kpokjn/domain"
	"kpokjn/internal/config"
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

func GetLastFetch(w *data.Writer, ctf *config.Config, ticker string) time.Time {
	var maxTimestamp sql.NullInt64

	err := w.QueryRow("SELECT MAX(timestamp) FROM ohlcv WHERE ticker = ?", ticker).Scan(&maxTimestamp)
	if err != nil && err != sql.ErrNoRows {
		logx.Errorf("failed to fetch max timestamp for %s: %v", ticker, err)
		return ctf.TickerTimeFallback
	}

	fmt.Println("Time= ===")
	if maxTimestamp.Valid {
		fmt.Println("f")
		fmt.Println(time.Unix(maxTimestamp.Int64, 0))
		return time.Unix(maxTimestamp.Int64, 0)
	}

	fmt.Println(ctf.TickerTimeFallback)
	return ctf.TickerTimeFallback
}

// tmp
func (APM *ApiManager) NewProducer(tickers []string) *ApiProducer {

	//check dup
	//get tickre and lastFetch from sql
	var tickerFormatted = map[string]*domain.Ticker{}

	for _, v := range tickers {
		tickerFormatted[v] = &domain.Ticker{
			Ticker:    v,
			LastFetch: GetLastFetch(APM.Writer, APM.Cfg, v),
		}
	}
	producer := &ApiProducer{
		Tickers:     tickerFormatted,
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
					end := time.Now().Add(-time.Minute * 60)
					if time.Since(v.LastFetch) > time.Duration(Ap.MaxDuration)*time.Second && Ap.Tickers[v.Ticker].LastFetch.Unix() < end.Unix() {
						go Ap.SubmitFunc(
							&domain.ApiJob{
								ID:        uuid.New().String(),
								Start:     v.LastFetch,
								End:       end,
								Ticker:    v.Ticker,
								TimeFrame: "15min",
								Priority:  1,
								Feedback:  Ap.FeedbackCh,
								Limit:     10000, //max limit who tf do this shit
							},
						)
					}
				}
			}()
		case result := <-Ap.FeedbackCh:
			Ap.mu.Lock()
			Ap.Tickers[result.Status.Ticker].LastFetch = time.Now()

			Ap.mu.Unlock()
		}
	}
}
