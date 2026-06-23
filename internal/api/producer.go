package api

import (
	"fmt"
	"kpokjn/domain"
	"sync"
	"time"
)

// stupid producer
type ApiProducer struct {
	mu      sync.Mutex
	Tickers map[string]*domain.Ticker

	MaxDuration int //sec
	SubmitFunc  func(job *domain.ApiJob)
	FeedbackCh  chan *domain.ApiResult
}

// tmp
func (APM *ApiManager) NewProducer() *ApiProducer {
	return &ApiProducer{
		Tickers: map[string]*domain.Ticker{
			"TSLA": {
				Ticker:    "TSLA",
				LastFetch: time.Now().Add(-time.Minute * 60 * 2),
			},
		},
		MaxDuration: 60 * 5,
		SubmitFunc: func(job *domain.ApiJob) {
			APM.Submit(job, 5)
		},
		FeedbackCh: make(chan *domain.ApiResult),
	}
}

func (Ap *ApiProducer) Run() {

	ticker := time.NewTicker(2 * time.Second)
	for {
		select {
		case <-ticker.C:
			go func() {
				Ap.mu.Lock()
				defer Ap.mu.Unlock()

				fmt.Println("Checking stock")
				for _, v := range Ap.Tickers {
					if time.Since(v.LastFetch) > time.Duration(Ap.MaxDuration)*time.Second {
						go Ap.SubmitFunc(
							&domain.ApiJob{
								ID:        v.Ticker,
								Start:     v.LastFetch,
								End:       time.Now().Add(-time.Minute * 60),
								Ticker:    v.Ticker,
								TimeFrame: "15min",
								Priority:  1,
								Feedback:  Ap.FeedbackCh,
								Limit:     2000,
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
