package domain

import (
	"kpokjn/internal/alpaca"
	"sync"
	"time"
)

type ApiManager struct {
	Gate        sync.RWMutex
	Closed      bool
	CloseSignal chan struct{}

	Client    *alpaca.Client
	RateLimit int // per sec
	ApiCh     chan *ApiJob
}

type ApiJob struct {
	Start         time.Time
	End           time.Time
	Ticker        string
	TimeFrame     string
	Limit         int
	NextPageToken string
}
