package domain

import (
	"kpokjn/internal/config"
	"net/http"
	"time"
)

type Client struct {
	Cfg    *config.Config
	Client *http.Client
}

type ApiJob struct {
	ID            string
	Feedback      chan<- *ApiResult
	Start         time.Time
	End           time.Time
	Ticker        string
	TimeFrame     string
	Limit         int
	NextPageToken string
	Priority      int
}
type Result int

const (
	Finish Result = iota
	Err
	Continous
)

type ApiResult struct {
	ID     string
	Data   []any
	Status JobResult
	Err    any
}
