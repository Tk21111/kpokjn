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
	Start         time.Time
	End           time.Time
	Ticker        string
	TimeFrame     string
	Limit         int
	NextPageToken string
	Priority      int
}
