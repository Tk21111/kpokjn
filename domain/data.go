package domain

import "time"

type OHLCV struct {
	Ticker    string
	Timestamp int64
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
}

type Ticker struct {
	Ticker    string
	LastFetch time.Time
}

type ProcessJob struct {
	JobID     string
	FormulaID string
	Ticker    string
	Data      []OHLCV
	Params    map[string]any
	Feedback  chan<- *JobResult
}

type JobResult struct {
	JobID  string
	Ticker string
	Signal int
	Err    error
}
