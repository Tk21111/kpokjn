package api

import (
	"kpokjn/domain"
	"kpokjn/internal/data"
	"time"
)

type ApiConsumer struct {
	Client *domain.Client
	Writer *data.Writer

	popFunc     func() *domain.ApiJob
	onPageToken func(*domain.ApiJob, string)
	RateLimit   int // per sec
}

func (Ap *ApiConsumer) Run() {
	ticker := time.NewTicker(time.Second / time.Duration(Ap.RateLimit))
	for {
		<-ticker.C
		job := Ap.popFunc()
		if job != nil {
			go FetchAndWrite(Ap.Client, Ap.Writer, job, Ap.onPageToken)
		}
	}
}
