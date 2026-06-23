package api

import (
	"fmt"
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

func (APM *ApiManager) NewConsumer(client *domain.Client, ratelimit int) *ApiConsumer {
	return &ApiConsumer{
		Client:    client,
		RateLimit: ratelimit,
		onPageToken: func(job *domain.ApiJob, s string) {
			job.NextPageToken = s
			APM.Submit(job, 5)
		},
		popFunc: func() *domain.ApiJob {
			return APM.Pop()
		},
		Writer: APM.Writer,
	}
}

func (Ap *ApiConsumer) Run() {
	ticker := time.NewTicker(time.Second / time.Duration(Ap.RateLimit))
	for {
		<-ticker.C
		job := Ap.popFunc()
		if job != nil {
			fmt.Printf("fetching job %v \n", job)
			go FetchAndWrite(Ap.Client, Ap.Writer, job, Ap.onPageToken)
		}
	}
}
