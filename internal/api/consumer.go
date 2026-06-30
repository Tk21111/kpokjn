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
	onResult    func([]domain.Bar, *domain.ApiJob)
	PerSecLimit int // per sec
	PerMinLimit int // per min
}

func (APM *ApiManager) NewConsumer(client *domain.Client, perSecLimit int, perMinLimit int) *ApiConsumer {
	return &ApiConsumer{
		Client:      client,
		PerSecLimit: perSecLimit,
		PerMinLimit: perMinLimit,
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
	perMinCount := 0
	currentMinute := time.Now().Unix() / 60
	tickerSec := time.NewTicker(time.Second / time.Duration(Ap.PerSecLimit))
	defer tickerSec.Stop()

	for {
		<-tickerSec.C

		nowMinute := time.Now().Unix() / 60
		if nowMinute != currentMinute {
			currentMinute = nowMinute
			perMinCount = 0
		}

		if perMinCount >= Ap.PerMinLimit {
			nextMinuteTime := time.Unix((currentMinute+1)*60, 0)
			//block
			time.Sleep(time.Until(nextMinuteTime))

			currentMinute = time.Now().Unix() / 60
			perMinCount = 0
		}

		job := Ap.popFunc()
		if job != nil {
			// fmt.Printf("fetching job %v \n", job)
			go FetchAndWrite(Ap.Client, Ap.Writer, job, Ap.onPageToken, Ap.onResult)
			perMinCount++
		}
	}
}
