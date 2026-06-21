package api

import (
	"context"
	"kpokjn/domain"
	"sync"
	"time"

	"golang.org/x/text/cases"
)

type ApiProducer struct {
	Client    *domain.Client
	RateLimit int // per sec
}

type ApiQueue struct {
	ApiJob         *domain.ApiJob
	timeStartQueue time.Time
}

type ApiManager struct {
	gate sync.RWMutex
	Queue []*ApiQueue
	
	ctx   context.Context
}

func NewApiProducer(client *domain.Client, ratelimit int) *ApiProducer {
	return &ApiProducer{
		Client:    client,
		RateLimit: ratelimit,
	}
}

func (APM *ApiManager) Submit(workerCfg *domain.ApiJob) {

	queue := &ApiQueue{
		ApiJob:         workerCfg,
		timeStartQueue: time.Now(),
	}

	APM.gate.Lock()
	defer APM.gate.Unlock()

	
}

func (APM *ApiManager) Run() {

	//read for queue and send work 
	for {
		select {
			case AP
		}
	}
}

func (Ap *ApiProducer) run() {
	ticker := time.NewTicker(500 * time.Millisecond)
	for {
		<-ticker.C
		select {
		case job := <-urgentCh: // drain urgent first
			go FetchAndWrite(job)
		default:
			select {
			case job := <-normalCh:
				go fetch(job)
			default:
				// nothing to do
			}
		}
	}
}
