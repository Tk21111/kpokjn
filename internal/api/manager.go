package api

import (
	"kpokjn/domain"
	"kpokjn/internal/alpaca"
	"time"
)

func NewApiManager(client *alpaca.Client, ratelimit int) *domain.ApiManager {
	return &domain.ApiManager{
		Client:    client,
		RateLimit: ratelimit,
	}
}

var apiCh = make(chan *domain.ApiJob, 1000)

func (AP *ApiManager) Submit(workerCfg *domain.ApiJob) {
	AP.gate.RLock()
	defer AP.gate.RUnlock()
	if AP.closed {
		return
	}

	select {
	case apiCh <- workerCfg:
		return
	case <-AP.closeSignal:
		return
	}
}

func (Ap *ApiManager) run() {
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
