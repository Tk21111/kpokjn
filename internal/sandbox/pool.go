package sandbox

import (
	"kpokjn/domain"
	"sync"
)

type PoolManager struct {
	jobCh            chan domain.ProcessJob
	workers          map[int]*Worker
	formulaWorkerMap map[string]int // formula_id -> worker index
	mu               sync.RWMutex
}

func NewPoolManager(inChBuf int) *PoolManager {
	pm := &PoolManager{
		jobCh:   make(chan domain.ProcessJob, inChBuf),
		workers: make(map[int][]*WorkerState),
	}
	go pm.managerLoop()
	return pm
}

func (pm *PoolManager) Submit(job domain.ProcessJob) {
	pm.inCh <- job
}

func (pm *PoolManager) managerLoop() {
	for job := range pm.inCh {
		if err := pm.route(job); err != nil {
			job.Feedback <- JobResult{JobID: job.JobID, Ticker: job.Ticker, Err: err}
		}
	}
}
