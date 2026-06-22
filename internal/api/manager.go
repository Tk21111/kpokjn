package api

import (
	"container/heap"
	"context"
	"kpokjn/domain"
	"kpokjn/internal/data"
	"sync"
	"time"
)

type ApiQueue struct {
	Job            *domain.ApiJob
	timeStartQueue time.Time

	index    int
	Priority int
}

type ApiManager struct {
	mu    sync.Mutex
	Queue *pq

	Writer      *data.Writer
	Rate        int
	dispatchJob chan *domain.ApiJob
	ctx         context.Context
}

type pq []*ApiQueue

func (q pq) Len() int           { return len(q) }
func (q pq) Less(i, j int) bool { return q[i].Priority < q[j].Priority } // lower = higher priority
func (q pq) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].index = i
	q[j].index = j
}
func (q *pq) Push(x any) {
	item := x.(*ApiQueue)
	item.index = len(*q)
	*q = append(*q, item)
}
func (q *pq) Pop() any {
	old := *q
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*q = old[:n-1]
	return item
}

func (AMP *ApiManager) Submit(job *domain.ApiJob, priority int) {
	item := &ApiQueue{
		Job:            job,
		Priority:       priority,
		timeStartQueue: time.Now(),
	}
	AMP.mu.Lock()
	heap.Push(AMP.Queue, item)
	AMP.mu.Unlock()
}

func (AMP *ApiManager) Pop() *domain.ApiJob {
	AMP.mu.Lock()
	if len(*AMP.Queue) > 0 {
		item := heap.Pop(AMP.Queue).(*ApiQueue)
		AMP.mu.Unlock()
		return item.Job
	}
	AMP.mu.Unlock()

	return nil
}

func (APM *ApiManager) NewApiConsumer(client *domain.Client, ratelimit int) *ApiConsumer {
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
	}
}

// watch dog
func (APM *ApiManager) Run() {

	agingTicker := time.NewTicker(10 * time.Second)
	defer agingTicker.Stop()

	for {
		<-agingTicker.C
		APM.age()

	}
}

func (AMP *ApiManager) age() {
	now := time.Now()
	AMP.mu.Lock()
	defer AMP.mu.Unlock()
	for i, item := range *AMP.Queue {
		if item.Priority > 0 && now.Sub(item.timeStartQueue) > 30*time.Second {
			item.Priority--
			heap.Fix(AMP.Queue, i) // O(log n), not a full rebuild
		}
	}
}
