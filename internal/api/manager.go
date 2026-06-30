package api

import (
	"container/heap"
	"context"
	"kpokjn/domain"
	"kpokjn/internal/config"
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
	Cfg   *config.Config
	mu    sync.Mutex
	Queue *pq

	onResult func([]domain.Bar, *domain.ApiJob)

	formulaMu  sync.RWMutex
	formulaMap map[string]*ApiProducer

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

func NewApiManager(ctx context.Context, writer *data.Writer, cfg *config.Config, onResult func([]domain.Bar, *domain.ApiJob), rate int) *ApiManager {
	// Initialize an empty priority queue
	q := make(pq, 0)

	// Optional: container/heap doesn't strictly require heap.Init() for an empty slice,
	// but it's safe to call if you ever decide to pre-populate 'q' in the future.
	heap.Init(&q)

	return &ApiManager{
		Cfg:         cfg,
		Queue:       &q,
		Writer:      writer,
		Rate:        rate,
		dispatchJob: make(chan *domain.ApiJob, 100), // Added a buffer; adjust size to your needs or remove the 100 for unbuffered
		onResult:    onResult,
		ctx:         ctx,
	}
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

func (m *ApiManager) GetProducer(id string) (*ApiProducer, bool) {
	m.formulaMu.RLock()
	defer m.formulaMu.RUnlock()

	p, exists := m.formulaMap[id]
	return p, exists
}

func (m *ApiManager) AddProducer(id string, p *ApiProducer) (bool, error) {
	m.formulaMu.Lock()
	defer m.formulaMu.Unlock()

	if m.formulaMap == nil {
		m.formulaMap = make(map[string]*ApiProducer)
	}

	if _, exists := m.formulaMap[id]; exists {
		return false, nil
	}

	m.formulaMap[id] = p
	return true, nil
}

func (m *ApiManager) RemoveProducer(id string) {
	m.formulaMu.Lock()
	defer m.formulaMu.Unlock()

	delete(m.formulaMap, id)
}
