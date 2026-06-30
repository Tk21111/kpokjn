package api

import (
	"container/heap"
	"database/sql"
	"kpokjn/domain"
	"kpokjn/internal/config"
	"kpokjn/internal/data"
	"kpokjn/internal/logx"
	"sync"
	"time"

	"github.com/google/uuid"
)

const pendingTimeout = 5 * time.Minute

type tickerState struct {
	ticker       *domain.Ticker
	nextAction   time.Time // when this ticker is next due, OR pending-retry deadline if in-flight
	pendingSince time.Time // zero == not waiting on a job
	heapIndex    int
}

type tickerQueue []*tickerState

func (q tickerQueue) Len() int           { return len(q) }
func (q tickerQueue) Less(i, j int) bool { return q[i].nextAction.Before(q[j].nextAction) }
func (q tickerQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].heapIndex, q[j].heapIndex = i, j
}
func (q *tickerQueue) Push(x any) {
	item := x.(*tickerState)
	item.heapIndex = len(*q)
	*q = append(*q, item)
}
func (q *tickerQueue) Pop() any {
	old := *q
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.heapIndex = -1
	*q = old[:n-1]
	return item
}

type ApiProducer struct {
	mu          sync.Mutex
	queue       tickerQueue
	byTicker    map[string]*tickerState
	pendingJobs map[string]*domain.ApiJob

	MaxDuration time.Duration
	SubmitFunc  func(job *domain.ApiJob)
	FeedbackCh  chan *domain.ApiResult
}

func GetLastFetch(w *data.Writer, ctf *config.Config, ticker string) time.Time {
	var maxTimestamp sql.NullInt64
	err := w.QueryRow("SELECT MAX(timestamp) FROM ohlcv WHERE ticker = ?", ticker).Scan(&maxTimestamp)
	if err != nil && err != sql.ErrNoRows {
		logx.Errorf("failed to fetch max timestamp for %s: %v", ticker, err)
		return ctf.TickerTimeFallback
	}
	if maxTimestamp.Valid {
		return time.Unix(maxTimestamp.Int64, 0)
	}
	return ctf.TickerTimeFallback
}

func (APM *ApiManager) NewProducer(tickers []string) *ApiProducer {
	Ap := &ApiProducer{
		byTicker:    make(map[string]*tickerState),
		pendingJobs: make(map[string]*domain.ApiJob),
		MaxDuration: 5 * time.Minute,
		SubmitFunc: func(job *domain.ApiJob) {
			APM.Submit(job, 5)
		},
		FeedbackCh: make(chan *domain.ApiResult),
	}

	for _, sym := range tickers {
		t := &domain.Ticker{
			Ticker:    sym,
			LastFetch: GetLastFetch(APM.Writer, APM.Cfg, sym),
		}
		state := &tickerState{
			ticker:     t,
			nextAction: t.LastFetch.Add(Ap.MaxDuration), // backlogged tickers land in the past -> sort to front, natural backfill priority
		}
		Ap.byTicker[sym] = state
		Ap.queue = append(Ap.queue, state)
	}
	heap.Init(&Ap.queue)

	return Ap
}

func (Ap *ApiProducer) nextDeadline() time.Time {
	Ap.mu.Lock()
	defer Ap.mu.Unlock()
	if Ap.queue.Len() == 0 {
		return time.Now().Add(time.Minute)
	}
	return Ap.queue[0].nextAction
}

func (Ap *ApiProducer) Run() {
	timer := time.NewTimer(time.Until(Ap.nextDeadline()))
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			Ap.dispatchDue()
		case result := <-Ap.FeedbackCh:
			Ap.handleFeedback(result)
		}

		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(time.Until(Ap.nextDeadline()))
	}
}

func (Ap *ApiProducer) dispatchDue() {
	Ap.mu.Lock()
	defer Ap.mu.Unlock()

	now := time.Now()
	end := now.Add(-time.Hour) // lag buffer

	for Ap.queue.Len() > 0 && !Ap.queue[0].nextAction.After(now) {
		item := heap.Pop(&Ap.queue).(*tickerState)

		if !item.pendingSince.IsZero() {
			// hit the failsafe deadline with no feedback -> worker died/dropped result, retry
			logx.Errorf("ticker %s: job timed out waiting for feedback, retrying", item.ticker.Ticker)
			item.pendingSince = time.Time{}
		}

		if item.ticker.LastFetch.Before(end) {
			jobID := uuid.New().String()
			job := &domain.ApiJob{
				ID:        jobID,
				Start:     item.ticker.LastFetch,
				End:       end,
				Ticker:    item.ticker.Ticker,
				TimeFrame: "1min",
				Priority:  1,
				Feedback:  Ap.FeedbackCh,
				Limit:     10000,
			}
			Ap.pendingJobs[jobID] = job
			item.pendingSince = now
			item.nextAction = now.Add(pendingTimeout) // failsafe, overwritten by real feedback if it arrives
			go Ap.SubmitFunc(job)                     // never block the loop on a slow/full downstream
		} else {
			item.nextAction = item.ticker.LastFetch.Add(Ap.MaxDuration)
		}

		heap.Push(&Ap.queue, item)
	}
}

func (Ap *ApiProducer) handleFeedback(result *domain.ApiResult) {
	Ap.mu.Lock()
	defer Ap.mu.Unlock()

	job, ok := Ap.pendingJobs[result.ID]
	if !ok {
		logx.Errorf("feedback for unknown job ID: %s", result.ID)
		return
	}
	delete(Ap.pendingJobs, result.ID)

	state, ok := Ap.byTicker[job.Ticker]
	if !ok {
		return
	}

	state.pendingSince = time.Time{}
	if result.Status.Err == nil {
		state.ticker.LastFetch = job.End
		state.nextAction = state.ticker.LastFetch.Add(Ap.MaxDuration)
	} else {
		logx.Errorf("fetch failed for %s: %v", job.Ticker, result.Status.Err)
		state.nextAction = time.Now() // immediate retry on next loop pass
	}
	heap.Fix(&Ap.queue, state.heapIndex)
}
