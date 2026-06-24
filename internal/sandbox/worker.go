package sandbox

import (
	"bufio"
	"encoding/json"
	"fmt"
	"kpokjn/domain"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"
)

type Worker struct {
	id   int
	cmd  *exec.Cmd
	enc  *json.Encoder
	scan *bufio.Scanner
	mu   sync.Mutex
}

type WorkerState struct {
	worker         *Worker
	loadedFormulas map[string]struct{}
	jobCh          chan domain.ProcessJob
	queueDepth     atomic.Int32
	lastJobTime    atomic.Int64
	eventCh        chan<- *WorkerEvent
	cfg            *PoolConfig
}

func NewWorker(id int) (*Worker, error) {
	cmd := exec.Command("python3", "worker.py")
	cmd.Stderr = os.Stderr

	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	return &Worker{
		id:   id,
		cmd:  cmd,
		enc:  json.NewEncoder(stdinPipe),
		scan: bufio.NewScanner(stdoutPipe),
	}, nil
}

func (w *Worker) Send(payload any) (map[string]any, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.enc.Encode(payload); err != nil {
		return nil, fmt.Errorf("worker %d encode: %w", w.id, err)
	}
	if !w.scan.Scan() {
		return nil, fmt.Errorf("worker %d stdout closed", w.id)
	}

	var result map[string]any
	if err := json.Unmarshal(w.scan.Bytes(), &result); err != nil {
		return nil, fmt.Errorf("worker %d decode: %w", w.id, err)
	}
	if errMsg, ok := result["error"]; ok {
		return nil, fmt.Errorf("worker %d py error: %v", w.id, errMsg)
	}
	return result, nil
}

func newWorkerState(id int, w *Worker, eventCh chan<- *WorkerEvent, cfg *PoolConfig) *WorkerState {
	ws := &WorkerState{
		worker:         w,
		loadedFormulas: make(map[string]struct{}),
		jobCh:          make(chan domain.ProcessJob, cfg.WorkerJobBuf),
		eventCh:        eventCh,
		cfg:            cfg,
	}
	ws.lastJobTime.Store(time.Now().UnixNano())
	return ws
}

func (ws *WorkerState) HasFormula(id string) bool {
	_, ok := ws.loadedFormulas[id]
	return ok
}

func (ws *WorkerState) IsIdle() bool {
	if ws.queueDepth.Load() > 0 {
		return false
	}
	return time.Since(time.Unix(0, ws.lastJobTime.Load())) > ws.cfg.IdleGrace
}

func (ws *WorkerState) stop() {
	close(ws.jobCh)
	ws.worker.cmd.Process.Kill()
	ws.worker.cmd.Wait()
}

func (ws *WorkerState) loop() {
	for job := range ws.jobCh {
		ws.queueDepth.Add(-1)
		ws.lastJobTime.Store(time.Now().UnixNano())

		rows := make([]map[string]any, len(job.Data))
		for i, v := range job.Data {
			rows[i] = map[string]any{
				"open": v.Open, "high": v.High,
				"low": v.Low, "close": v.Close, "volume": v.Volume,
			}
		}

		resp, err := ws.worker.Send(map[string]any{
			"cmd":        "eval",
			"formula_id": job.FormulaID,
			"ticker":     job.Ticker,
			"data":       rows,
			"params":     job.Params,
		})

		result := &domain.JobResult{JobID: job.JobID, Ticker: job.Ticker}
		if err != nil {
			result.Err = err
		} else if sig, ok := resp["signal"].(float64); ok {
			result.Signal = int(sig)
		}

		job.Feedback <- result

		if ws.queueDepth.Load() == 0 {
			go ws.scheduleIdleCheck()
		}
	}
}

func (ws *WorkerState) scheduleIdleCheck() {
	time.Sleep(ws.cfg.IdleGrace)
	if ws.queueDepth.Load() == 0 {
		select {
		case ws.eventCh <- &WorkerEvent{ws: ws, eventType: EventIdle}:
		default:
		}
	}
}
