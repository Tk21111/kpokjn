package sandbox

import (
	"fmt"
	"kpokjn/domain"
	"kpokjn/internal/logx"
	"log"
	"sync"
	"time"
)

type EventType string

const (
	EventNearFull EventType = "near_full"
	EventIdle     EventType = "idle"
)

type WorkerEvent struct {
	ws        *WorkerState
	eventType EventType
}

type PoolManager struct {
	cfg           *PoolConfig
	workerEventCh chan *WorkerEvent
	inCh          chan domain.ProcessJob
	workers       []*WorkerState
	formulas      map[string][]*WorkerState // formula_id -> worker index
	formulaPaths  map[string]string
	mu            sync.RWMutex
}

type PoolConfig struct {
	DepthThreshold int32
	NearFullMark   int32
	IdleGrace      time.Duration
	MaxWorkers     int
	WorkerJobBuf   int
	EventChBuf     int
}

// note path will actually store in sql so load func is need but for now it fine
func NewPoolManager(cfg *PoolConfig, inChBuf int) *PoolManager {
	pm := &PoolManager{
		inCh:          make(chan domain.ProcessJob, inChBuf),
		workerEventCh: make(chan *WorkerEvent, cfg.EventChBuf),
		formulas:      make(map[string][]*WorkerState),
		formulaPaths:  make(map[string]string),
		cfg:           cfg,
	}
	go pm.managerLoop()
	return pm
}

func (pm *PoolManager) Submit(job domain.ProcessJob) {
	pm.inCh <- job
}

func (pm *PoolManager) managerLoop() {
	for {
		select {
		case job := <-pm.inCh:
			if err := pm.route(job); err != nil {
				job.Feedback <- &domain.JobResult{JobID: job.JobID, Ticker: job.Ticker, Err: err}
			}
		case event := <-pm.workerEventCh:
			pm.handleWorkerEvent(event)
		}
	}
}

func (pm *PoolManager) route(job domain.ProcessJob) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	best := pm.leastLoaded(job.FormulaID)

	if best == nil || best.queueDepth.Load() >= pm.cfg.DepthThreshold {
		if idle := pm.idleWorker(job.FormulaID); idle != nil {
			if err := pm.loadFormula(idle, job.FormulaID); err != nil {
				log.Printf("[pool] load onto idle worker failed: %v", err)
			} else {
				best = idle
			}
		}
	}

	if best == nil || best.queueDepth.Load() >= pm.cfg.DepthThreshold {
		if len(pm.workers) < pm.cfg.MaxWorkers {
			ws, err := pm.spawnWorker()
			if err != nil {
				if best == nil {
					return fmt.Errorf("spawn failed, no fallback: %w", err)
				}
				log.Printf("[pool] spawn failed, falling back to depth=%d: %v",
					best.queueDepth.Load(), err)
			} else {
				if err := pm.loadFormula(ws, job.FormulaID); err != nil {
					ws.stop()
					if best == nil {
						return fmt.Errorf("load on fresh worker failed: %w", err)
					}
				} else {
					best = ws
				}
			}
		}
	}

	if best == nil {
		return fmt.Errorf("no available worker for formula %s", job.FormulaID)
	}

	newDepth := best.queueDepth.Add(1)
	best.jobCh <- job

	if newDepth >= pm.cfg.NearFullMark {
		select {
		case pm.workerEventCh <- &WorkerEvent{ws: best, eventType: EventNearFull}:
		default:
		}
	}

	return nil
}

func (pm *PoolManager) handleWorkerEvent(event *WorkerEvent) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	switch event.eventType {
	case EventNearFull:
		if len(pm.workers) < pm.cfg.MaxWorkers {
			logx.Infof("[pool] worker %d near full — preemptive spawn", event.ws.worker.id)
			if _, err := pm.spawnWorker(); err != nil {
				logx.Infof("[pool] preemptive spawn failed: %v", err)
			}
		}
	case EventIdle:
		// hook for eviction — just logx Infof now
		logx.Infof("[pool] worker %d idle (loaded: %d formulas)",
			event.ws.worker.id, len(event.ws.loadedFormulas))
	}
}

func (pm *PoolManager) spawnWorker() (*WorkerState, error) {
	id := len(pm.workers)
	w, err := NewWorker(id)
	if err != nil {
		return nil, fmt.Errorf("spawn worker %d: %w", id, err)
	}

	ws := newWorkerState(id, w, pm.workerEventCh, pm.cfg)
	go ws.loop()

	pm.workers = append(pm.workers, ws)
	log.Printf("[pool] spawned worker %d (total=%d)", id, len(pm.workers))
	return ws, nil
}

func (pm *PoolManager) RegisterFormula(formulaID, path string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.formulaPaths[formulaID] = path
}

// DeregisterFormula removes the formula from the pool entirely.
// Unloads from all workers that have it loaded.
func (pm *PoolManager) DeregisterFormula(formulaID string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for _, ws := range pm.formulas[formulaID] {
		if err := pm.unloadFormula(ws, formulaID); err != nil {
			log.Printf("[pool] deregister unload failed on worker %d: %v", ws.worker.id, err)
		}
	}
	delete(pm.formulaPaths, formulaID)
}

func (pm *PoolManager) loadFormula(ws *WorkerState, formulaID string) error {

	formulaPath, ok := pm.formulaPaths[formulaID]
	if !ok {
		return fmt.Errorf("FormulaPath not found ")
	}

	resp, err := ws.worker.Send(map[string]any{
		"cmd":          "load",
		"formula_id":   formulaID,
		"formula_path": formulaPath,
	})
	if err != nil {
		return fmt.Errorf("load cmd: %w", err)
	}
	log.Printf("[pool] worker %d loaded formula %s: %v", ws.worker.id, formulaID, resp)

	ws.loadedFormulas[formulaID] = struct{}{}
	pm.formulas[formulaID] = append(pm.formulas[formulaID], ws)
	return nil
}

func (pm *PoolManager) unloadFormula(ws *WorkerState, formulaID string) error {
	resp, err := ws.worker.Send(map[string]any{
		"cmd":        "unload",
		"formula_id": formulaID,
	})
	if err != nil {
		return fmt.Errorf("unload cmd: %w", err)
	}
	log.Printf("[pool] worker %d unloaded formula %s: %v", ws.worker.id, formulaID, resp)

	delete(ws.loadedFormulas, formulaID)
	pm.removeFromFormulaMap(formulaID, ws)
	return nil
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func (pm *PoolManager) leastLoaded(formulaID string) *WorkerState {
	var best *WorkerState
	bestDepth := int32(1<<31 - 1)
	for _, ws := range pm.formulas[formulaID] {
		if d := ws.queueDepth.Load(); d < bestDepth {
			bestDepth = d
			best = ws
		}
	}
	return best
}

func (pm *PoolManager) idleWorker(formulaID string) *WorkerState {
	for _, ws := range pm.workers {
		if !ws.HasFormula(formulaID) && ws.queueDepth.Load() < pm.cfg.DepthThreshold {
			return ws
		}
	}
	return nil
}

func (pm *PoolManager) removeFromFormulaMap(formulaID string, target *WorkerState) {
	list := pm.formulas[formulaID]
	for i, ws := range list {
		if ws == target {
			pm.formulas[formulaID] = append(list[:i], list[i+1:]...)
			return
		}
	}
}
