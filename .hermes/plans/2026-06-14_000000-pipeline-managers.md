# Manager Interaction Pipeline

> **Source:** Derived from `plan.html` (Trading Execution Engine — Full Architecture)
> **Scope:** How every manager/goroutine in the system interacts with every other, in execution order

---

## 1. All Managers — Inventory

| # | Manager | Language | Role |
|---|---------|----------|------|
| M1 | **Filler Scheduler** | Go | Priority-queue scheduler; pops tickers, dispatches Alpaca fetches |
| M2 | **Alpaca Fetch Pool** | Go | Bounded goroutine pool that executes actual HTTP calls to Alpaca |
| M3 | **SQLite Writer** | Go | Single goroutine that owns all SQLite writes; processes WriteRequests |
| M4 | **Sync & Decision (Time Gate)** | Go | Collects TickerUpdate batch, performs staleness check, emits EvalRequests |
| M5 | **Formula Manager** | Go | Loads/caches formulas, groups by hash, injects params |
| M6 | **Process Data Manager (PDM)** | Go | Semaphore-bounded dispatcher; routes BatchEvalRequests to Python workers |
| M7 | **PyWorker Pool** | Go + Python | N persistent Python subprocesses; each has a Go goroutine + Python process |
| M8 | **Post Processor (Result Sink)** | Go | Dedup check, Discord signal POST, updates last_signal |
| M9 | **Error Manager** | Go | Observability component; receives all ErrorEvents, logs, aggregates, routes to Discord error webhook |
| M9.5 | **Engine Monitor** | Go | Collects heartbeats, queue depths, worker health; exposes /health and /status endpoints |
| M10 | **Backfill Worker** | Go | One-time historical data fetch on startup; separate rate-limit budget |
| M11 | **Formula File Watcher** | Go | Watches formulas/*.py for changes; invalidates Formula Manager cache |
| M12 | **Rolling Pruner** | Go | Weekly low-priority goroutine; deletes OHLCV rows older than 2 years |
| M13 | **Config Loader** | Go | Reads config.json + env vars at startup; provides snapshot to all managers |

---

## 2. Interaction Map — Who Talks to Whom

```
M13 Config Loader
  │  provides config snapshot to ALL managers at startup
  ▼
M10 Backfill Worker ──── writes via ──────► M3 SQLite Writer
  │                                           │
  │  backfill_done=1                          │  ohlcv rows
  ▼                                           ▼
M1 Filler Scheduler ─── writes via ──────► M3 SQLite Writer  (hourly candles)
  │                                        │
  │  TickerUpdate channel                  │  WriteRequest channel
  ▼                                        │
M4 Sync & Decision ◄── reads ── M1 Filler Scheduler (liveCandle via shared FillerState)
  │                                        │
  │  EvalRequest channel                   │
  ▼                                        │
M5 Formula Manager ─── reads ── M11 Formula File Watcher (cache invalidation signal)
  │                                        │
  │  BatchEvalRequest channel              │
  ▼                                        │
M6 PDM ── dispatches to ──► M7 PyWorker Pool (stdin JSON)
  │                            │
  │  EvalResult channel        │  JSON over stdin/stdout
  │  ◄─────────────────────────┘
  ▼
M8 Post Processor ── writes via ──► M3 SQLite Writer (last_signal, signal_log)
  │
  │  Discord webhook POST
  ▼
  External: Discord

ALL managers ── ErrorEvent channel ──► M9 Error Manager
                                        │
                                        │  Discord error webhook POST
                                        ▼
                                        External: Discord #engine-errors

ALL managers ── StatusUpdate channel ──► M9.5 Engine Monitor
                                          │
                                          ├─ manager health
                                          ├─ queue depth metrics
                                          ├─ worker status
                                          └─ /status endpoint

M12 Rolling Pruner ── writes via ──► M3 SQLite Writer (DELETE + VACUUM)
```

---

## 3. Channel Inventory — The Typed Contracts

| Channel | Type | Producer → Consumer | Purpose |
|---------|------|---------------------|---------|
| `writerCh` | `chan WriteRequest` | M1, M3(caller), M8, M12 → M3 | All SQLite writes funneled through single writer |
| `tickerUpdateCh` | `chan TickerUpdate` | M1 → M4 | New candle data from Alpaca fetch |
| `evalRequestCh` | `chan EvalRequest` | M4 → M5 | Bare eval request (ticker + data, no formula) |
| `batchEvalRequestCh` | `chan BatchEvalRequest` | M5 → M6 | Formula attached, grouped by hash |
| `evalResultCh` | `chan EvalResult` | M6 → M8 | Signal + new_state from Python |
| `errorCh` | `chan ErrorEvent` | ALL → M9 | Centralized error reporting |
| `statusCh` | `chan StatusUpdate` | ALL → M9.5 | Health, heartbeat, queue depth reporting |
| `configCh` | `chan ConfigSnapshot` | M13 → all at boot | One-shot config distribution |
| `formulaInvalidationCh` | `chan string` (ticker) | M11 → M5 | File change notification |
| `workCh` | `chan BatchEvalRequest` | M6 → M7[i] | Per-worker job dispatch |
| stdin/stdout | JSON lines | M6 ↔ M7[i] | Per-evaluation request/response |

### Shared Types

```go
type StatusUpdate struct {
    Manager       string
    Healthy       bool
    QueueDepth    int
    ErrorCount    int
    LastHeartbeat time.Time
}

type FormulaMeta struct {
    ID      string
    Version int
}
```

---

## 4. Execution Flow — Per-Tick Walkthrough

This is what happens when a single hourly tick fires, in goroutine order:

### Phase A — Data Fetch (M1 + M2)

```
M1 Filler Scheduler
  │ pops TickerJob from min-heap
  │ dispatches to M2 Alpaca Fetch Pool (bounded pool of 5 goroutines)
  │
M2 Alpaca Fetch Pool
  │ HTTP GET to Alpaca (timeframe=1Hour for normal, 15Min for hot-path)
  │ on success:
  │   hourly path → M1 writes to FillerState.RingBuffers[ticker].Append()
  │                 M1 sends WriteRequest{INSERT ohlcv} → M3 SQLite Writer
  │                 M1 sends TickerUpdate → M4 Sync & Decision
  │   hot path   → M1 writes to FillerState.LiveCandle[ticker]
  │                 M1 sends TickerUpdate → M4 Sync & Decision
  │ on failure:
  │   M1 sends TickerUpdate{Err: ...} → M4 (Stage 2 skips this ticker)
  │   M1 sends ErrorEvent → M9 Error Manager
```

### Phase B — Sync Gate (M4)

```
M4 Sync & Decision
  │ receives TickerUpdate from M1 (one per ticker)
  │ counts: received / expected
  │ gate opens when: count == len(tickers) OR 3-min timeout
  │
  │ for each ticker in batch:
  │   staleness check: ringBuffer[ticker].Head().Timestamp == expected?
  │     no  → skip, send ErrorEvent → M9
  │     yes → slice last N candles from ringBuffer, append liveCandle
  │
  │ sends EvalRequest{Ticker, TickTime, Data} → M5 Formula Manager
  │ (one EvalRequest per ticker, sent sequentially)
```

### Phase C — Formula Resolution (M5)

```
M5 Formula Manager
  │ receives EvalRequest from M4
  │
  │ checks formulaCache[ticker]
  │   cache miss or invalidated → read formulas/{ticker}.py from disk
  │                              → compute FormulaMeta{ID, Version}
  │                              → store FormulaMeta in formulaCache
  │
  │ injects per-ticker params from config (threshold overrides, etc.)
  │
  │ groups by FormulaMeta.ID:
  │   if same ID as previous ticker → append to current BatchEvalRequest
  │   if new ID → flush previous BatchEvalRequest → M6
  │             start new BatchEvalRequest
  │
  │ sends BatchEvalRequest{FormulaMeta, Code, []BatchItem} → M6 PDM
  │
  │ M11 Formula File Watcher (async, independent):
  │   fsnotify on formulas/*.py
  │   on change → sends ticker name → M5 formulaInvalidationCh
  │   M5 deletes formulaCache[ticker] → next eval re-reads from disk
  │   new Version propagates naturally → M6 detects version mismatch → resets prev_state
```

### Phase D — Python Evaluation (M6 + M7)

```
M6 PDM
  │ receives BatchEvalRequest from M5
  │ acquires semaphore slot (blocks if all workers busy)
  │ dispatches to next free M7 PyWorker via workCh
  │
M7 PyWorker[i]
  │ Go goroutine:
  │   wraps in context.WithTimeout(30s)
  │   writes JSON to Python subprocess stdin
  │   reads JSON from Python subprocess stdout
  │
  │ PDM checks FormulaMeta.Version vs stored version
  │   version mismatch → prev_state = nil (full recompute)
  │
  │ Python subprocess:
  │   _run_job(job):
  │     checks formula version vs stored version
  │       mismatch → prev_state = {} (full recompute)
  │     exec(formula_code, fresh_namespace)
  │     signal, new_state = namespace["evaluate"](df, prev_state)
  │     stores _state_store[ticker] = {version, state: new_state}
  │     returns {signal, new_state, error: null}
  │
  │ Go goroutine:
  │   on success → sends EvalResult → M8 Post Processor
  │   on timeout → kills stdin, respawns Python subprocess
  │              → sends EvalResult{Err: timeout} → M8
  │   on crash   → detects broken pipe, respawns subprocess
  │              → sends ErrorEvent → M9
  │   releases semaphore slot
```

### Phase E — Result Processing (M8)

```
M8 Post Processor
  │ receives EvalResult from M6
  │
  │ if EvalResult.Err != null:
  │   sends ErrorEvent → M9 Error Manager
  │   (done, no Discord signal)
  │
  │ if EvalResult.Signal == 1:
  │   reads last_signal from SQLite (via M3 WriteRequest + replyCh)
  │   checks cooldown: (now - triggered_at) < cooldown_hours?
  │     yes → skip (dedup)
  │     no  → POST to Discord signal webhook
  │           on success:
  │             WriteRequest{UPSERT last_signal} → M3 SQLite Writer
  │             WriteRequest{INSERT signal_log} → M3 SQLite Writer
  │           on failure:
  │             retry with exponential backoff (max 3)
  │             on final failure → ErrorEvent → M9
  │
  │ if EvalResult.Signal == 0:
  │   (hold — no action, no Discord)
```

### Phase F — Error Handling (M9)

M9 Error Manager is an **observability component** — it does not own recovery.

Responsibilities:
- Logging
- Alert routing
- Error aggregation
- Burst suppression (>10 errors in 10s from same source → throttle, log summary)
- Discord error delivery

Non-Responsibilities:
- Retries (handled by M7/M8)
- Worker recovery (handled by M6)
- Process supervision
- Pipeline orchestration

```
M9 Error Manager
  │ receives ErrorEvent from ANY manager (non-blocking send)
  │
  │ burst suppression:
  │   if >10 errors in 10s from same source → throttle, log summary
  │
  │ routing:
  │   Alpaca errors     → log + Discord error webhook
  │   Stale data        → log + Discord error webhook
  │   SQLite failures   → log + Discord error webhook + trigger graceful shutdown
  │   Python exceptions → log + Discord error webhook
  │   Discord failures  → log (retry already handled by M8)
  │
  │ POST to Discord error webhook (separate channel from signals)
```

### Phase F.5 — Engine Monitor (M9.5)

M9.5 Engine Monitor provides a system-wide view without coupling managers together.

Responsibilities:
- Receive StatusUpdate from all managers
- Track heartbeat freshness
- Track queue depths
- Track worker availability
- Expose /health endpoint
- Expose /status endpoint
- Generate periodic system snapshots

Non-Responsibilities:
- No retries
- No orchestration
- No job scheduling
- No business logic

```
M9.5 Engine Monitor
  │ receives StatusUpdate from ALL managers (non-blocking send)
  │
  │ tracks per-manager:
  │   heartbeat freshness  → stale if >2x expected interval
  │   queue depth          → alert if >80% capacity
  │   error count          → rolling 1-minute window
  │   worker status        → idle / busy / crashed
  │
  │ exposes:
  │   /health  → liveness probe (200 if all managers healthy)
  │   /status  → full JSON snapshot of all manager states
  │
  │ generates periodic system snapshots (every 60s) for logging
```

### Phase G — Background Maintenance (M12)

```
M12 Rolling Pruner (weekly, independent)
  │ DELETE FROM ohlcv WHERE timestamp < unixepoch('now', '-2 years')
  │ PRAGMA optimize
  │ VACUUM
  │ all via M3 SQLite Writer channel
```

---

## 5. Startup Sequence — Manager Initialization Order

```
1. M13 Config Loader
     reads config.json + env vars
     broadcasts ConfigSnapshot to all managers

2. M3 SQLite Writer
     opens SQLite, applies schema migrations
     starts single writer goroutine (blocks on writerCh)

3. M6 PDM + M7 PyWorker Pool
     creates N PyWorker structs
     spawns N Python subprocesses
     waits for each subprocess to ack ready (stdin handshake)
     workers now blocking on workCh

4. M10 Backfill Worker
     checks bootstrap_state table
     for each ticker with backfill_done=0:
       fetches 2 years of hourly data from Alpaca
       writes via M3 SQLite Writer
       sets backfill_done=1
     BLOCKING — must complete before pipeline starts

5. M9.5 Engine Monitor
     starts status collector goroutine (blocks on statusCh)
     exposes /health endpoint
     exposes /status endpoint

6. M9 Error Manager
     starts goroutine blocking on errorCh

7. M8 Post Processor
     starts goroutine blocking on evalResultCh

8. M4 Sync & Decision
     starts goroutine blocking on tickerUpdateCh

9. M5 Formula Manager
     pre-warms formulaCache for all tickers
     starts M11 Formula File Watcher goroutine

10. M1 Filler Scheduler
     builds initial min-heap from config tickers
     arms hourly timer (HH:02 Eastern)
     SYSTEM IS LIVE
```

---

## 6. Shared State — Who Owns What

| State | Owner | Readers | Concurrency |
|-------|-------|---------|-------------|
| `FillerState` (RingBuffers + LiveCandle) | M1 Filler | M1 (write), M4 (read) | `sync.RWMutex` inside FillerState |
| `formulaCache` | M5 Formula Manager | M5 only | single goroutine (M5) + M11 sends invalidation via channel |
| `_state_store` (Python) | M7 PyWorker[i] | M7[i] only | one Python subprocess per worker, no sharing |
| `prevState` (Go side) | M6 PDM | M6 only | one map per worker goroutine |
| SQLite DB | M3 SQLite Writer | M3 only (all writes funneled) | single writer goroutine |
| `last_signal` cache | M8 Post Processor | M8 only | in-memory copy, refreshed from SQLite |
| `errorCh` | M9 Error Manager | all producers | thread-safe channel |
| `statusCh` | M9.5 Engine Monitor | all producers | thread-safe channel |
| Config snapshot | M13 Config Loader | all managers | read-only after boot |

---

## 7. Failure Cascade — What Breaks When Something Fails

```
M2 Alpaca Fetch fails for ticker X
  → M1 sends TickerUpdate{Err} → M4
  → M4 excludes X from this cycle
  → M1 sends ErrorEvent → M9
  → pipeline continues for remaining 49 tickers

M3 SQLite Writer fails
  → replyCh returns error to caller
  → M1/M8 receive error
  → M1/M8 send ErrorEvent → M9
  → M9 triggers graceful shutdown (critical: data loss risk)

M7 PyWorker[i] crashes
  → M6 detects broken pipe on stdin write
  → M6 respawns Python subprocess
  → M6 retries job once
  → if retry fails → EvalResult{Err} → M8 → M9

M8 Discord POST fails
  → M8 retries with exponential backoff (max 3)
  → on final failure → ErrorEvent → M9
  → signal already persisted to signal_log (audit trail intact)

M10 Backfill Worker fails
  → ticker stays backfill_done=0
  → next startup retries backfill
  → pipeline does NOT start until all tickers backfilled

M12 Rolling Pruner fails
  → SQLite grows beyond 2 years
  → no data loss, just disk usage
  → next weekly run retries
```

---

## 8. Key Design Decisions in Manager Interactions

1. **Single Writer Pattern (M3):** All SQLite writes go through one goroutine via `writerCh`. No WAL mode. No lock contention. Every write is a `WriteRequest{SQL, Args, Reply}` — the caller blocks on `Reply` until the writer commits.

2. **Channel-only communication:** No manager holds a reference to another manager's internals. M1 doesn't know about M5. M4 doesn't know about M7. All communication is through typed Go channels.

3. **Error Manager is fire-and-forget:** Every manager sends to `errorCh` (buffered, non-blocking). M9 owns all Discord error delivery. Stages never POST to Discord errors themselves.

4. **Formula Manager is the sole formula reader:** No other stage reads `formulas/*.py`. M5 owns the cache, the file watcher, and the grouping logic.

5. **PDM is the sole concurrency governor:** M6's semaphore bounds how many Python evaluations run simultaneously. M4 blocks on `evalRequestCh` if all workers are busy — natural backpressure.

6. **RAM is the serving layer, SQLite is persistence:** M4 reads from `FillerState` (RAM), never from SQLite. SQLite is only read on startup (backfill warm-up) and for `last_signal` dedup checks in M8.

7. **Pre-Discord persistence:** M8 writes to `signal_log` BEFORE attempting Discord POST. A signal computed but failed to deliver is never lost.

8. **Engine Monitor is read-only:** M9.5 observes but never mutates. It has no backpressure mechanism, no retry logic, no orchestration. If M9.5 fails, the pipeline continues unaffected.

---

## 9. File Map — Where Each Manager Lives

```
cmd/engine/
  main.go                    ← startup sequence, channel creation, goroutine spawn

internal/
  filler/
    scheduler.go             ← M1 Filler Scheduler (min-heap, priority queue)
    fetch_pool.go            ← M2 Alpaca Fetch Pool (bounded goroutine pool)
    filler_state.go          ← FillerState struct (RingBuffers + LiveCandle + RWMutex)
    rate_limiter.go          ← FillerRateLimiter (live + backfill lanes)

  sync/
    time_gate.go             ← M4 Sync & Decision (gate, staleness, array build)

  formula/
    manager.go               ← M5 Formula Manager (cache, params, grouping)
    watcher.go               ← M11 Formula File Watcher (fsnotify)

  pdm/
    pdm.go                   ← M6 Process Data Manager (semaphore, dispatch)
    worker.go                ← M7 PyWorker struct (goroutine + subprocess lifecycle)

  post/
    processor.go             ← M8 Post Processor (dedup, Discord POST)

  error/
    manager.go               ← M9 Error Manager (burst suppression, routing)

  monitor/
    collector.go             ← M9.5 Engine Monitor (health, status endpoints)

  backfill/
    worker.go                ← M10 Backfill Worker (historical fetch)

  data/
    writer.go                ← M3 SQLite Writer (single writer goroutine)
    schema.go                ← SQL migrations
    pruner.go                ← M12 Rolling Pruner

  config/
    loader.go                ← M13 Config Loader (config.json + env vars)

scripts/
  worker.py                  ← Python subprocess entry point (_run_job, _state_store)
```
