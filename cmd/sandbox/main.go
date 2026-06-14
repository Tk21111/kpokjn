package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

const formulaConfigPath = "cmd/sandbox/formulas.json"

type Bar struct {
	Timestamp string  `json:"t"`
	Open      float64 `json:"o"`
	High      float64 `json:"h"`
	Low       float64 `json:"l"`
	Close     float64 `json:"c"`
	Volume    int64   `json:"v"`
}

type WorkerRequest struct {
	ID        string                 `json:"id"`
	Ticker    string                 `json:"ticker,omitempty"`
	FormulaID string                 `json:"formula_id,omitempty"`
	Command   string                 `json:"command,omitempty"`
	Params    map[string]interface{} `json:"params,omitempty"`
	Bars      []Bar                  `json:"bars,omitempty"`
}

type WorkerResponse struct {
	ID           string                 `json:"id,omitempty"`
	OK           bool                   `json:"ok,omitempty"`
	Ready        bool                   `json:"ready,omitempty"`
	Signal       int                    `json:"signal,omitempty"`
	FormulaID    string                 `json:"formula_id,omitempty"`
	ProcessID    int                    `json:"pid,omitempty"`
	PandasLoaded bool                   `json:"pandas_loaded,omitempty"`
	ImportError  string                 `json:"import_error,omitempty"`
	Details      map[string]interface{} `json:"details,omitempty"`
	Error        string                 `json:"error,omitempty"`
}

type PythonRuntime struct {
	pythonBin string
	script    string

	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Scanner
	mu     sync.Mutex
}

type FormulaConfig struct {
	DefaultFormula string                 `json:"default_formula"`
	Stocks         map[string]StockConfig `json:"stocks"`
}

type StockConfig struct {
	FormulaID string                 `json:"formula_id"`
	Params    map[string]interface{} `json:"params"`
}

func main() {
	ctx := context.Background()
	runtime := NewPythonRuntime(pythonBin(), filepath.Join("cmd", "sandbox", "worker.py"))
	formulas, err := LoadFormulaConfig(formulaConfigPath)
	if err != nil {
		fatalf("load formula config: %v", err)
	}

	fmt.Println("=== Go -> Warm Python Runtime Sandbox ===")
	fmt.Printf("loaded formula config default=%s stocks=%d\n", formulas.DefaultFormula, len(formulas.Stocks))
	if err := runtime.Start(ctx); err != nil {
		fatalf("start runtime: %v", err)
	}
	defer runtime.Stop()

	responses := []WorkerResponse{
		mustCall(runtime, formulas.Job("job-001", "MSFT", sampleBars())),
		mustCall(runtime, formulas.Job("job-002", "TSLA", sampleBars())),
		mustCall(runtime, formulas.Job("job-003", "AAPL", sampleBars())),
	}
	for _, resp := range responses {
		fmt.Printf("%s formula=%s signal=%d pid=%d details=%v\n", resp.ID, resp.FormulaID, resp.Signal, resp.ProcessID, resp.Details)
	}
	verifySamePID(responses)

	unknown := formulas.Job("job-unknown-formula", "MSFT", sampleBars())
	unknown.FormulaID = "does_not_exist"
	if resp, err := runtime.Call(unknown); err != nil {
		fmt.Printf("unknown formula check: OK, formula=%s error=%q\n", resp.FormulaID, resp.Error)
	} else {
		fatalf("expected unknown formula to return a clean error")
	}

	fmt.Println("forcing Python runtime failure...")
	if _, err := runtime.Call(WorkerRequest{ID: "job-crash", Command: "crash"}); err != nil {
		fmt.Printf("failure detected by Go: %v\n", err)
	} else {
		fatalf("expected crash command to fail")
	}

	fmt.Println("respawning Python runtime...")
	if err := runtime.Restart(ctx); err != nil {
		fatalf("restart runtime: %v", err)
	}

	afterRestart := mustCall(runtime, formulas.Job("job-004", "MSFT", sampleBars()))
	fmt.Printf("job-004 formula=%s signal=%d pid=%d details=%v\n", afterRestart.FormulaID, afterRestart.Signal, afterRestart.ProcessID, afterRestart.Details)
	if afterRestart.ProcessID != responses[0].ProcessID {
		fmt.Println("respawn check: OK, replacement Python process has a new pid")
	}

	fmt.Println("sandbox complete")
}

func LoadFormulaConfig(path string) (FormulaConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return FormulaConfig{}, fmt.Errorf("open %s: %w", path, err)
	}
	defer file.Close()

	var cfg FormulaConfig
	if err := json.NewDecoder(file).Decode(&cfg); err != nil {
		return FormulaConfig{}, fmt.Errorf("decode %s: %w", path, err)
	}
	if cfg.DefaultFormula == "" {
		return FormulaConfig{}, errors.New("default_formula is required")
	}
	if cfg.Stocks == nil {
		cfg.Stocks = map[string]StockConfig{}
	}
	return cfg, nil
}

func (c FormulaConfig) Job(id, ticker string, bars []Bar) WorkerRequest {
	stock := c.Stocks[ticker]
	formulaID := stock.FormulaID
	if formulaID == "" {
		formulaID = c.DefaultFormula
	}

	return WorkerRequest{
		ID:        id,
		Ticker:    ticker,
		FormulaID: formulaID,
		Params:    stock.Params,
		Bars:      bars,
	}
}

func NewPythonRuntime(pythonBin, script string) *PythonRuntime {
	return &PythonRuntime{
		pythonBin: pythonBin,
		script:    script,
	}
}

func (r *PythonRuntime) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	cmd := exec.CommandContext(ctx, r.pythonBin, "-u", r.script)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("open stdin pipe: %w", err)
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("open stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("open stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start python process: %w", err)
	}

	go copyPrefixed(os.Stderr, stderr, "[python stderr] ")

	scanner := bufio.NewScanner(stdoutPipe)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	r.cmd = cmd
	r.stdin = stdin
	r.stdout = scanner

	ready, err := r.readLocked()
	if err != nil {
		_ = r.stopLocked()
		return fmt.Errorf("read ready response: %w", err)
	}
	if !ready.Ready {
		_ = r.stopLocked()
		return fmt.Errorf("python sent non-ready startup response: %+v", ready)
	}

	fmt.Printf("started Python runtime pid=%d pandas_loaded=%v", ready.ProcessID, ready.PandasLoaded)
	if ready.ImportError != "" {
		fmt.Printf(" import_error=%q", ready.ImportError)
	}
	fmt.Println()
	return nil
}

func (r *PythonRuntime) Call(req WorkerRequest) (WorkerResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.cmd == nil || r.stdin == nil || r.stdout == nil {
		return WorkerResponse{}, errors.New("python runtime is not running")
	}

	payload, err := json.Marshal(req)
	if err != nil {
		return WorkerResponse{}, fmt.Errorf("marshal request: %w", err)
	}
	if _, err := fmt.Fprintln(r.stdin, string(payload)); err != nil {
		return WorkerResponse{}, fmt.Errorf("write request: %w", err)
	}

	resp, err := r.readLocked()
	if err != nil {
		return WorkerResponse{}, err
	}
	if resp.ID != req.ID {
		return WorkerResponse{}, fmt.Errorf("protocol mismatch: request id %q response id %q", req.ID, resp.ID)
	}
	if !resp.OK {
		return resp, fmt.Errorf("worker error: %s", resp.Error)
	}
	return resp, nil
}

func (r *PythonRuntime) Restart(ctx context.Context) error {
	r.Stop()
	return r.Start(ctx)
}

func (r *PythonRuntime) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	_ = r.stopLocked()
}

func (r *PythonRuntime) readLocked() (WorkerResponse, error) {
	if !r.stdout.Scan() {
		if err := r.stdout.Err(); err != nil {
			return WorkerResponse{}, fmt.Errorf("read stdout: %w", err)
		}
		return WorkerResponse{}, errors.New("python runtime closed stdout")
	}

	var resp WorkerResponse
	line := r.stdout.Text()
	if err := json.Unmarshal([]byte(line), &resp); err != nil {
		return WorkerResponse{}, fmt.Errorf("decode response %q: %w", line, err)
	}
	return resp, nil
}

func (r *PythonRuntime) stopLocked() error {
	if r.stdin != nil {
		_ = r.stdin.Close()
	}

	if r.cmd != nil && r.cmd.Process != nil {
		done := make(chan error, 1)
		go func() {
			done <- r.cmd.Wait()
		}()

		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			_ = r.cmd.Process.Kill()
			<-done
		}
	}

	r.cmd = nil
	r.stdin = nil
	r.stdout = nil
	return nil
}

func pythonBin() string {
	if v := os.Getenv("PYTHON_BIN"); v != "" {
		return v
	}
	return "python"
}

func copyPrefixed(dst io.Writer, src io.Reader, prefix string) {
	scanner := bufio.NewScanner(src)
	for scanner.Scan() {
		fmt.Fprintln(dst, prefix+scanner.Text())
	}
}

func mustCall(runtime *PythonRuntime, req WorkerRequest) WorkerResponse {
	resp, err := runtime.Call(req)
	if err != nil {
		fatalf("%s failed: %v", req.ID, err)
	}
	return resp
}

func verifySamePID(responses []WorkerResponse) {
	if len(responses) == 0 {
		return
	}

	pid := responses[0].ProcessID
	for _, resp := range responses[1:] {
		if resp.ProcessID != pid {
			fatalf("reuse check failed: got pids %d and %d", pid, resp.ProcessID)
		}
	}
	fmt.Printf("reuse check: OK, %d formula jobs used warm Python pid %d\n", len(responses), pid)
}

func fatalf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "sandbox failed: "+format+"\n", args...)
	os.Exit(1)
}

func sampleBars() []Bar {
	return []Bar{
		{Timestamp: "2026-06-12T12:00:00Z", Open: 100, High: 103, Low: 99, Close: 101, Volume: 1000},
		{Timestamp: "2026-06-12T13:00:00Z", Open: 101, High: 104, Low: 100, Close: 103, Volume: 1200},
		{Timestamp: "2026-06-12T14:00:00Z", Open: 103, High: 108, Low: 102, Close: 107, Volume: 1500},
	}
}
