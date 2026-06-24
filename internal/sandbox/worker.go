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
)

type Worker struct {
	id   int
	cmd  *exec.Cmd
	enc  *json.Encoder
	scan *bufio.Scanner
	mu   sync.Mutex
}

type WorkerState struct {
	worker     *Worker
	formulaID  string
	jobCh      chan domain.ProcessJob
	queueDepth atomic.Int32
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
