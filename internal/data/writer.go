package data

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	_ "github.com/mattn/go-sqlite3"
	"kpokjn/internal/logx"
)

// WriteRequest is a request to execute a SQL statement via the single writer goroutine.
type WriteRequest struct {
	SQL        string
	Args       []any
	BatchFlush bool
	Reply      chan error
}

// Writer is the single-writer goroutine manager for SQLite.
// All writes go through the WriteRequest channel to avoid lock contention.
type Writer struct {
	db   *sql.DB
	ch   chan WriteRequest
	done chan struct{}
	mu   sync.Mutex
}

// NewWriter opens the SQLite database, applies migrations, and starts the writer goroutine.
func NewWriter(dbPath string) (*Writer, error) {
	// Ensure parent directory exists
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create db directory %s: %w", dir, err)
	}

	logx.Infof("Opening SQLite database at %s", dbPath)

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite %s: %w", dbPath, err)
	}

	// Verify connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}

	w := &Writer{
		db:   db,
		ch:   make(chan WriteRequest, 256),
		done: make(chan struct{}),
	}

	// Apply schema migrations
	if err := w.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	// Start the single writer goroutine
	go w.run()

	logx.Info("SQLite writer started")
	return w, nil
}

// Channel returns the write request channel.
func (w *Writer) Channel() chan<- WriteRequest {
	return w.ch
}

// Write sends a WriteRequest and blocks until the writer processes it.
func (w *Writer) Write(sql string, args ...any) error {
	req := WriteRequest{
		SQL:   sql,
		Args:  args,
		Reply: make(chan error, 1),
	}
	w.ch <- req
	return <-req.Reply
}

// WriteBatch sends multiple WriteRequests in a single transaction.
// The last request should have BatchFlush=true to commit.
func (w *Writer) WriteBatch(reqs []WriteRequest) error {
	for i := range reqs {
		if i < len(reqs)-1 {
			reqs[i].BatchFlush = false
		}
		if reqs[i].Reply == nil {
			reqs[i].Reply = make(chan error, 1)
		}
		w.ch <- reqs[i]
	}
	return <-reqs[len(reqs)-1].Reply
}

// QueryRow executes a read query (not through the writer goroutine).
func (w *Writer) QueryRow(query string, args ...any) *sql.Row {
	return w.db.QueryRow(query, args...)
}

// Query executes a read query (not through the writer goroutine).
func (w *Writer) Query(query string, args ...any) (*sql.Rows, error) {
	return w.db.Query(query, args...)
}

// Close shuts down the writer goroutine and closes the database.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	select {
	case <-w.done:
		return nil
	default:
		close(w.done)
	}

	go func() {
		for range w.ch {
		}
	}()

	return w.db.Close()
}

// run is the single writer goroutine. It processes WriteRequests sequentially.
func (w *Writer) run() {
	var tx *sql.Tx
	txCount := 0

	for {
		select {
		case <-w.done:
			if tx != nil {
				if err := tx.Commit(); err != nil {
					logx.Errorf("SQLite final commit error: %v", err)
				}
				logx.Info("SQLite writer: committed final transaction on shutdown")
			}
			return

		case req, ok := <-w.ch:
			if !ok {
				return
			}

			if req.BatchFlush {
				if tx != nil {
					if err := tx.Commit(); err != nil {
						logx.Errorf("SQLite batch commit error: %v", err)
						req.Reply <- err
					} else {
						logx.Debugf("SQLite batch committed (%d statements)", txCount)
						req.Reply <- nil
					}
					tx = nil
					txCount = 0
				} else {
					req.Reply <- nil
				}
				continue
			}

			if tx == nil {
				var err error
				tx, err = w.db.Begin()
				if err != nil {
					logx.Errorf("SQLite begin transaction error: %v", err)
					req.Reply <- err
					continue
				}
				txCount = 0
			}

			_, err := tx.Exec(req.SQL, req.Args...)
			if err != nil {
				logx.Errorf("SQLite exec error: %s | %v | args=%v", req.SQL, err, req.Args)
				req.Reply <- fmt.Errorf("exec %s: %w", truncate(req.SQL, 80), err)
			} else {
				txCount++
				req.Reply <- nil
			}
		}
	}
}

// migrate creates tables if they don't exist.
func (w *Writer) migrate() error {
	migrations := []string{
		`CREATE TABLE IF NOT EXISTS ohlcv (
			ticker    TEXT    NOT NULL,
			timestamp INTEGER NOT NULL,
			open      REAL    NOT NULL,
			high      REAL    NOT NULL,
			low       REAL    NOT NULL,
			close     REAL    NOT NULL,
			volume    REAL    NOT NULL,
			PRIMARY KEY (ticker, timestamp)
		)`,
		`CREATE TABLE IF NOT EXISTS last_signal (
			ticker         TEXT    PRIMARY KEY,
			triggered_at   INTEGER NOT NULL,
			formula_id     TEXT    NOT NULL,
			formula_version INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS signal_log (
			id             INTEGER PRIMARY KEY AUTOINCREMENT,
			ticker         TEXT    NOT NULL,
			triggered_at   INTEGER NOT NULL,
			formula_id     TEXT    NOT NULL,
			formula_version INTEGER NOT NULL,
			signal_value   INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS bootstrap_state (
			ticker        TEXT    PRIMARY KEY,
			backfill_done INTEGER NOT NULL DEFAULT 0,
			oldest_ts     INTEGER
		)`,
	}

	for _, sql := range migrations {
		if _, err := w.db.Exec(sql); err != nil {
			return fmt.Errorf("migration failed: %s: %w", truncate(sql, 60), err)
		}
	}

	logx.Info("SQLite schema migrations applied")
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
