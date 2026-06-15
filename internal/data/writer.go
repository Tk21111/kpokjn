package data

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"kpokjn/internal/logx"

	_ "github.com/mattn/go-sqlite3"
)

// WriteRequest is a request to execute a SQL statement via the single writer goroutine.
type WriteRequest struct {
	SQL   string
	Args  []any
	Reply chan error
}

// Writer is the single-writer goroutine manager for SQLite.
// All writes go through the WriteRequest channel to avoid lock contention.
type Writer struct {
	db  *sql.DB
	ctx context.Context
	ch  chan WriteRequest
	mu  sync.Mutex
}

// reed from path -> ping -> create writer -> apply sqlite config -> go run()
func NewWriter(ctx context.Context, dbPath string) (*Writer, error) {
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
		db:  db,
		ch:  make(chan WriteRequest, 256),
		ctx: ctx,
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

// QueryRow executes a read query (not through the writer goroutine).
func (w *Writer) QueryRow(query string, args ...any) *sql.Row {
	return w.db.QueryRow(query, args...)
}

// Query executes a read query (not through the writer goroutine).
func (w *Writer) Query(query string, args ...any) (*sql.Rows, error) {
	return w.db.Query(query, args...)
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	go func() {
		errClosed := fmt.Errorf("sqlite writer is shutting down")
		for {
			select {
			case req, ok := <-w.ch:
				if !ok {
					return
				}
				// Reply to unblock the producer
				if req.Reply != nil {
					req.Reply <- errClosed
				}
			default:
				return
			}
		}
	}()

	return w.db.Close()
}

func (w *Writer) run() {
	var tx *sql.Tx
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop() // ALWAYS stop tickers to prevent memory leaks
	txCount := 0

	// Helper function to handle commits safely and reduce code duplication
	commitTx := func(reason string) {
		if tx != nil {
			if err := tx.Commit(); err != nil {
				logx.Errorf("SQLite %s commit error: %v", reason, err)
			} else {
				logx.Infof("SQLite writer: committed %s transaction (%d statements)", reason, txCount)
			}
			tx = nil
			txCount = 0
		}
	}

	for {
		select {

		// 1. Shutdown requested via context
		case <-w.ctx.Done():
			commitTx("final (shutdown)")
			w.Close()
			return

		// 2. Time limit reached
		case <-ticker.C:
			commitTx("time limit")

		// 3. New Data Arrived
		case req, ok := <-w.ch:
			if !ok {
				// Channel was closed! Commit any pending work before exiting
				commitTx("final (channel closed)")
				return
			}

			// Start a new transaction if one doesn't exist
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

			// Execute the current request BEFORE checking the batch limit
			_, err := tx.Exec(req.SQL, req.Args...)
			if err != nil {
				logx.Errorf("SQLite exec error: %s | %v | args=%v", req.SQL, err, req.Args)
				req.Reply <- fmt.Errorf("exec %s: %w", truncate(req.SQL, 80), err)
			} else {
				txCount++
				req.Reply <- nil
			}

			// Hard 100 write limit
			// We check this AFTER execution so the 100th item isn't discarded
			if txCount >= 100 {
				commitTx("batch limit")
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
