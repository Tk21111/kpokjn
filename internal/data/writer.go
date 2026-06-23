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

type WriteRequest struct {
	SQL   string
	Args  []any
	Reply chan error
}

type Writer struct {
	gate sync.RWMutex

	db          *sql.DB
	ch          chan *WriteRequest
	closeSignal chan struct{}
	done        chan struct{}
}

func NewWriter(ctx context.Context, dbName string) (*Writer, error) {

	projectDir, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("could not get working directory: %w", err)
	}

	// 2. Construct the path to the 'data' folder
	dataDir := filepath.Join(projectDir, "data")

	dbPath := filepath.Join(dataDir, dbName)

	// Ensure parent directory exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create db directory %s: %w", dataDir, err)
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
		db:          db,
		ch:          make(chan *WriteRequest, 256),
		closeSignal: make(chan struct{}),
		done:        make(chan struct{}),
	}

	// Apply schema migrations
	if err := w.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	// Start the single writer goroutine
	go w.Run()

	logx.Info("SQLite writer started")
	return w, nil
}

// submit write req without return err if fail
func (w *Writer) Submit(sql string, args ...any) {

	fmt.Println("wwwwwwwwwwwwwwwwwwwwwwwwwwwwww")
	w.ch <- &WriteRequest{
		SQL:  sql,
		Args: args,
	}
}

func (w *Writer) Exec(sql string, args ...any) error {

	reply := make(chan error, 1)
	select {
	case w.ch <- &WriteRequest{
		SQL:   sql,
		Args:  args,
		Reply: reply,
	}:
		return <-reply
	case <-w.closeSignal:
		return fmt.Errorf("writer down")
	}
}

func (w *Writer) Close() error {

	close(w.closeSignal)
	<-w.done
	return w.db.Close()
}

func (w *Writer) Run() {
	defer close(w.done)

	var tx *sql.Tx
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	txCount := 0
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

	handle := func(req *WriteRequest) {
		if tx == nil {
			var err error
			tx, err = w.db.Begin()
			if err != nil {
				logx.Errorf("SQLite begin transaction error: %v", err)
				req.Reply <- err
				return
			}
			txCount = 0
		}
		_, err := tx.Exec(req.SQL, req.Args...)
		fmt.Println(txCount)
		if err != nil {
			logx.Errorf("SQLite exec error: %s | %v | args=%v", req.SQL, err, req.Args)
			req.Reply <- fmt.Errorf("exec %s: %w", truncate(req.SQL, 80), err)
		} else {
			txCount++
			req.Reply <- nil
		}
		if txCount >= 10 {
			commitTx("batch limit")
		}
	}

	for {
		select {
		case <-w.closeSignal:
			for req := range w.ch {
				handle(req)
			}
			commitTx("final (shutdown)")
		case <-ticker.C:
			commitTx("time limit")
		case req := <-w.ch:
			handle(req)
		}
	}
}

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

func (w *Writer) QueryRow(query string, args ...any) *sql.Row {
	return w.db.QueryRow(query, args...)
}

func (w *Writer) Query(query string, args ...any) (*sql.Rows, error) {
	return w.db.Query(query, args...)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
