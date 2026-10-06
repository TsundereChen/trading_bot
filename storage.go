package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// Repository keeps database-specific SQL out of trading and HTTP code.
// A PostgreSQL adapter must preserve Commit's transaction boundary.
type Repository interface {
	Load(context.Context) (State, error)
	Commit(context.Context, State, string, any) error
	Events(context.Context) ([]Event, error)
	Prune(context.Context, time.Time, int) error
	Close() error
}

type Event struct {
	ID   int64           `json:"id"`
	Time string          `json:"time"`
	Kind string          `json:"kind"`
	Data json.RawMessage `json:"data"`
}

type SQLite struct {
	db    *sql.DB
	lease *os.File
}

func OpenSQLite(path string) (*SQLite, error) {
	// The lock and SQLite must refer to exactly the same filesystem path.
	// Driver URI/query syntax can otherwise lock a different literal filename.
	if strings.HasPrefix(path, "file:") || strings.Contains(path, "?") {
		return nil, fmt.Errorf("DATABASE_PATH must be a filesystem path, not a SQLite URI or DSN")
	}
	// Lock the database inode, not a removable sidecar. Symlink aliases acquire
	// the same lock. Closing it releases ownership even after a process crash.
	var lease *os.File
	if path != ":memory:" {
		var err error
		lease, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return nil, err
		}
		if err = lockDatabase(lease); err != nil {
			lease.Close()
			return nil, fmt.Errorf("cannot own SQLite database (another bot may be running): %w", err)
		}
		if err = lease.Chmod(0600); err != nil {
			lease.Close()
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		if lease != nil {
			lease.Close()
		}
		return nil, err
	}
	db.SetMaxOpenConns(1)
	for _, statement := range []string{
		`PRAGMA journal_mode=WAL`, `PRAGMA busy_timeout=5000`, `PRAGMA foreign_keys=ON`,
		`CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY)`,
		`CREATE TABLE IF NOT EXISTS bot_state (id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, time TEXT NOT NULL, kind TEXT NOT NULL, payload TEXT NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS events_time_id ON events(time,id)`,
		`INSERT OR IGNORE INTO schema_migrations(version) VALUES(1)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			db.Close()
			if lease != nil {
				lease.Close()
			}
			return nil, fmt.Errorf("initialize SQLite: %w", err)
		}
	}
	return &SQLite{db: db, lease: lease}, nil
}

func (s *SQLite) Load(ctx context.Context) (State, error) {
	var state State
	var payload string
	err := s.db.QueryRowContext(ctx, `SELECT payload FROM bot_state WHERE id=1`).Scan(&payload)
	if err == sql.ErrNoRows {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	err = json.Unmarshal([]byte(payload), &state)
	return state, err
}

func (s *SQLite) Commit(ctx context.Context, state State, kind string, data any) error {
	payload, err := json.Marshal(state)
	if err != nil {
		return err
	}
	event, err := json.Marshal(data)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO bot_state(id,payload) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload`, string(payload)); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO events(time,kind,payload) VALUES(?,?,?)`, time.Now().UTC().Format(time.RFC3339Nano), kind, string(event)); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLite) Events(ctx context.Context) ([]Event, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,time,kind,payload FROM events ORDER BY id DESC LIMIT 100`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []Event{}
	for rows.Next() {
		var e Event
		var payload string
		if err := rows.Scan(&e.ID, &e.Time, &e.Kind, &payload); err != nil {
			return nil, err
		}
		e.Data = json.RawMessage(payload)
		result = append(result, e)
	}
	return result, rows.Err()
}

// Maintenance is batched so pruning an old database does not monopolize the
// one SQLite connection. Order intents live in bot_state and are never pruned.
func (s *SQLite) Prune(ctx context.Context, cutoff time.Time, keep int) error {
	if keep < 1 {
		return fmt.Errorf("audit retention count must be positive")
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM events WHERE id IN (
		SELECT id FROM events WHERE time < ?
		OR id < COALESCE((SELECT id FROM events ORDER BY id DESC LIMIT 1 OFFSET ?),0)
		ORDER BY id LIMIT 1000)`, cutoff.UTC().Format(time.RFC3339Nano), keep-1)
	return err
}

func (s *SQLite) Close() error {
	err := s.db.Close()
	if s.lease != nil {
		if closeErr := s.lease.Close(); err == nil {
			err = closeErr
		}
	}
	return err
}
