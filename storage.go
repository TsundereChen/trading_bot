package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// Repository keeps database-specific SQL out of trading and HTTP code.
// A PostgreSQL adapter must preserve Commit's transaction boundary.
type Repository interface {
	Load(context.Context) (State, error)
	Commit(context.Context, State, string, any) error
	Events(context.Context) ([]Event, error)
	Close() error
}

type Event struct {
	ID   int64           `json:"id"`
	Time string          `json:"time"`
	Kind string          `json:"kind"`
	Data json.RawMessage `json:"data"`
}

type SQLite struct{ db *sql.DB }

func OpenSQLite(path string) (*SQLite, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	for _, statement := range []string{
		`PRAGMA journal_mode=WAL`, `PRAGMA busy_timeout=5000`, `PRAGMA foreign_keys=ON`,
		`CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY)`,
		`CREATE TABLE IF NOT EXISTS bot_state (id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, time TEXT NOT NULL, kind TEXT NOT NULL, payload TEXT NOT NULL)`,
		`INSERT OR IGNORE INTO schema_migrations(version) VALUES(1)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			db.Close()
			return nil, fmt.Errorf("initialize SQLite: %w", err)
		}
	}
	return &SQLite{db}, nil
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

func (s *SQLite) Close() error { return s.db.Close() }
