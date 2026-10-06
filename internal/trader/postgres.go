package trader

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

//go:embed migrations/postgres/*.sql
var postgresMigrations embed.FS

type Postgres struct {
	mu    sync.Mutex // One leased SQL session: never interleave operations inside a transaction.
	db    *sql.DB
	lease *sql.Conn
}

var _ Repository = (*Postgres)(nil)

func OpenPostgres(ctx context.Context, dsn string) (*Postgres, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	lease, err := db.Conn(ctx)
	if err != nil {
		db.Close()
		return nil, err
	}
	p := &Postgres{db: db, lease: lease}
	var locked bool
	if err = lease.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(741930281)`).Scan(&locked); err != nil || !locked {
		lease.Close()
		db.Close()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("another bot already owns this PostgreSQL database")
	}
	tx, err := lease.BeginTx(ctx, nil)
	if err != nil {
		p.Close()
		return nil, err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY)`)
	if err == nil {
		var version int
		err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM schema_migrations`).Scan(&version)
		if err == nil && version > 1 {
			err = fmt.Errorf("database schema is newer than this bot")
		}
		if err == nil && version < 1 {
			migration, readErr := postgresMigrations.ReadFile("migrations/postgres/001_initial.sql")
			if readErr != nil {
				err = readErr
			} else {
				_, err = tx.ExecContext(ctx, string(migration))
			}
			if err == nil {
				_, err = tx.ExecContext(ctx, `INSERT INTO schema_migrations(version) VALUES(1)`)
			}
		}
	}
	if err == nil {
		_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS events_time_id ON events(time,id)`)
	}
	if err == nil {
		err = tx.Commit()
	}
	if err != nil {
		tx.Rollback()
		p.Close()
		return nil, fmt.Errorf("migrate PostgreSQL: %w", err)
	}
	return p, nil
}

// Use the lease connection for every operation: losing its lock also loses the
// ability to commit, rather than continuing writes on another pooled connection.
func (p *Postgres) Load(ctx context.Context) (State, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var state State
	var payload string
	err := p.lease.QueryRowContext(ctx, `SELECT payload FROM bot_state WHERE id=1`).Scan(&payload)
	if err == sql.ErrNoRows {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	err = json.Unmarshal([]byte(payload), &state)
	return state, err
}
func (p *Postgres) Commit(ctx context.Context, state State, kind string, data any) error {
	stateJSON, err := json.Marshal(state)
	if err != nil {
		return err
	}
	eventJSON, err := json.Marshal(data)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	tx, err := p.lease.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO bot_state(id,payload) VALUES(1,$1::jsonb) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload`, string(stateJSON)); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO events(time,kind,payload) VALUES($1,$2,$3::jsonb)`, time.Now().UTC().Format(time.RFC3339Nano), kind, string(eventJSON)); err != nil {
		return err
	}
	return tx.Commit()
}
func (p *Postgres) Events(ctx context.Context) ([]Event, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	rows, err := p.lease.QueryContext(ctx, `SELECT id,time,kind,payload FROM events ORDER BY id DESC LIMIT 100`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := []Event{}
	for rows.Next() {
		var e Event
		var payload string
		if err := rows.Scan(&e.ID, &e.Time, &e.Kind, &payload); err != nil {
			return nil, err
		}
		e.Data = json.RawMessage(payload)
		events = append(events, e)
	}
	return events, rows.Err()
}
func (p *Postgres) Prune(ctx context.Context, cutoff time.Time, keep int) error {
	if keep < 1 {
		return fmt.Errorf("audit retention count must be positive")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	_, err := p.lease.ExecContext(ctx, `DELETE FROM events WHERE id IN (
		SELECT id FROM events WHERE time < $1
		OR id < COALESCE((SELECT id FROM events ORDER BY id DESC LIMIT 1 OFFSET $2),0)
		ORDER BY id LIMIT 1000)`, cutoff.UTC().Format(time.RFC3339Nano), keep-1)
	return err
}
func (p *Postgres) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = p.lease.ExecContext(ctx, `SELECT pg_advisory_unlock(741930281)`)
	_ = p.lease.Close()
	return p.db.Close()
}
