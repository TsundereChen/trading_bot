package trader

import (
	"context"
	"encoding/json"
	"time"
)

// Repository keeps database-specific SQL out of trading and HTTP code.
// Implementations must commit state and its audit event in one transaction.
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
