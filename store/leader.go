package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Leadership holds a Postgres session-level advisory lock, making the holder
// the single active scheduler/timer leader. The lock is released when Release
// is called or when the underlying connection (and thus session) closes — so a
// crashed leader automatically yields to a standby (spec: single-active
// scheduler assumption + SPOF mitigation).
type Leadership struct {
	conn *pgxpool.Conn
	key  int64
}

// AcquireLeadership attempts to take the advisory lock identified by key. It
// returns ok=false (and no Leadership) if another instance already holds it.
func AcquireLeadership(ctx context.Context, pool *pgxpool.Pool, key int64) (*Leadership, bool, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, false, err
	}
	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&got); err != nil {
		conn.Release()
		return nil, false, fmt.Errorf("try advisory lock: %w", err)
	}
	if !got {
		conn.Release()
		return nil, false, nil
	}
	return &Leadership{conn: conn, key: key}, true, nil
}

// Release unlocks and returns the connection to the pool.
func (l *Leadership) Release(ctx context.Context) {
	if l == nil || l.conn == nil {
		return
	}
	_, _ = l.conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, l.key)
	l.conn.Release()
	l.conn = nil
}

// Healthy reports whether the leadership connection is still usable.
func (l *Leadership) Healthy(ctx context.Context) bool {
	if l == nil || l.conn == nil {
		return false
	}
	return l.conn.Ping(ctx) == nil
}
