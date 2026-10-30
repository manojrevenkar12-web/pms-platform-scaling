// Package directory is the shard map: tenant -> shard, read through a small cache.
//
// The cache may be stale; that is safe, because the shard itself has the last word. A write that
// reaches the wrong shard finds the tenant fenced there (frozen during a move, or moved_to set
// after it) and the router refreshes the entry and tries again.
package directory

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrUnknownTenant means the tenant is not in the directory at all.
var ErrUnknownTenant = errors.New("unknown tenant")

// Entry is where a tenant lives.
type Entry struct {
	AccountID string
	ShardID   string
	Epoch     int64
	loadedAt  time.Time
}

// Shard is a database the platform can route to.
type Shard struct {
	ID       string
	DSN      string
	ReplConn string
}

// Directory reads the shard map from the control database.
type Directory struct {
	db  *sql.DB
	ttl time.Duration

	mu    sync.RWMutex
	cache map[string]Entry
}

// New returns a directory over the control database. ttl bounds how long an entry is trusted
// before it is re-read; the fence makes even an old entry safe, ttl only limits wasted retries.
func New(db *sql.DB, ttl time.Duration) *Directory {
	return &Directory{db: db, ttl: ttl, cache: map[string]Entry{}}
}

// Lookup returns the tenant's shard, from the cache when fresh.
func (d *Directory) Lookup(ctx context.Context, accountID string) (Entry, error) {
	d.mu.RLock()
	e, ok := d.cache[accountID]
	d.mu.RUnlock()
	if ok && time.Since(e.loadedAt) < d.ttl {
		return e, nil
	}
	return d.Refresh(ctx, accountID)
}

// Refresh re-reads one tenant from the control database.
func (d *Directory) Refresh(ctx context.Context, accountID string) (Entry, error) {
	e := Entry{AccountID: accountID}
	err := d.db.QueryRowContext(ctx, `SELECT shard_id, epoch FROM tenants WHERE account_id = $1`, accountID).Scan(&e.ShardID, &e.Epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return Entry{}, fmt.Errorf("%w: %s", ErrUnknownTenant, accountID)
	}
	if err != nil {
		return Entry{}, fmt.Errorf("directory lookup %s: %w", accountID, err)
	}
	e.loadedAt = time.Now()
	d.mu.Lock()
	d.cache[accountID] = e
	d.mu.Unlock()
	return e, nil
}

// Invalidate drops a cached entry.
func (d *Directory) Invalidate(accountID string) {
	d.mu.Lock()
	delete(d.cache, accountID)
	d.mu.Unlock()
}

// Shards lists the shards.
func (d *Directory) Shards(ctx context.Context) ([]Shard, error) {
	rows, err := d.db.QueryContext(ctx, `SELECT shard_id, dsn, repl_conn FROM shards ORDER BY shard_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Shard
	for rows.Next() {
		var s Shard
		if err := rows.Scan(&s.ID, &s.DSN, &s.ReplConn); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// AddShard registers a shard.
func AddShard(ctx context.Context, db *sql.DB, s Shard) error {
	_, err := db.ExecContext(ctx, `INSERT INTO shards (shard_id, dsn, repl_conn) VALUES ($1,$2,$3)
		ON CONFLICT (shard_id) DO UPDATE SET dsn = EXCLUDED.dsn, repl_conn = EXCLUDED.repl_conn`, s.ID, s.DSN, s.ReplConn)
	return err
}

// Place puts a new tenant on a shard: the directory row and the fence row on the shard.
func Place(ctx context.Context, control, shard *sql.DB, accountID, shardID string) error {
	if _, err := shard.ExecContext(ctx, `INSERT INTO tenant_fence (account_id) VALUES ($1) ON CONFLICT DO NOTHING`, accountID); err != nil {
		return fmt.Errorf("fence row: %w", err)
	}
	_, err := control.ExecContext(ctx, `INSERT INTO tenants (account_id, shard_id) VALUES ($1,$2) ON CONFLICT (account_id) DO NOTHING`, accountID, shardID)
	return err
}
