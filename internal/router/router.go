// Package router runs a tenant's transaction on the shard that owns the tenant.
//
// Every write transaction starts by taking the tenant's fence lock (a shared advisory lock) on that
// shard and then reading the tenant's fence row. A move freezes the tenant by taking the same lock
// exclusively and setting the fence row's frozen flag: the exclusive lock waits for every in-flight
// writer to finish, and new writers queue behind it. After that, writers see the fence frozen or the
// tenant moved, and the router waits, re-reads the directory and goes to the new shard. No write can land on a shard that no longer owns the
// tenant, whatever any cache says.
package router

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/lib/pq"

	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/directory"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/schema"
)

// ErrMoving is returned when a tenant stays frozen longer than the router waits.
var ErrMoving = errors.New("tenant is being moved")

// errStale means the shard does not own the tenant (any more): refresh and retry.
var errStale = errors.New("stale route")

// Options tune retries. Zero values get defaults.
type Options struct {
	// MaxWait bounds how long a write waits for a frozen tenant before giving up.
	MaxWait time.Duration
	// Backoff is the first wait between attempts; it doubles up to MaxBackoff, with jitter.
	Backoff    time.Duration
	MaxBackoff time.Duration
	// Pool settings per shard.
	MaxOpenConns int
}

func (o Options) withDefaults() Options {
	if o.MaxWait == 0 {
		o.MaxWait = 10 * time.Second
	}
	if o.Backoff == 0 {
		o.Backoff = 2 * time.Millisecond
	}
	if o.MaxBackoff == 0 {
		o.MaxBackoff = 100 * time.Millisecond
	}
	if o.MaxOpenConns == 0 {
		o.MaxOpenConns = 32
	}
	return o
}

// Router owns one connection pool per shard.
type Router struct {
	dir  *directory.Directory
	opts Options

	mu    sync.RWMutex
	pools map[string]*sql.DB
	dsns  map[string]string

	// Metrics hooks (optional).
	OnRetry func(reason string)
}

// New creates a router. Shards are loaded from the directory lazily.
func New(dir *directory.Directory, opts Options) *Router {
	return &Router{dir: dir, opts: opts.withDefaults(), pools: map[string]*sql.DB{}, dsns: map[string]string{}}
}

// Directory returns the directory the router reads.
func (r *Router) Directory() *directory.Directory { return r.dir }

func (r *Router) pool(ctx context.Context, shardID string) (*sql.DB, error) {
	r.mu.RLock()
	p, ok := r.pools[shardID]
	r.mu.RUnlock()
	if ok {
		return p, nil
	}
	shards, err := r.dir.Shards(ctx)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range shards {
		r.dsns[s.ID] = s.DSN
	}
	dsn, ok := r.dsns[shardID]
	if !ok {
		return nil, fmt.Errorf("unknown shard %s", shardID)
	}
	if p, ok := r.pools[shardID]; ok {
		return p, nil
	}
	p, err = sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	p.SetMaxOpenConns(r.opts.MaxOpenConns)
	p.SetMaxIdleConns(r.opts.MaxOpenConns)
	r.pools[shardID] = p
	return p, nil
}

// Pool exposes a shard's pool (for reads that do not need the fence, and for tools).
func (r *Router) Pool(ctx context.Context, shardID string) (*sql.DB, error) {
	return r.pool(ctx, shardID)
}

// Close closes all pools.
func (r *Router) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range r.pools {
		_ = p.Close()
	}
}

// Tx runs fn in a transaction on the shard that owns accountID, behind the fence. fn may run more
// than once (after a stale route, a deadlock or a serialization failure), so it must not have side
// effects outside the transaction.
func (r *Router) Tx(ctx context.Context, accountID string, fn func(tx *sql.Tx) error) (shardID string, err error) {
	deadline := time.Now().Add(r.opts.MaxWait)
	backoff := r.opts.Backoff
	for attempt := 0; ; attempt++ {
		entry, err := r.dir.Lookup(ctx, accountID)
		if err != nil {
			return "", err
		}
		err = r.attempt(ctx, entry.ShardID, accountID, fn)
		switch {
		case err == nil:
			return entry.ShardID, nil
		case errors.Is(err, errStale), errors.Is(err, ErrMoving), retryable(err) != "":
			reason := "stale"
			if errors.Is(err, ErrMoving) {
				reason = "frozen"
			} else if r := retryable(err); r != "" {
				reason = r
			}
			if r.OnRetry != nil {
				r.OnRetry(reason)
			}
			r.dir.Invalidate(accountID)
			if time.Now().After(deadline) {
				return "", fmt.Errorf("%w: %s gave up after %s", ErrMoving, accountID, r.opts.MaxWait)
			}
			sleep := backoff/2 + time.Duration(rand.Int64N(int64(backoff)))
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(sleep):
			}
			backoff = min(backoff*2, r.opts.MaxBackoff)
		default:
			return "", err
		}
	}
}

// retryable names PostgreSQL errors after which the whole transaction can simply run again.
func retryable(err error) string {
	var pe *pq.Error
	if errors.As(err, &pe) {
		switch pe.Code {
		case "40P01":
			return "deadlock"
		case "40001":
			return "serialization"
		}
	}
	return ""
}

func (r *Router) attempt(ctx context.Context, shardID, accountID string, fn func(tx *sql.Tx) error) error {
	db, err := r.pool(ctx, shardID)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	// Two statements on purpose: the fence row must be read with a snapshot taken after the lock.
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock_shared($1, hashtext($2))`, schema.FenceLockSpace, accountID); err != nil {
		return err
	}
	var frozen bool
	var movedTo sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT frozen, moved_to FROM tenant_fence WHERE account_id = $1`, accountID).Scan(&frozen, &movedTo)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return errStale // this shard has never had (or no longer has) the tenant
	case err != nil:
		return err
	case movedTo.Valid:
		return errStale
	case frozen:
		return ErrMoving
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
