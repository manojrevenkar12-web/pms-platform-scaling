// Package cluster wires the control database, the shards, the directory and the router together,
// for the API, the reshard tool, the benchmark and the tests.
package cluster

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"sync"
	"time"

	_ "github.com/lib/pq" // driver

	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/directory"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/mover"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/router"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/schema"
)

// Cluster is an open platform.
type Cluster struct {
	Control *sql.DB
	Dir     *directory.Directory
	Router  *router.Router
	mu      sync.Mutex
	shards  map[string]directory.Shard
	admin   map[string]*sql.DB // small pools for tools, separate from the router's
}

// Options for Open.
type Options struct {
	DirectoryTTL time.Duration
	Router       router.Options
}

// Open connects to the control database and every shard registered there.
func Open(ctx context.Context, controlDSN string, o Options) (*Cluster, error) {
	if o.DirectoryTTL == 0 {
		o.DirectoryTTL = 5 * time.Second
	}
	control, err := sql.Open("postgres", controlDSN)
	if err != nil {
		return nil, err
	}
	if err := control.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("control database: %w", err)
	}
	dir := directory.New(control, o.DirectoryTTL)
	c := &Cluster{Control: control, Dir: dir, Router: router.New(dir, o.Router), shards: map[string]directory.Shard{}, admin: map[string]*sql.DB{}}
	if err := c.reload(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Cluster) reload(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reloadLocked(ctx)
}

func (c *Cluster) reloadLocked(ctx context.Context) error {
	shards, err := c.Dir.Shards(ctx)
	if err != nil {
		return err
	}
	for _, s := range shards {
		c.shards[s.ID] = s
	}
	return nil
}

// Close closes every pool.
func (c *Cluster) Close() {
	c.Router.Close()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, db := range c.admin {
		_ = db.Close()
	}
	_ = c.Control.Close()
}

// ShardIDs lists the shards in order.
func (c *Cluster) ShardIDs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	ids := make([]string, 0, len(c.shards))
	for id := range c.shards {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Admin returns a small pool to a shard for tools (moves, checks).
func (c *Cluster) Admin(ctx context.Context, shardID string) (*sql.DB, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if db, ok := c.admin[shardID]; ok {
		return db, nil
	}
	s, ok := c.shards[shardID]
	if !ok {
		if err := c.reloadLocked(ctx); err != nil {
			return nil, err
		}
		if s, ok = c.shards[shardID]; !ok {
			return nil, fmt.Errorf("unknown shard %s", shardID)
		}
	}
	db, err := sql.Open("postgres", s.DSN)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	c.admin[shardID] = db
	return db, nil
}

// Init creates the control schema, registers the shards and creates the shard schema on each.
func Init(ctx context.Context, controlDSN string, shards []directory.Shard) error {
	control, err := sql.Open("postgres", controlDSN)
	if err != nil {
		return err
	}
	defer control.Close()
	if err := schema.Apply(ctx, control, schema.Control); err != nil {
		return err
	}
	for _, s := range shards {
		db, err := sql.Open("postgres", s.DSN)
		if err != nil {
			return err
		}
		err = schema.Apply(ctx, db, schema.Shard)
		_ = db.Close()
		if err != nil {
			return fmt.Errorf("shard %s: %w", s.ID, err)
		}
		if err := directory.AddShard(ctx, control, s); err != nil {
			return err
		}
	}
	return nil
}

// Place puts a new tenant on a shard.
func (c *Cluster) Place(ctx context.Context, accountID, shardID string) error {
	db, err := c.Admin(ctx, shardID)
	if err != nil {
		return err
	}
	return directory.Place(ctx, c.Control, db, accountID, shardID)
}

// Owner returns the shard the directory says owns the tenant (no cache).
func (c *Cluster) Owner(ctx context.Context, accountID string) (string, error) {
	e, err := c.Dir.Refresh(ctx, accountID)
	return e.ShardID, err
}

// NewMove prepares a move of a tenant to target. id must be unique per move.
func (c *Cluster) NewMove(ctx context.Context, id, accountID, target string, cfg mover.Config) (*mover.Move, error) {
	source, err := c.Owner(ctx, accountID)
	if err != nil {
		return nil, err
	}
	// A resumed move keeps its original source, even though the directory may have flipped.
	var recorded string
	if err := c.Control.QueryRowContext(ctx, `SELECT source FROM moves WHERE move_id = $1`, id).Scan(&recorded); err == nil {
		source = recorded
	}
	src, err := c.Admin(ctx, source)
	if err != nil {
		return nil, err
	}
	dst, err := c.Admin(ctx, target)
	if err != nil {
		return nil, err
	}
	return &mover.Move{
		ID: id, AccountID: accountID, Control: c.Control, Config: cfg,
		Source: mover.Shard{ID: source, DB: src, ReplConn: c.shard(source).ReplConn},
		Target: mover.Shard{ID: target, DB: dst, ReplConn: c.shard(target).ReplConn},
	}, nil
}

func (c *Cluster) shard(id string) directory.Shard {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.shards[id]
}
