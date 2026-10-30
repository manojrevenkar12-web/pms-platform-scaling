// Package mover moves one tenant from one shard to another while the tenant keeps writing.
//
// It uses PostgreSQL logical replication with a row filter, so only the tenant's rows travel:
//
//	planned      a row in the control database's moves table (one active move per tenant)
//	published    source: CREATE PUBLICATION ... FOR TABLE ... WHERE (account_id = '<tenant>')
//	subscribed   target: CREATE SUBSCRIPTION (initial copy of the tenant's rows, then streaming)
//	copied       every table of the subscription is in sync (pg_subscription_rel state 'r')
//	caught_up    the target has received the source's WAL up to where it was when this began
//	frozen       source: the tenant's fence lock taken exclusively (waits for in-flight writers,
//	             new ones queue behind it), then tenant_fence.frozen = true (see package router)
//	verified     the freeze has arrived on the target (so every earlier write has), and the
//	             tenant's rows hash the same on both shards. On a mismatch the move is aborted.
//	flipped      control: tenants.shard_id = target, epoch + 1
//	released     target: subscription dropped, fence unfrozen; source: fence moved_to = target,
//	             publication dropped. Writers resume on the target.
//	cleaned      source: the tenant's rows deleted (the fence row stays as a tombstone)
//
// The write pause is frozen -> released. Every step is idempotent and recorded, so a crashed
// mover is simply run again; a move that has not flipped can be aborted cleanly.
package mover

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/schema"
)

// Steps in order.
var Steps = []string{"planned", "published", "subscribed", "copied", "caught_up", "frozen", "verified", "flipped", "released", "cleaned", "finished"}

// ErrCrash is returned when a test asks the mover to stop after a step (to prove resumability).
var ErrCrash = errors.New("injected crash")

var safeID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,48}$`)

// Config tunes a move. Zero values get defaults.
type Config struct {
	PollEvery    time.Duration // how often to check replication progress
	StepTimeout  time.Duration // how long any waiting step may take
	CleanupAfter bool          // delete the tenant's rows on the source when done
	// SkipChecksum trusts commit ordering alone at the verified step. The checksum makes the
	// write pause grow with the tenant's size; skipping it is for very large tenants, after a
	// checksum-verified rehearsal.
	SkipChecksum bool
	// OnStep is called after each step finishes (for logs and metrics).
	OnStep func(step string, took time.Duration)
	// CrashAfter stops the mover right after the named step (tests only).
	CrashAfter string
}

func (c Config) withDefaults() Config {
	if c.PollEvery == 0 {
		c.PollEvery = 10 * time.Millisecond
	}
	if c.StepTimeout == 0 {
		c.StepTimeout = 2 * time.Minute
	}
	return c
}

// Shard is what the mover needs to know about a shard.
type Shard struct {
	ID       string
	DB       *sql.DB
	ReplConn string // connection string the other shard uses to reach this one
}

// Move is one tenant move.
type Move struct {
	ID        string
	AccountID string
	Source    Shard
	Target    Shard
	Control   *sql.DB
	Config    Config

	timings map[string]float64
	started time.Time
}

// Result summarises a finished move.
type Result struct {
	MoveID      string             `json:"move_id"`
	AccountID   string             `json:"account_id"`
	Source      string             `json:"source"`
	Target      string             `json:"target"`
	WritePause  time.Duration      `json:"write_pause_ns"`
	Total       time.Duration      `json:"total_ns"`
	StepSeconds map[string]float64 `json:"step_seconds"`
}

func (m *Move) name() string { return "mv_" + strings.ReplaceAll(m.ID, "-", "_") }

// Run executes (or resumes) the move to the end.
func (m *Move) Run(ctx context.Context) (Result, error) {
	m.Config = m.Config.withDefaults()
	if !safeID.MatchString(m.AccountID) || !safeID.MatchString(m.ID) {
		return Result{}, fmt.Errorf("account and move ids must match %s", safeID)
	}
	if m.Source.ID == m.Target.ID {
		return Result{}, errors.New("source and target are the same shard")
	}
	m.timings = map[string]float64{}
	m.started = time.Now()
	step, err := m.ensurePlanned(ctx)
	if err != nil {
		return Result{}, err
	}
	for i := indexOf(step) + 1; i < len(Steps); i++ {
		next := Steps[i]
		t0 := time.Now()
		if err := m.do(ctx, next); err != nil {
			if errors.Is(err, ErrMismatch) {
				if aerr := Abort(context.Background(), m); aerr != nil {
					err = errors.Join(err, fmt.Errorf("abort: %w", aerr))
				}
			}
			_, _ = m.Control.ExecContext(context.Background(), `UPDATE moves SET error = $2, updated_at = now() WHERE move_id = $1`, m.ID, err.Error())
			return Result{}, fmt.Errorf("move %s step %s: %w", m.ID, next, err)
		}
		took := time.Since(t0)
		m.timings[next] = took.Seconds()
		if m.Config.OnStep != nil {
			m.Config.OnStep(next, took)
		}
		if err := m.record(ctx, next); err != nil {
			return Result{}, err
		}
		if m.Config.CrashAfter == next {
			return Result{}, fmt.Errorf("%w after %s", ErrCrash, next)
		}
	}
	return m.result(ctx)
}

func indexOf(step string) int {
	for i, s := range Steps {
		if s == step {
			return i
		}
	}
	return -1
}

func (m *Move) ensurePlanned(ctx context.Context) (string, error) {
	var step string
	err := m.Control.QueryRowContext(ctx, `SELECT step FROM moves WHERE move_id = $1`, m.ID).Scan(&step)
	if err == nil {
		return step, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	var current string
	if err := m.Control.QueryRowContext(ctx, `SELECT shard_id FROM tenants WHERE account_id = $1`, m.AccountID).Scan(&current); err != nil {
		return "", fmt.Errorf("tenant %s: %w", m.AccountID, err)
	}
	if current != m.Source.ID {
		return "", fmt.Errorf("tenant %s lives on %s, not %s", m.AccountID, current, m.Source.ID)
	}
	if _, err := m.Control.ExecContext(ctx, `INSERT INTO moves (move_id, account_id, source, target, step) VALUES ($1,$2,$3,$4,'planned')`, m.ID, m.AccountID, m.Source.ID, m.Target.ID); err != nil {
		return "", fmt.Errorf("plan move (another move of this tenant may be running): %w", err)
	}
	return "planned", nil
}

func (m *Move) record(ctx context.Context, step string) error {
	t, _ := json.Marshal(m.timings)
	extra := ""
	switch step {
	case "frozen":
		extra = ", frozen_at = now()"
	case "flipped":
		extra = ", flipped_at = now()"
	case "finished":
		extra = ", finished_at = now()"
	}
	_, err := m.Control.ExecContext(ctx, `UPDATE moves SET step = $2, updated_at = now(), timings = timings || $3::jsonb, error = NULL`+extra+` WHERE move_id = $1`, m.ID, step, string(t))
	return err
}

func (m *Move) do(ctx context.Context, step string) error {
	switch step {
	case "published":
		return m.publish(ctx)
	case "subscribed":
		return m.subscribe(ctx)
	case "copied":
		return m.waitCopied(ctx)
	case "caught_up":
		return m.waitCaughtUp(ctx)
	case "frozen":
		return m.freeze(ctx)
	case "verified":
		return m.verify(ctx)
	case "flipped":
		_, err := m.Control.ExecContext(ctx, `UPDATE tenants SET shard_id = $2, epoch = epoch + 1, updated_at = now() WHERE account_id = $1 AND shard_id = $3`, m.AccountID, m.Target.ID, m.Source.ID)
		return err
	case "released":
		return m.release(ctx)
	case "cleaned":
		if !m.Config.CleanupAfter {
			return nil
		}
		return m.cleanup(ctx)
	case "finished":
		return nil
	}
	return fmt.Errorf("unknown step %s", step)
}

func (m *Move) publish(ctx context.Context) error {
	var exists bool
	if err := m.Source.DB.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)`, m.name()).Scan(&exists); err != nil || exists {
		return err
	}
	parts := make([]string, len(schema.TenantTables))
	for i, t := range schema.TenantTables {
		parts[i] = fmt.Sprintf("TABLE %s WHERE (account_id = '%s')", t, m.AccountID) // account id validated by safeID
	}
	_, err := m.Source.DB.ExecContext(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR %s", m.name(), strings.Join(parts, ", ")))
	return err
}

func (m *Move) freeze(ctx context.Context) error {
	tx, err := m.Source.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1, hashtext($2))`, schema.FenceLockSpace, m.AccountID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE tenant_fence SET frozen = true WHERE account_id = $1`, m.AccountID); err != nil {
		return err
	}
	return tx.Commit()
}

func (m *Move) subscribe(ctx context.Context) error {
	var exists bool
	if err := m.Target.DB.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_subscription WHERE subname = $1)`, m.name()).Scan(&exists); err != nil || exists {
		return err
	}
	// Leftovers of an earlier aborted move would collide with the initial copy.
	if err := deleteTenant(ctx, m.Target.DB, m.AccountID, true); err != nil {
		return err
	}
	conn := strings.ReplaceAll(m.Source.ReplConn, "'", "''")
	_, err := m.Target.DB.ExecContext(ctx, fmt.Sprintf("CREATE SUBSCRIPTION %s CONNECTION '%s' PUBLICATION %s WITH (copy_data = true, slot_name = '%s')", m.name(), conn, m.name(), m.name()))
	return err
}

func (m *Move) waitCopied(ctx context.Context) error {
	return m.poll(ctx, func() (bool, error) {
		var notReady int
		err := m.Target.DB.QueryRowContext(ctx, `SELECT count(*) FROM pg_subscription_rel r JOIN pg_subscription s ON s.oid = r.srsubid WHERE s.subname = $1 AND r.srsubstate <> 'r'`, m.name()).Scan(&notReady)
		if err != nil {
			return false, err
		}
		var total int
		err = m.Target.DB.QueryRowContext(ctx, `SELECT count(*) FROM pg_subscription_rel r JOIN pg_subscription s ON s.oid = r.srsubid WHERE s.subname = $1`, m.name()).Scan(&total)
		return err == nil && total == len(schema.TenantTables) && notReady == 0, err
	})
}

// waitCaughtUp waits until the target has received the source's WAL up to where it was when the
// step began. It only keeps the freeze short; correctness comes from the verified step.
func (m *Move) waitCaughtUp(ctx context.Context) error {
	var at string
	if err := m.Source.DB.QueryRowContext(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&at); err != nil {
		return err
	}
	return m.poll(ctx, func() (bool, error) {
		var ok sql.NullBool
		err := m.Target.DB.QueryRowContext(ctx, `SELECT bool_or(received_lsn >= $2::pg_lsn) FROM pg_stat_subscription WHERE subname = $1 AND relid IS NULL`, m.name(), at).Scan(&ok)
		return ok.Valid && ok.Bool, err
	})
}

// ErrMismatch means the tenant's data differs between the shards after the freeze arrived.
var ErrMismatch = errors.New("tenant data differs between source and target")

// verify waits for the freeze itself to arrive on the target. The target applies the source's
// commits in commit order and the freeze committed after the tenant's last write, so from then on
// the target has everything. It then compares the tenant's rows on both shards once; a difference
// is a real divergence, never "not there yet".
func (m *Move) verify(ctx context.Context) error {
	if err := m.poll(ctx, func() (bool, error) {
		var frozen bool
		err := m.Target.DB.QueryRowContext(ctx, `SELECT frozen FROM tenant_fence WHERE account_id = $1`, m.AccountID).Scan(&frozen)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return frozen, err
	}); err != nil {
		return err
	}
	if m.Config.SkipChecksum {
		return nil
	}
	var src, dst string
	var srcErr, dstErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); src, srcErr = Checksum(ctx, m.Source.DB, m.AccountID, true) }()
	go func() { defer wg.Done(); dst, dstErr = Checksum(ctx, m.Target.DB, m.AccountID, true) }()
	wg.Wait()
	if err := errors.Join(srcErr, dstErr); err != nil {
		return err
	}
	if src != dst {
		return fmt.Errorf("%w (source %s, target %s)", ErrMismatch, src, dst)
	}
	return nil
}

func (m *Move) release(ctx context.Context) error {
	var exists bool
	if err := m.Target.DB.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_subscription WHERE subname = $1)`, m.name()).Scan(&exists); err != nil {
		return err
	}
	if exists {
		if _, err := m.Target.DB.ExecContext(ctx, "DROP SUBSCRIPTION "+m.name()); err != nil {
			return err
		}
	}
	// Order matters: the subscription is gone, so nothing can overwrite the target's fence again.
	if _, err := m.Target.DB.ExecContext(ctx, `UPDATE tenant_fence SET frozen = false, moved_to = NULL, epoch = epoch + 1 WHERE account_id = $1`, m.AccountID); err != nil {
		return err
	}
	if _, err := m.Source.DB.ExecContext(ctx, `UPDATE tenant_fence SET moved_to = $2 WHERE account_id = $1`, m.AccountID, m.Target.ID); err != nil {
		return err
	}
	_, err := m.Source.DB.ExecContext(ctx, "DROP PUBLICATION IF EXISTS "+m.name())
	return err
}

func (m *Move) cleanup(ctx context.Context) error {
	return deleteTenant(ctx, m.Source.DB, m.AccountID, false)
}

func (m *Move) poll(ctx context.Context, ok func() (bool, error)) error {
	deadline := time.Now().Add(m.Config.StepTimeout)
	for {
		done, err := ok()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %s", m.Config.StepTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(m.Config.PollEvery):
		}
	}
}

func (m *Move) result(ctx context.Context) (Result, error) {
	var frozen, released time.Time
	var timings []byte
	err := m.Control.QueryRowContext(ctx, `SELECT frozen_at, updated_at, timings FROM moves WHERE move_id = $1`, m.ID).Scan(&frozen, &released, &timings)
	if err != nil {
		return Result{}, err
	}
	steps := map[string]float64{}
	_ = json.Unmarshal(timings, &steps)
	pause := 0.0
	for _, s := range []string{"frozen", "verified", "flipped", "released"} {
		pause += steps[s]
	}
	return Result{
		MoveID: m.ID, AccountID: m.AccountID, Source: m.Source.ID, Target: m.Target.ID,
		WritePause: time.Duration(pause * float64(time.Second)), Total: time.Since(m.started), StepSeconds: steps,
	}, nil
}

// Abort undoes a move that has not flipped yet: replication removed, target rows removed,
// source unfrozen. A flipped move cannot be aborted; run it to the end.
func Abort(ctx context.Context, m *Move) error {
	var step string
	if err := m.Control.QueryRowContext(ctx, `SELECT step FROM moves WHERE move_id = $1`, m.ID).Scan(&step); err != nil {
		return err
	}
	if indexOf(step) >= indexOf("flipped") {
		return fmt.Errorf("move %s already flipped (%s); run it to the end instead", m.ID, step)
	}
	if _, err := m.Target.DB.ExecContext(ctx, "DROP SUBSCRIPTION IF EXISTS "+m.name()); err != nil {
		return err
	}
	if _, err := m.Source.DB.ExecContext(ctx, "DROP PUBLICATION IF EXISTS "+m.name()); err != nil {
		return err
	}
	if err := deleteTenant(ctx, m.Target.DB, m.AccountID, true); err != nil {
		return err
	}
	if _, err := m.Source.DB.ExecContext(ctx, `UPDATE tenant_fence SET frozen = false WHERE account_id = $1`, m.AccountID); err != nil {
		return err
	}
	_, err := m.Control.ExecContext(ctx, `UPDATE moves SET step = 'aborted', finished_at = now(), updated_at = now() WHERE move_id = $1`, m.ID)
	return err
}

func deleteTenant(ctx context.Context, db *sql.DB, accountID string, withFence bool) error {
	tables := schema.TenantTables
	for i := len(tables) - 1; i >= 0; i-- {
		if tables[i] == "tenant_fence" && !withFence {
			continue
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE account_id = $1", tables[i]), accountID); err != nil {
			return err
		}
	}
	return nil
}

// Checksum hashes all of a tenant's rows (in key order) on one shard. The fence row is left out
// when ignoreFence is set, because its frozen flag legitimately differs during a move.
func Checksum(ctx context.Context, db *sql.DB, accountID string, ignoreFence bool) (string, error) {
	var parts []string
	for _, t := range schema.TenantTables {
		if t == "tenant_fence" && ignoreFence {
			continue
		}
		var h sql.NullString
		q := fmt.Sprintf(`SELECT md5(string_agg(md5(x::text), '' ORDER BY x::text)) FROM %s x WHERE account_id = $1`, t)
		if err := db.QueryRowContext(ctx, q, accountID).Scan(&h); err != nil {
			return "", err
		}
		parts = append(parts, t+"="+h.String)
	}
	return strings.Join(parts, ";"), nil
}
