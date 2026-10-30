package mover_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/booking"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/cluster"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/directory"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/mover"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/router"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/testcluster"
)

var day0 = time.Date(2027, 3, 1, 0, 0, 0, 0, time.UTC)

const nights = 30

func seed(t *testing.T, c *cluster.Cluster, account, shard string) {
	t.Helper()
	ctx := context.Background()
	if err := c.Place(ctx, account, shard); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Router.Tx(ctx, account, func(tx *sql.Tx) error {
		if _, err := tx.Exec(`INSERT INTO properties (account_id, property_id, name) VALUES ($1, 'MUC', 'Munich')`, account); err != nil {
			return err
		}
		for i := 0; i < nights; i++ {
			if err := booking.SetInventory(ctx, tx, account, "MUC", "DBL", day0.AddDate(0, 0, i), 100000, 8); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// writers books continuously for each account until stop is closed and records every
// acknowledged reservation.
type writers struct {
	mu    sync.Mutex
	acked map[string][]string // account -> reservation ids
	fails atomic.Int64
	wg    sync.WaitGroup
	stop  chan struct{}
	last  sync.Map // account -> time of last ack, to measure the longest gap
	gap   sync.Map // account -> longest gap between acks
}

func startWriters(t *testing.T, r *router.Router, accounts []string, perAccount int) *writers {
	w := &writers{acked: map[string][]string{}, stop: make(chan struct{})}
	for _, a := range accounts {
		for g := 0; g < perAccount; g++ {
			w.wg.Add(1)
			go func(a string, g int) {
				defer w.wg.Done()
				rng := rand.New(rand.NewPCG(uint64(g), uint64(len(a))))
				for i := 0; ; i++ {
					select {
					case <-w.stop:
						return
					default:
					}
					from := rng.IntN(nights - 3)
					key := fmt.Sprintf("%s-%d-%d", a, g, i)
					var res booking.Reservation
					_, err := r.Tx(context.Background(), a, func(tx *sql.Tx) error {
						var err error
						res, err = booking.Book(context.Background(), tx, booking.Request{
							AccountID: a, PropertyID: "MUC", RoomType: "DBL", Arrival: day0.AddDate(0, 0, from), Departure: day0.AddDate(0, 0, from+1+rng.IntN(3)),
							GuestRef: "g", RequestKey: key,
						})
						return err
					})
					if err != nil {
						w.fails.Add(1)
						t.Errorf("write for %s failed: %v", a, err)
						continue
					}
					now := time.Now()
					if prev, ok := w.last.Load(a); ok {
						d := now.Sub(prev.(time.Time))
						if old, ok := w.gap.Load(a); !ok || d > old.(time.Duration) {
							w.gap.Store(a, d)
						}
					}
					w.last.Store(a, now)
					w.mu.Lock()
					w.acked[a] = append(w.acked[a], res.ID)
					w.mu.Unlock()
				}
			}(a, g)
		}
	}
	return w
}

func (w *writers) halt() { close(w.stop); w.wg.Wait() }

func (w *writers) longestGap(a string) time.Duration {
	v, _ := w.gap.Load(a)
	d, _ := v.(time.Duration)
	return d
}

func count(t *testing.T, c *cluster.Cluster, shard, table, account string) int {
	t.Helper()
	db, err := c.Admin(context.Background(), shard)
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err := db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM %s WHERE account_id = $1`, table), account).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// checkHome checks that every acknowledged write of the account is on its shard, exactly once,
// and that its availability still adds up.
func checkHome(t *testing.T, c *cluster.Cluster, account, shard string, acked []string) {
	t.Helper()
	db, _ := c.Admin(context.Background(), shard)
	if n := count(t, c, shard, "reservations", account); n != len(acked) {
		t.Errorf("%s on %s: %d reservations, %d acknowledged", account, shard, n, len(acked))
	}
	missing := 0
	for _, id := range acked {
		var ok bool
		if err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM reservations WHERE account_id = $1 AND reservation_id = $2)`, account, id).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		if !ok {
			missing++
		}
	}
	if missing > 0 {
		t.Errorf("%s: %d acknowledged reservations lost", account, missing)
	}
	var held, left int
	if err := db.QueryRow(`SELECT (SELECT count(*) FROM reservation_nights WHERE account_id = $1), (SELECT sum(remaining) FROM availability WHERE account_id = $1)`, account).Scan(&held, &left); err != nil {
		t.Fatal(err)
	}
	if held+left != nights*100000 {
		t.Errorf("%s: %d nights held + %d left != %d loaded", account, held, left, nights*100000)
	}
}

func noReplicationLeft(t *testing.T, c *cluster.Cluster) {
	t.Helper()
	for _, s := range c.ShardIDs() {
		db, _ := c.Admin(context.Background(), s)
		var subs, pubs, slots int
		if err := db.QueryRow(`SELECT (SELECT count(*) FROM pg_subscription), (SELECT count(*) FROM pg_publication), (SELECT count(*) FROM pg_replication_slots)`).Scan(&subs, &pubs, &slots); err != nil {
			t.Fatal(err)
		}
		if subs+pubs+slots != 0 {
			t.Errorf("%s: %d subscriptions, %d publications, %d slots left behind", s, subs, pubs, slots)
		}
	}
}

// The main property: a tenant moves while it and a neighbour keep writing, and no acknowledged
// write is lost or doubled; the neighbour is not paused at all.
func TestMoveUnderLoad(t *testing.T) {
	c := testcluster.Start(t, 2)
	ctx := context.Background()
	seed(t, c, "acc-moving", "shard-0")
	seed(t, c, "acc-staying", "shard-0")
	w := startWriters(t, c.Router, []string{"acc-moving", "acc-staying"}, 4)
	time.Sleep(time.Second)

	m, err := c.NewMove(ctx, "m1", "acc-moving", "shard-1", mover.Config{CleanupAfter: true})
	if err != nil {
		t.Fatal(err)
	}
	res, err := m.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	w.halt()

	if owner, _ := c.Owner(ctx, "acc-moving"); owner != "shard-1" {
		t.Fatalf("owner after move = %s", owner)
	}
	checkHome(t, c, "acc-moving", "shard-1", w.acked["acc-moving"])
	checkHome(t, c, "acc-staying", "shard-0", w.acked["acc-staying"])
	if n := count(t, c, "shard-0", "reservations", "acc-moving"); n != 0 {
		t.Errorf("source still has %d reservations after cleanup", n)
	}
	if n := count(t, c, "shard-0", "tenant_fence", "acc-moving"); n != 1 {
		t.Errorf("source tombstone missing")
	}
	noReplicationLeft(t, c)
	t.Logf("moved %d reservations; pause %s (writer-observed longest gap %s), neighbour's longest gap %s; steps %v",
		len(w.acked["acc-moving"]), res.WritePause, w.longestGap("acc-moving"), w.longestGap("acc-staying"), res.StepSeconds)
	if w.fails.Load() != 0 {
		t.Fatalf("%d writes failed", w.fails.Load())
	}
}

// A router that still believes the old shard owns the tenant (a long cache) must not write there.
func TestStaleRouterFollowsTheMove(t *testing.T) {
	c := testcluster.Start(t, 2)
	ctx := context.Background()
	seed(t, c, "acc-stale", "shard-0")
	stale := router.New(directory.New(c.Control, time.Hour), router.Options{})
	defer stale.Close()
	if _, err := stale.Tx(ctx, "acc-stale", func(*sql.Tx) error { return nil }); err != nil { // warm its cache
		t.Fatal(err)
	}
	m, _ := c.NewMove(ctx, "m-stale", "acc-stale", "shard-1", mover.Config{})
	if _, err := m.Run(ctx); err != nil {
		t.Fatal(err)
	}
	var retries []string
	stale.OnRetry = func(r string) { retries = append(retries, r) }
	shard, err := stale.Tx(ctx, "acc-stale", func(tx *sql.Tx) error {
		_, err := booking.Book(ctx, tx, booking.Request{AccountID: "acc-stale", PropertyID: "MUC", RoomType: "DBL", Arrival: day0, Departure: day0.AddDate(0, 0, 1), GuestRef: "g", RequestKey: "after-move"})
		return err
	})
	if err != nil || shard != "shard-1" {
		t.Fatalf("write went to %s (%v), want shard-1", shard, err)
	}
	if len(retries) != 1 || retries[0] != "stale" {
		t.Fatalf("retries = %v, want one stale", retries)
	}
	if n := count(t, c, "shard-0", "reservations", "acc-stale"); n != 0 {
		t.Fatalf("a write landed on the old shard")
	}
}

// Kill the mover after each step and run it again: it must finish the move correctly.
func TestMoveResumesAfterCrash(t *testing.T) {
	c := testcluster.Start(t, 2)
	ctx := context.Background()
	for i, step := range []string{"published", "subscribed", "copied", "caught_up", "frozen", "verified", "flipped", "released"} {
		account := fmt.Sprintf("acc-crash-%d", i)
		seed(t, c, account, "shard-0")
		w := startWriters(t, c.Router, []string{account}, 2)
		time.Sleep(200 * time.Millisecond)
		id := fmt.Sprintf("crash-%d", i)
		m, _ := c.NewMove(ctx, id, account, "shard-1", mover.Config{CrashAfter: step, CleanupAfter: true})
		if _, err := m.Run(ctx); !errors.Is(err, mover.ErrCrash) {
			t.Fatalf("%s: expected injected crash, got %v", step, err)
		}
		time.Sleep(100 * time.Millisecond)
		m2, err := c.NewMove(ctx, id, account, "shard-1", mover.Config{CleanupAfter: true})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := m2.Run(ctx); err != nil {
			t.Fatalf("resume after %s: %v", step, err)
		}
		time.Sleep(100 * time.Millisecond)
		w.halt()
		checkHome(t, c, account, "shard-1", w.acked[account])
		if w.fails.Load() != 0 {
			t.Fatalf("after %s: %d writes failed", step, w.fails.Load())
		}
	}
	noReplicationLeft(t, c)
}

// A move that has not flipped can be abandoned: the tenant stays where it was, writable, and the
// target is left clean.
func TestAbortBeforeFlip(t *testing.T) {
	c := testcluster.Start(t, 2)
	ctx := context.Background()
	seed(t, c, "acc-abort", "shard-0")
	w := startWriters(t, c.Router, []string{"acc-abort"}, 2)
	time.Sleep(200 * time.Millisecond)
	m, _ := c.NewMove(ctx, "m-abort", "acc-abort", "shard-1", mover.Config{CrashAfter: "frozen"})
	if _, err := m.Run(ctx); !errors.Is(err, mover.ErrCrash) {
		t.Fatal(err)
	}
	if err := mover.Abort(ctx, m); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	w.halt()
	if owner, _ := c.Owner(ctx, "acc-abort"); owner != "shard-0" {
		t.Fatalf("owner after abort = %s", owner)
	}
	checkHome(t, c, "acc-abort", "shard-0", w.acked["acc-abort"])
	for _, table := range []string{"tenant_fence", "reservations", "availability"} {
		if n := count(t, c, "shard-1", table, "acc-abort"); n != 0 {
			t.Errorf("target still has %d %s rows", n, table)
		}
	}
	noReplicationLeft(t, c)
	// A flipped move cannot be aborted.
	m2, _ := c.NewMove(ctx, "m-abort-2", "acc-abort", "shard-1", mover.Config{CrashAfter: "flipped"})
	if _, err := m2.Run(ctx); !errors.Is(err, mover.ErrCrash) {
		t.Fatal(err)
	}
	if err := mover.Abort(ctx, m2); err == nil {
		t.Fatal("aborting a flipped move should fail")
	}
}

func TestOneMoveAtATime(t *testing.T) {
	c := testcluster.Start(t, 3)
	ctx := context.Background()
	seed(t, c, "acc-one", "shard-0")
	m, _ := c.NewMove(ctx, "first", "acc-one", "shard-1", mover.Config{CrashAfter: "published"})
	_, _ = m.Run(ctx)
	other, _ := c.NewMove(ctx, "second", "acc-one", "shard-2", mover.Config{})
	if _, err := other.Run(ctx); err == nil {
		t.Fatal("a second concurrent move of one tenant was allowed")
	}
	if err := mover.Abort(ctx, m); err != nil {
		t.Fatal(err)
	}
}

// If the target's copy differs from the source after the freeze (here: a stray row written to the
// target by hand), the move must not flip; it aborts and the tenant keeps working on the source.
func TestMismatchAbortsTheMove(t *testing.T) {
	c := testcluster.Start(t, 2)
	ctx := context.Background()
	seed(t, c, "acc-diverge", "shard-0")
	m, _ := c.NewMove(ctx, "m-diverge", "acc-diverge", "shard-1", mover.Config{CrashAfter: "caught_up"})
	if _, err := m.Run(ctx); !errors.Is(err, mover.ErrCrash) {
		t.Fatal(err)
	}
	target, _ := c.Admin(ctx, "shard-1")
	if _, err := target.Exec(`INSERT INTO properties (account_id, property_id, name) VALUES ('acc-diverge', 'STRAY', 'written to the target by hand')`); err != nil {
		t.Fatal(err)
	}
	m2, _ := c.NewMove(ctx, "m-diverge", "acc-diverge", "shard-1", mover.Config{})
	if _, err := m2.Run(ctx); !errors.Is(err, mover.ErrMismatch) {
		t.Fatalf("got %v, want ErrMismatch", err)
	}
	if owner, _ := c.Owner(ctx, "acc-diverge"); owner != "shard-0" {
		t.Fatalf("owner = %s after a failed verification", owner)
	}
	if _, err := c.Router.Tx(ctx, "acc-diverge", func(tx *sql.Tx) error {
		_, err := booking.Book(ctx, tx, booking.Request{AccountID: "acc-diverge", PropertyID: "MUC", RoomType: "DBL", Arrival: day0, Departure: day0.AddDate(0, 0, 1), GuestRef: "g", RequestKey: "after-abort"})
		return err
	}); err != nil {
		t.Fatalf("tenant not writable after the abort: %v", err)
	}
	noReplicationLeft(t, c)
}
