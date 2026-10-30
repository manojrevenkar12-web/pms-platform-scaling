// Command bench measures the write path on real PostgreSQL servers it starts itself, and writes
// reports/benchmark.json and reports/BENCHMARK.md. Do not edit those files by hand.
//
//	go run ./cmd/bench            # full run, a few minutes
//	go run ./cmd/bench -quick     # shorter, for CI
//
// Scenarios:
//
//	hot-night    many writers booking the same room type and night, with 1, 4 and 16 slots
//	sell-out     exactly as many requests as rooms, and four times as many, all at once
//	move         tenants moved between shards while they and their neighbours keep writing:
//	             write pause, lost and duplicated writes, neighbours' latency
//	move-size    the write pause against the tenant's size, with and without the checksum
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
	"time"

	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/booking"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/cluster"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/mover"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/pgtest"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/router"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/testcluster"
)

var day0 = time.Date(2027, 6, 1, 0, 0, 0, 0, time.UTC)

// Report is everything one run measured.
type Report struct {
	When       string      `json:"when"`
	Machine    Machine     `json:"machine"`
	Quick      bool        `json:"quick"`
	HotNight   []HotResult `json:"hot_night"`
	SellOut    []SellOut   `json:"sell_out"`
	Moves      MoveSummary `json:"moves"`
	MoveSize   []SizeRow   `json:"move_size"`
	Durability string      `json:"durability"`
}

// Machine describes where the numbers came from.
type Machine struct {
	CPUs     int    `json:"cpus"`
	GOOS     string `json:"goos"`
	Go       string `json:"go"`
	Postgres string `json:"postgres"`
}

// HotResult is one hot-night configuration.
type HotResult struct {
	Slots       int     `json:"slots"`
	Writers     int     `json:"writers"`
	Seconds     float64 `json:"seconds"`
	Bookings    int     `json:"bookings"`
	PerSecond   float64 `json:"per_second"`
	P50ms       float64 `json:"p50_ms"`
	P99ms       float64 `json:"p99_ms"`
	WaitedShare float64 `json:"waited_share"` // bookings that had to queue for a slot
	Deadlocks   int64   `json:"deadlock_retries"`
	Oversold    int     `json:"oversold"`
}

// SellOut is one sell-out race.
type SellOut struct {
	Slots    int `json:"slots"`
	Rooms    int `json:"rooms"`
	Requests int `json:"requests"`
	Sold     int `json:"sold"`
	SoldOut  int `json:"sold_out"`
	Left     int `json:"left"`
	Errors   int `json:"errors"`
}

// MoveRow is one move under load.
type MoveRow struct {
	Reservations      int     `json:"reservations"`
	PauseMs           float64 `json:"pause_ms"`             // mover: freeze to release
	MovingMaxMs       float64 `json:"moving_max_ms"`        // longest write of the moving tenant during the move
	NeighbourMaxMs    float64 `json:"neighbour_max_ms"`     // longest write of any other tenant during the move
	NeighbourP99Ms    float64 `json:"neighbour_p99_ms"`     // during the move
	NeighbourP99BefMs float64 `json:"neighbour_p99_before"` // before the move, same load
	TotalSeconds      float64 `json:"total_seconds"`
	Acked             int     `json:"acked"`
	Lost              int     `json:"lost"`
	Duplicated        int     `json:"duplicated"`
	FailedWrites      int64   `json:"failed_writes"`
}

// MoveSummary aggregates the moves.
type MoveSummary struct {
	Rows         []MoveRow `json:"rows"`
	PauseP50ms   float64   `json:"pause_p50_ms"`
	PauseMaxms   float64   `json:"pause_max_ms"`
	MovingMaxms  float64   `json:"moving_write_max_ms"`
	TotalLost    int       `json:"total_lost"`
	TotalDup     int       `json:"total_duplicated"`
	TotalAcked   int       `json:"total_acked"`
	TotalFailed  int64     `json:"total_failed_writes"`
	TenantsShard int       `json:"tenants_on_source"`
}

// SizeRow is the pause for one tenant size.
type SizeRow struct {
	Reservations int     `json:"reservations"`
	Checksum     bool    `json:"checksum"`
	PauseMs      float64 `json:"pause_ms"`
	VerifyMs     float64 `json:"verify_ms"`
}

func main() {
	quick := flag.Bool("quick", false, "shorter run")
	out := flag.String("out", "reports", "output directory")
	flag.Parse()
	if err := run(*quick, *out); err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(1)
	}
}

func run(quick bool, out string) error {
	if !pgtest.Available() {
		return errors.New("PostgreSQL server binaries not found (set PG_BIN)")
	}
	pgtest.Durable = true
	rep := Report{When: time.Now().UTC().Format(time.RFC3339), Quick: quick, Machine: machine(),
		Durability: "fsync=on, synchronous_commit=on, full_page_writes=on"}
	ctx := context.Background()

	dur := 6 * time.Second
	if quick {
		dur = 2 * time.Second
	}
	if err := withCluster(1, func(c *cluster.Cluster) error {
		for _, slots := range []int{1, 4, 16} {
			for _, writers := range []int{16, 64} {
				r, err := hotNight(ctx, c, slots, writers, dur)
				if err != nil {
					return err
				}
				fmt.Printf("hot-night slots=%d writers=%d: %.0f/s p99 %.1fms waited %.0f%%\n", slots, writers, r.PerSecond, r.P99ms, 100*r.WaitedShare)
				rep.HotNight = append(rep.HotNight, r)
			}
		}
		for _, slots := range []int{1, 16} {
			for _, factor := range []int{1, 4} {
				r, err := sellOut(ctx, c, slots, 400, 400*factor)
				if err != nil {
					return err
				}
				fmt.Printf("sell-out slots=%d rooms=%d requests=%d: sold %d, sold out %d, left %d\n", slots, r.Rooms, r.Requests, r.Sold, r.SoldOut, r.Left)
				rep.SellOut = append(rep.SellOut, r)
			}
		}
		return nil
	}); err != nil {
		return err
	}

	moves := 10
	if quick {
		moves = 4
	}
	if err := withCluster(2, func(c *cluster.Cluster) error {
		s, err := moveUnderLoad(ctx, c, moves)
		rep.Moves = s
		return err
	}); err != nil {
		return err
	}

	sizes := []int{2000, 20000, 100000}
	if quick {
		sizes = []int{2000, 20000}
	}
	if err := withCluster(2, func(c *cluster.Cluster) error {
		for i, n := range sizes {
			for _, checksum := range []bool{true, false} {
				r, err := moveSize(ctx, c, fmt.Sprintf("size-%d-%v", i, checksum), n, checksum)
				if err != nil {
					return err
				}
				fmt.Printf("move-size %d checksum=%v: pause %.0fms\n", n, checksum, r.PauseMs)
				rep.MoveSize = append(rep.MoveSize, r)
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return write(out, rep)
}

func machine() Machine {
	m := Machine{CPUs: runtime.NumCPU(), GOOS: runtime.GOOS + "/" + runtime.GOARCH, Go: runtime.Version()}
	bin := os.Getenv("PG_BIN")
	if bin == "" {
		bin = "/usr/lib/postgresql/16/bin"
	}
	if b, err := exec.Command(filepath.Join(bin, "postgres"), "--version").Output(); err == nil {
		m.Postgres = strings.TrimSpace(string(b))
	}
	return m
}

func withCluster(n int, fn func(*cluster.Cluster) error) error {
	control, servers, stop, err := pgtest.StartServers(os.Getenv("PGTEST_DIR"), n)
	if err != nil {
		return err
	}
	defer stop()
	c, err := testcluster.Setup(context.Background(), control, servers, cluster.Options{DirectoryTTL: time.Second, Router: router.Options{MaxWait: 30 * time.Second, MaxOpenConns: 80}})
	if err != nil {
		return err
	}
	defer c.Close()
	return fn(c)
}

func seed(ctx context.Context, c *cluster.Cluster, account, shard string, nights, rooms, slots int) error {
	if err := c.Place(ctx, account, shard); err != nil {
		return err
	}
	_, err := c.Router.Tx(ctx, account, func(tx *sql.Tx) error {
		if _, err := tx.Exec(`INSERT INTO properties (account_id, property_id, name) VALUES ($1, 'MUC', 'Munich')`, account); err != nil {
			return err
		}
		for i := 0; i < nights; i++ {
			if err := booking.SetInventory(ctx, tx, account, "MUC", "DBL", day0.AddDate(0, 0, i), rooms, slots); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func book(ctx context.Context, r *router.Router, account, key string, from, nights int) (booking.Reservation, error) {
	var res booking.Reservation
	_, err := r.Tx(ctx, account, func(tx *sql.Tx) error {
		var err error
		res, err = booking.Book(ctx, tx, booking.Request{AccountID: account, PropertyID: "MUC", RoomType: "DBL",
			Arrival: day0.AddDate(0, 0, from), Departure: day0.AddDate(0, 0, from+nights), GuestRef: "g", RequestKey: key})
		return err
	})
	return res, err
}

func pct(xs []time.Duration, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := slices.Clone(xs)
	slices.Sort(s)
	i := int(p * float64(len(s)-1))
	return float64(s[i].Microseconds()) / 1000
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func hotNight(ctx context.Context, c *cluster.Cluster, slots, writers int, d time.Duration) (HotResult, error) {
	account := fmt.Sprintf("hot-%d-%d", slots, writers)
	const rooms = 10_000_000
	if err := seed(ctx, c, account, "shard-0", 1, rooms, slots); err != nil {
		return HotResult{}, err
	}
	var deadlocks atomic.Int64
	c.Router.OnRetry = func(reason string) {
		if reason == "deadlock" {
			deadlocks.Add(1)
		}
	}
	defer func() { c.Router.OnRetry = nil }()
	var mu sync.Mutex
	var lat []time.Duration
	var waited, failed atomic.Int64
	stop := time.Now().Add(d)
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			var local []time.Duration
			for i := 0; time.Now().Before(stop); i++ {
				t0 := time.Now()
				res, err := book(ctx, c.Router, account, fmt.Sprintf("%d-%d", w, i), 0, 1)
				if err != nil {
					failed.Add(1)
					continue
				}
				local = append(local, time.Since(t0))
				if res.Waited > 0 {
					waited.Add(1)
				}
			}
			mu.Lock()
			lat = append(lat, local...)
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	if failed.Load() > 0 {
		return HotResult{}, fmt.Errorf("hot-night: %d bookings failed", failed.Load())
	}
	left, err := sumLeft(ctx, c, account)
	if err != nil {
		return HotResult{}, err
	}
	return HotResult{Slots: slots, Writers: writers, Seconds: d.Seconds(), Bookings: len(lat), PerSecond: float64(len(lat)) / d.Seconds(),
		P50ms: pct(lat, 0.5), P99ms: pct(lat, 0.99), WaitedShare: float64(waited.Load()) / float64(max(1, len(lat))),
		Deadlocks: deadlocks.Load(), Oversold: max(0, len(lat)-(rooms-left))}, nil
}

func sumLeft(ctx context.Context, c *cluster.Cluster, account string) (int, error) {
	db, err := c.Admin(ctx, "shard-0")
	if err != nil {
		return 0, err
	}
	var left int
	err = db.QueryRowContext(ctx, `SELECT coalesce(sum(remaining), 0) FROM availability WHERE account_id = $1`, account).Scan(&left)
	return left, err
}

func sellOut(ctx context.Context, c *cluster.Cluster, slots, rooms, requests int) (SellOut, error) {
	account := fmt.Sprintf("sellout-%d-%d", slots, requests)
	if err := seed(ctx, c, account, "shard-0", 1, rooms, slots); err != nil {
		return SellOut{}, err
	}
	var sold, soldOut, errs atomic.Int64
	jobs := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < 64; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				_, err := book(ctx, c.Router, account, fmt.Sprint(i), 0, 1)
				switch {
				case err == nil:
					sold.Add(1)
				case errors.Is(err, booking.ErrSoldOut):
					soldOut.Add(1)
				default:
					errs.Add(1)
				}
			}
		}()
	}
	for i := 0; i < requests; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	left, err := sumLeft(ctx, c, account)
	return SellOut{Slots: slots, Rooms: rooms, Requests: requests, Sold: int(sold.Load()), SoldOut: int(soldOut.Load()), Left: left, Errors: int(errs.Load())}, err
}

// load writes for a set of tenants and records every acknowledged reservation and its latency.
type load struct {
	r     *router.Router
	mu    sync.Mutex
	acked map[string][]string
	lat   map[string][]sample
	fails atomic.Int64
	stop  chan struct{}
	wg    sync.WaitGroup
}

type sample struct {
	at  time.Time
	dur time.Duration
}

func startLoad(ctx context.Context, r *router.Router, accounts []string, perAccount int) *load {
	l := &load{r: r, acked: map[string][]string{}, lat: map[string][]sample{}, stop: make(chan struct{})}
	for ai, a := range accounts {
		for g := 0; g < perAccount; g++ {
			l.wg.Add(1)
			go func(a string, g int) {
				defer l.wg.Done()
				rng := rand.New(rand.NewPCG(uint64(ai), uint64(g)))
				for i := 0; ; i++ {
					select {
					case <-l.stop:
						return
					default:
					}
					t0 := time.Now()
					res, err := book(ctx, r, a, fmt.Sprintf("%s-%d-%d-%d", a, g, i, t0.UnixNano()), rng.IntN(25), 1+rng.IntN(3))
					d := time.Since(t0)
					if err != nil {
						l.fails.Add(1)
						continue
					}
					l.mu.Lock()
					l.acked[a] = append(l.acked[a], res.ID)
					l.lat[a] = append(l.lat[a], sample{t0, d})
					l.mu.Unlock()
					time.Sleep(2 * time.Millisecond) // a steady load, not a saturating one
				}
			}(a, g)
		}
	}
	return l
}

func (l *load) halt() { close(l.stop); l.wg.Wait() }

func (l *load) window(accounts []string, from, to time.Time) []time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []time.Duration
	for _, a := range accounts {
		for _, s := range l.lat[a] {
			if !s.at.Before(from) && s.at.Before(to) {
				out = append(out, s.dur)
			}
		}
	}
	return out
}

func moveUnderLoad(ctx context.Context, c *cluster.Cluster, moves int) (MoveSummary, error) {
	const tenants = 8
	var accounts []string
	for i := 0; i < tenants; i++ {
		a := fmt.Sprintf("tenant-%d", i)
		accounts = append(accounts, a)
		if err := seed(ctx, c, a, "shard-0", 30, 1_000_000, 8); err != nil {
			return MoveSummary{}, err
		}
	}
	l := startLoad(ctx, c.Router, accounts, 3)
	time.Sleep(3 * time.Second)
	var sum MoveSummary
	sum.TenantsShard = tenants
	where := map[string]string{}
	for _, a := range accounts {
		where[a] = "shard-0"
	}
	var pauses []time.Duration
	for i := 0; i < moves; i++ {
		a := accounts[i%2] // tenant-0 and tenant-1 go back and forth; the others stay
		target := "shard-1"
		if where[a] == "shard-1" {
			target = "shard-0"
		}
		before0 := time.Now().Add(-2 * time.Second)
		start := time.Now()
		m, err := c.NewMove(ctx, fmt.Sprintf("bench-%d", i), a, target, mover.Config{CleanupAfter: true})
		if err != nil {
			return sum, err
		}
		res, err := m.Run(ctx)
		if err != nil {
			return sum, err
		}
		end := time.Now()
		where[a] = target
		var others []string
		for _, o := range accounts {
			if o != a {
				others = append(others, o)
			}
		}
		moving := l.window([]string{a}, start, end)
		during := l.window(others, start, end)
		before := l.window(others, before0, start)
		row := MoveRow{PauseMs: ms(res.WritePause), TotalSeconds: res.Total.Seconds(),
			MovingMaxMs: pct(moving, 1), NeighbourMaxMs: pct(during, 1), NeighbourP99Ms: pct(during, 0.99), NeighbourP99BefMs: pct(before, 0.99)}
		db, _ := c.Admin(ctx, target)
		_ = db.QueryRowContext(ctx, `SELECT count(*) FROM reservations WHERE account_id = $1`, a).Scan(&row.Reservations)
		pauses = append(pauses, res.WritePause)
		fmt.Printf("move %d %s -> %s: %d reservations, pause %.0fms, longest write %.0fms, neighbours p99 %.1fms (before %.1fms)\n",
			i, a, target, row.Reservations, row.PauseMs, row.MovingMaxMs, row.NeighbourP99Ms, row.NeighbourP99BefMs)
		sum.Rows = append(sum.Rows, row)
		time.Sleep(2 * time.Second)
	}
	time.Sleep(time.Second)
	l.halt()
	// Every acknowledged write must be on the tenant's current shard, exactly once.
	for _, a := range accounts {
		db, err := c.Admin(ctx, where[a])
		if err != nil {
			return sum, err
		}
		have := map[string]int{}
		rows, err := db.QueryContext(ctx, `SELECT reservation_id::text FROM reservations WHERE account_id = $1`, a)
		if err != nil {
			return sum, err
		}
		for rows.Next() {
			var id string
			_ = rows.Scan(&id)
			have[id]++
		}
		rows.Close()
		for _, id := range l.acked[a] {
			if have[id] == 0 {
				sum.TotalLost++
			}
		}
		for _, n := range have {
			if n > 1 {
				sum.TotalDup += n - 1
			}
		}
		if len(have) != len(l.acked[a]) {
			// rows nobody acknowledged would be writes the client was told failed
			sum.TotalDup += max(0, len(have)-len(l.acked[a]))
		}
		sum.TotalAcked += len(l.acked[a])
	}
	sum.TotalFailed = l.fails.Load()
	sum.PauseP50ms = pct(pauses, 0.5)
	sum.PauseMaxms = pct(pauses, 1)
	for _, r := range sum.Rows {
		sum.MovingMaxms = max(sum.MovingMaxms, r.MovingMaxMs)
	}
	return sum, nil
}

// moveSize loads a tenant with n reservations (no other load) and moves it.
func moveSize(ctx context.Context, c *cluster.Cluster, account string, n int, checksum bool) (SizeRow, error) {
	if err := seed(ctx, c, account, "shard-0", 30, 10_000_000, 8); err != nil {
		return SizeRow{}, err
	}
	db, _ := c.Admin(ctx, "shard-0")
	// Bulk-load reservations directly; the booking path is measured elsewhere.
	if _, err := db.ExecContext(ctx, `
		WITH r AS (
			INSERT INTO reservations (account_id, reservation_id, property_id, room_type, arrival, departure, guest_ref, request_key)
			SELECT $1, gen_random_uuid(), 'MUC', 'DBL', $2::date + (i % 28), $2::date + (i % 28) + 1, 'g', 'bulk-' || i
			FROM generate_series(1, $3) i RETURNING reservation_id, arrival)
		INSERT INTO reservation_nights (account_id, reservation_id, night, slot) SELECT $1, reservation_id, arrival, 0 FROM r`,
		account, day0.Format(booking.DateLayout), n); err != nil {
		return SizeRow{}, err
	}
	m, err := c.NewMove(ctx, "m-"+account, account, "shard-1", mover.Config{SkipChecksum: !checksum, CleanupAfter: true})
	if err != nil {
		return SizeRow{}, err
	}
	res, err := m.Run(ctx)
	if err != nil {
		return SizeRow{}, err
	}
	return SizeRow{Reservations: n, Checksum: checksum, PauseMs: ms(res.WritePause), VerifyMs: res.StepSeconds["verified"] * 1000}, nil
}

func write(dir string, rep Report) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(rep, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, "benchmark.json"), append(b, '\n'), 0o644); err != nil {
		return err
	}
	f, err := os.Create(filepath.Join(dir, "BENCHMARK.md"))
	if err != nil {
		return err
	}
	defer f.Close()
	return mdTemplate.Execute(f, rep)
}

var mdTemplate = template.Must(template.New("md").Funcs(template.FuncMap{
	"pct": func(f float64) string { return fmt.Sprintf("%.0f%%", 100*f) },
	"f1":  func(f float64) string { return fmt.Sprintf("%.1f", f) },
	"f0":  func(f float64) string { return fmt.Sprintf("%.0f", f) },
}).Parse(`# Benchmark

Generated by ` + "`go run ./cmd/bench`" + ` on {{.When}}{{if .Quick}} (quick run){{end}}. Do not edit by hand.

Machine: {{.Machine.CPUs}} CPUs, {{.Machine.GOOS}}, {{.Machine.Go}}, {{.Machine.Postgres}}.
Servers run with {{.Durability}}. All servers share this one machine, so these numbers show
behaviour (contention, pauses, correctness), not the capacity of a production cluster.

## Hot night: one room type, one night, many writers

Each writer books one night as fast as it can for {{with index .HotNight 0}}{{f0 .Seconds}}{{end}} s. "Waited" is the share of
bookings that found every slot with rooms locked and had to queue.

| Slots | Writers | Bookings/s | p50 ms | p99 ms | Waited | Deadlock retries | Oversold |
|---:|---:|---:|---:|---:|---:|---:|---:|
{{range .HotNight}}| {{.Slots}} | {{.Writers}} | {{f0 .PerSecond}} | {{f1 .P50ms}} | {{f1 .P99ms}} | {{pct .WaitedShare}} | {{.Deadlocks}} | {{.Oversold}} |
{{end}}
## Sell-out race

All requests at once from 64 writers, one night.

| Slots | Rooms | Requests | Sold | Sold out | Left | Errors |
|---:|---:|---:|---:|---:|---:|---:|
{{range .SellOut}}| {{.Slots}} | {{.Rooms}} | {{.Requests}} | {{.Sold}} | {{.SoldOut}} | {{.Left}} | {{.Errors}} |
{{end}}
## Moving tenants under load

{{.Moves.TenantsShard}} tenants write continuously (3 writers each); two of them are moved back and forth
between two shards {{len .Moves.Rows}} times. Pause is the mover's freeze-to-release time; "longest write"
is the slowest acknowledged write of the moving tenant during its move, as its client saw it.

| Reservations moved | Pause ms | Longest write ms | Neighbours p99 before ms | Neighbours p99 during ms | Neighbours max ms | Move s |
|---:|---:|---:|---:|---:|---:|---:|
{{range .Moves.Rows}}| {{.Reservations}} | {{f0 .PauseMs}} | {{f0 .MovingMaxMs}} | {{f1 .NeighbourP99BefMs}} | {{f1 .NeighbourP99Ms}} | {{f0 .NeighbourMaxMs}} | {{f1 .TotalSeconds}} |
{{end}}
Pause p50 {{f0 .Moves.PauseP50ms}} ms, max {{f0 .Moves.PauseMaxms}} ms. Acknowledged writes: {{.Moves.TotalAcked}};
lost: {{.Moves.TotalLost}}; duplicated: {{.Moves.TotalDup}}; failed writes: {{.Moves.TotalFailed}}.

## Pause against tenant size

One tenant, no other load. The checksum compares every row of the tenant while it is frozen, so
the pause grows with its size; without it, correctness rests on commit ordering alone.

| Reservations | Checksum | Pause ms | Verify step ms |
|---:|:---:|---:|---:|
{{range .MoveSize}}| {{.Reservations}} | {{if .Checksum}}yes{{else}}no{{end}} | {{f0 .PauseMs}} | {{f0 .VerifyMs}} |
{{end}}`))
