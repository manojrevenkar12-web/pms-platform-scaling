package booking_test

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
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/testcluster"
)

var day0 = time.Date(2026, 12, 28, 0, 0, 0, 0, time.UTC)

func night(i int) time.Time { return day0.AddDate(0, 0, i) }

func seed(t *testing.T, c *cluster.Cluster, account string, nights, rooms, slots int) {
	t.Helper()
	ctx := context.Background()
	if err := c.Place(ctx, account, "shard-0"); err != nil {
		t.Fatal(err)
	}
	_, err := c.Router.Tx(ctx, account, func(tx *sql.Tx) error {
		if _, err := tx.Exec(`INSERT INTO properties (account_id, property_id, name) VALUES ($1, 'MUC', 'Munich Centre')`, account); err != nil {
			return err
		}
		for i := 0; i < nights; i++ {
			if err := booking.SetInventory(ctx, tx, account, "MUC", "DBL", night(i), rooms, slots); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func book(c *cluster.Cluster, account, key string, from, nights int) (booking.Reservation, error) {
	var res booking.Reservation
	_, err := c.Router.Tx(context.Background(), account, func(tx *sql.Tx) error {
		var err error
		res, err = booking.Book(context.Background(), tx, booking.Request{
			AccountID: account, PropertyID: "MUC", RoomType: "DBL", Arrival: night(from), Departure: night(from + nights),
			GuestRef: "guest-" + key, RequestKey: key,
		})
		return err
	})
	return res, err
}

func left(t *testing.T, c *cluster.Cluster, account string, n int) int {
	t.Helper()
	db, _ := c.Admin(context.Background(), "shard-0")
	var v int
	if err := db.QueryRow(`SELECT coalesce(sum(remaining),0) FROM availability WHERE account_id = $1 AND night = $2`, account, night(n).Format(booking.DateLayout)).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestValidate(t *testing.T) {
	ok := booking.Request{AccountID: "a", PropertyID: "p", RoomType: "r", GuestRef: "g", RequestKey: "k", Arrival: night(0), Departure: night(2)}
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(r *booking.Request){
		"no key":        func(r *booking.Request) { r.RequestKey = "" },
		"zero nights":   func(r *booking.Request) { r.Departure = r.Arrival },
		"backwards":     func(r *booking.Request) { r.Departure = night(-1) },
		"too long":      func(r *booking.Request) { r.Departure = night(booking.MaxNights + 1) },
		"no guest":      func(r *booking.Request) { r.GuestRef = "" },
		"key too long":  func(r *booking.Request) { r.RequestKey = string(make([]byte, 129)) },
		"no room type":  func(r *booking.Request) { r.RoomType = "" },
		"no property":   func(r *booking.Request) { r.PropertyID = "" },
		"no account id": func(r *booking.Request) { r.AccountID = "" },
	}
	for name, mutate := range cases {
		r := ok
		mutate(&r)
		if err := r.Validate(); !errors.Is(err, booking.ErrInvalid) {
			t.Errorf("%s: got %v, want ErrInvalid", name, err)
		}
	}
}

// Many more requests than rooms, all at once, for each slot count: exactly the rooms are sold.
func TestNoOversellUnderContention(t *testing.T) {
	c := testcluster.Start(t, 1)
	for _, slots := range []int{1, 8} {
		t.Run(fmt.Sprintf("slots=%d", slots), func(t *testing.T) {
			account := fmt.Sprintf("acc-oversell-%d", slots)
			seed(t, c, account, 1, 50, slots)
			var sold, soldOut, other atomic.Int64
			var wg sync.WaitGroup
			for w := 0; w < 32; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for i := 0; i < 8; i++ {
						_, err := book(c, account, fmt.Sprintf("k-%d-%d", w, i), 0, 1)
						switch {
						case err == nil:
							sold.Add(1)
						case errors.Is(err, booking.ErrSoldOut):
							soldOut.Add(1)
						default:
							other.Add(1)
							t.Errorf("unexpected error: %v", err)
						}
					}
				}(w)
			}
			wg.Wait()
			if sold.Load() != 50 || soldOut.Load() != 256-50 || other.Load() != 0 {
				t.Fatalf("sold %d, sold out %d, other %d; want 50, 206, 0", sold.Load(), soldOut.Load(), other.Load())
			}
			if l := left(t, c, account, 0); l != 0 {
				t.Fatalf("%d rooms left, want 0", l)
			}
		})
	}
}

// Exactly as many requests as rooms, all at once: every one must succeed. A sold-out answer
// here would mean a booking gave up because the free slots were locked.
func TestNoFalseSoldOut(t *testing.T) {
	c := testcluster.Start(t, 1)
	seed(t, c, "acc-exact", 1, 64, 4)
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := book(c, "acc-exact", fmt.Sprintf("k-%d", i), 0, 1); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("booking failed with rooms left: %v", err)
	}
	if l := left(t, c, "acc-exact", 0); l != 0 {
		t.Fatalf("%d rooms left, want 0", l)
	}
}

func TestIdempotentRetries(t *testing.T) {
	c := testcluster.Start(t, 1)
	seed(t, c, "acc-idem", 3, 10, 2)
	var wg sync.WaitGroup
	results := make([]booking.Reservation, 16)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := book(c, "acc-idem", "same-key", 0, 2)
			if err != nil {
				t.Error(err)
			}
			results[i] = r
		}(i)
	}
	wg.Wait()
	fresh := 0
	for _, r := range results {
		if r.ID != results[0].ID {
			t.Fatalf("two reservations for one key: %s and %s", r.ID, results[0].ID)
		}
		if !r.Replayed {
			fresh++
		}
	}
	if fresh != 1 {
		t.Fatalf("%d requests booked rooms, want 1", fresh)
	}
	if l := left(t, c, "acc-idem", 0); l != 9 {
		t.Fatalf("%d rooms left, want 9", l)
	}
	if _, err := book(c, "acc-idem", "same-key", 1, 2); !errors.Is(err, booking.ErrInvalid) {
		t.Fatalf("reusing a key for a different stay: got %v, want ErrInvalid", err)
	}
}

// Multi-night bookings and cancellations over overlapping nights, concurrently. They must not
// deadlock, and at the end rooms left + rooms held by confirmed reservations = rooms loaded.
func TestBookAndCancelConserveRooms(t *testing.T) {
	c := testcluster.Start(t, 1)
	const nights, rooms = 7, 40
	seed(t, c, "acc-mix", nights, rooms, 4)
	ctx := context.Background()
	var mu sync.Mutex
	var booked []string
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w), 7))
			for i := 0; i < 40; i++ {
				if rng.IntN(3) == 0 {
					mu.Lock()
					if len(booked) == 0 {
						mu.Unlock()
						continue
					}
					id := booked[rng.IntN(len(booked))]
					mu.Unlock()
					if _, err := c.Router.Tx(ctx, "acc-mix", func(tx *sql.Tx) error { return booking.Cancel(ctx, tx, "acc-mix", id) }); err != nil {
						t.Errorf("cancel: %v", err)
					}
					continue
				}
				from := rng.IntN(nights - 1)
				r, err := book(c, "acc-mix", fmt.Sprintf("m-%d-%d", w, i), from, 1+rng.IntN(nights-from))
				if err == nil {
					mu.Lock()
					booked = append(booked, r.ID)
					mu.Unlock()
				} else if !errors.Is(err, booking.ErrSoldOut) {
					t.Errorf("book: %v", err)
				}
			}
		}(w)
	}
	wg.Wait()
	db, _ := c.Admin(ctx, "shard-0")
	for n := 0; n < nights; n++ {
		var held int
		if err := db.QueryRow(`SELECT count(*) FROM reservation_nights rn JOIN reservations r USING (account_id, reservation_id)
			WHERE r.account_id = 'acc-mix' AND r.status = 'confirmed' AND rn.night = $1`, night(n).Format(booking.DateLayout)).Scan(&held); err != nil {
			t.Fatal(err)
		}
		if l := left(t, c, "acc-mix", n); l+held != rooms {
			t.Errorf("night %d: %d left + %d held = %d, want %d", n, l, held, l+held, rooms)
		}
	}
	// Cancelling twice is fine; an unknown reservation is not found.
	if len(booked) > 0 {
		for i := 0; i < 2; i++ {
			if _, err := c.Router.Tx(ctx, "acc-mix", func(tx *sql.Tx) error { return booking.Cancel(ctx, tx, "acc-mix", booked[0]) }); err != nil {
				t.Fatal(err)
			}
		}
	}
	_, err := c.Router.Tx(ctx, "acc-mix", func(tx *sql.Tx) error {
		return booking.Cancel(ctx, tx, "acc-mix", "00000000-0000-0000-0000-000000000000")
	})
	if !errors.Is(err, booking.ErrNotFound) {
		t.Fatalf("unknown reservation: got %v", err)
	}
}

func TestAdjustInventory(t *testing.T) {
	c := testcluster.Start(t, 1)
	seed(t, c, "acc-adj", 1, 10, 4)
	ctx := context.Background()
	for i := 0; i < 6; i++ {
		if _, err := book(c, "acc-adj", fmt.Sprintf("a-%d", i), 0, 1); err != nil {
			t.Fatal(err)
		}
	}
	adjust := func(d int) error {
		_, err := c.Router.Tx(ctx, "acc-adj", func(tx *sql.Tx) error {
			return booking.AdjustInventory(ctx, tx, "acc-adj", "MUC", "DBL", night(0), d)
		})
		return err
	}
	if err := adjust(-5); !errors.Is(err, booking.ErrInsufficient) {
		t.Fatalf("removing more than unsold: got %v", err)
	}
	if err := adjust(-3); err != nil {
		t.Fatal(err)
	}
	if l := left(t, c, "acc-adj", 0); l != 1 {
		t.Fatalf("%d left, want 1", l)
	}
	if err := adjust(+5); err != nil {
		t.Fatal(err)
	}
	if l := left(t, c, "acc-adj", 0); l != 6 {
		t.Fatalf("%d left, want 6", l)
	}
	avail, err := booking.Available(ctx, mustAdmin(t, c), "acc-adj", "MUC", "DBL", night(0), night(1))
	if err != nil || avail[night(0).Format(booking.DateLayout)] != 6 {
		t.Fatalf("available = %v, %v", avail, err)
	}
}

func mustAdmin(t *testing.T, c *cluster.Cluster) *sql.DB {
	db, err := c.Admin(context.Background(), "shard-0")
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// Re-loading inventory for a night that is selling counts what is sold, and cancellations
// after re-slicing still give the room back.
func TestSetInventoryOnASellingNight(t *testing.T) {
	c := testcluster.Start(t, 1)
	seed(t, c, "acc-reload", 1, 10, 8)
	ctx := context.Background()
	var ids []string
	for i := 0; i < 4; i++ {
		r, err := book(c, "acc-reload", fmt.Sprintf("r-%d", i), 0, 1)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, r.ID)
	}
	set := func(rooms, slots int) error {
		_, err := c.Router.Tx(ctx, "acc-reload", func(tx *sql.Tx) error {
			return booking.SetInventory(ctx, tx, "acc-reload", "MUC", "DBL", night(0), rooms, slots)
		})
		return err
	}
	if err := set(3, 2); !errors.Is(err, booking.ErrInsufficient) {
		t.Fatalf("setting below sold: %v", err)
	}
	if err := set(12, 2); err != nil {
		t.Fatal(err)
	}
	if l := left(t, c, "acc-reload", 0); l != 8 {
		t.Fatalf("%d left, want 12 - 4 sold = 8", l)
	}
	for _, id := range ids {
		if _, err := c.Router.Tx(ctx, "acc-reload", func(tx *sql.Tx) error { return booking.Cancel(ctx, tx, "acc-reload", id) }); err != nil {
			t.Fatal(err)
		}
	}
	if l := left(t, c, "acc-reload", 0); l != 12 {
		t.Fatalf("%d left after cancelling all, want 12", l)
	}
}
