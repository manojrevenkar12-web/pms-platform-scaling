// Package booking takes rooms out of availability without overselling, under heavy contention.
//
// The hot rows in a hotel system are the availability counters: every booking for the same room
// type and night decrements the same number. With one row per (property, room type, night) every
// booking for a popular night queues on one row lock. Here each counter is split into N slots
// whose remainders add up to the rooms left. A booking takes any slot that is not locked
// (FOR UPDATE SKIP LOCKED) and only waits when every slot with rooms left is busy.
//
// Two properties hold at any contention:
//   - no oversell: a slot never goes below zero (CHECK remaining >= 0, and the decrement only
//     touches a row it has locked with remaining > 0);
//   - no false sold-out: ErrSoldOut is returned only when a fresh snapshot shows zero rooms left
//     for the night, never because the free slots happened to be locked.
//
// Locks are taken in (night, slot) order, in bookings and in cancellations alike, so two
// transactions do not wait on each other in a cycle. In the rare case they still do (PostgreSQL
// detects it), the router retries the whole transaction.
package booking

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

var (
	// ErrSoldOut means at least one night has no room of that type left.
	ErrSoldOut = errors.New("sold out")
	// ErrNotFound means the reservation does not exist for this account.
	ErrNotFound = errors.New("reservation not found")
	// ErrInvalid means the request cannot be a reservation.
	ErrInvalid = errors.New("invalid request")
	// ErrInsufficient means inventory cannot be reduced below what is already booked.
	ErrInsufficient = errors.New("not enough unsold rooms")
)

// MaxNights bounds one reservation.
const MaxNights = 60

// DateLayout is how nights are written in the API.
const DateLayout = "2006-01-02"

// Request is a booking request. RequestKey makes it idempotent: the same key returns the same
// reservation, however often the client retries.
type Request struct {
	AccountID  string    `json:"-"`
	PropertyID string    `json:"property_id"`
	RoomType   string    `json:"room_type"`
	Arrival    time.Time `json:"-"`
	Departure  time.Time `json:"-"`
	GuestRef   string    `json:"guest_ref"`
	RequestKey string    `json:"-"`
}

// Reservation is a confirmed booking.
type Reservation struct {
	ID         string `json:"id"`
	PropertyID string `json:"property_id"`
	RoomType   string `json:"room_type"`
	Arrival    string `json:"arrival"`
	Departure  string `json:"departure"`
	Status     string `json:"status"`
	// Replayed is true when the request key had already been used: nothing new was booked.
	Replayed bool `json:"replayed"`
	// Waited counts nights where every free slot was locked and the booking had to queue.
	Waited int `json:"-"`
}

// Validate checks a request.
func (r Request) Validate() error {
	switch {
	case r.AccountID == "" || r.PropertyID == "" || r.RoomType == "" || r.GuestRef == "":
		return fmt.Errorf("%w: account, property, room type and guest are required", ErrInvalid)
	case r.RequestKey == "" || len(r.RequestKey) > 128:
		return fmt.Errorf("%w: a request key of 1-128 characters is required", ErrInvalid)
	case !r.Departure.After(r.Arrival):
		return fmt.Errorf("%w: departure must be after arrival", ErrInvalid)
	case nights(r.Arrival, r.Departure) > MaxNights:
		return fmt.Errorf("%w: at most %d nights", ErrInvalid, MaxNights)
	}
	return nil
}

func nights(a, d time.Time) int { return int(d.Sub(a).Hours()/24 + 0.5) }

// Book makes the reservation inside tx. If it returns an error the caller must roll back.
func Book(ctx context.Context, tx *sql.Tx, r Request) (Reservation, error) {
	if err := r.Validate(); err != nil {
		return Reservation{}, err
	}
	res := Reservation{PropertyID: r.PropertyID, RoomType: r.RoomType, Arrival: r.Arrival.Format(DateLayout), Departure: r.Departure.Format(DateLayout), Status: "confirmed"}
	err := tx.QueryRowContext(ctx, `
		INSERT INTO reservations (account_id, reservation_id, property_id, room_type, arrival, departure, guest_ref, request_key)
		VALUES ($1, gen_random_uuid(), $2, $3, $4, $5, $6, $7)
		ON CONFLICT (account_id, request_key) DO NOTHING
		RETURNING reservation_id`,
		r.AccountID, r.PropertyID, r.RoomType, res.Arrival, res.Departure, r.GuestRef, r.RequestKey).Scan(&res.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return replay(ctx, tx, r)
	}
	if err != nil {
		return Reservation{}, err
	}
	for night := r.Arrival; night.Before(r.Departure); night = night.AddDate(0, 0, 1) {
		slot, waited, err := takeSlot(ctx, tx, r.AccountID, r.PropertyID, r.RoomType, night)
		if err != nil {
			return Reservation{}, err
		}
		if waited {
			res.Waited++
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO reservation_nights (account_id, reservation_id, night, slot) VALUES ($1,$2,$3,$4)`,
			r.AccountID, res.ID, night.Format(DateLayout), slot); err != nil {
			return Reservation{}, err
		}
	}
	return res, nil
}

func replay(ctx context.Context, tx *sql.Tx, r Request) (Reservation, error) {
	var res Reservation
	var arrival, departure time.Time
	err := tx.QueryRowContext(ctx, `SELECT reservation_id, property_id, room_type, arrival, departure, status FROM reservations WHERE account_id = $1 AND request_key = $2`,
		r.AccountID, r.RequestKey).Scan(&res.ID, &res.PropertyID, &res.RoomType, &arrival, &departure, &res.Status)
	if err != nil {
		return Reservation{}, err
	}
	res.Arrival, res.Departure, res.Replayed = arrival.Format(DateLayout), departure.Format(DateLayout), true
	if res.PropertyID != r.PropertyID || res.RoomType != r.RoomType || res.Arrival != r.Arrival.Format(DateLayout) || res.Departure != r.Departure.Format(DateLayout) {
		return Reservation{}, fmt.Errorf("%w: request key already used for a different booking", ErrInvalid)
	}
	return res, nil
}

const decrement = `
	UPDATE availability SET remaining = remaining - 1
	WHERE (account_id, property_id, room_type, night, slot) = (
		SELECT account_id, property_id, room_type, night, slot FROM availability
		WHERE account_id = $1 AND property_id = $2 AND room_type = $3 AND night = $4 AND remaining > 0
		ORDER BY %s LIMIT 1 FOR UPDATE %s)
	RETURNING slot`

var (
	// Any free slot, starting at a random one so concurrent bookings spread out.
	takeFree = fmt.Sprintf(decrement, "random()", "SKIP LOCKED")
	// Every slot with rooms is locked: queue for them in slot order. A fixed order matters: a
	// waiter that finds its slot emptied keeps that row locked and moves to the next one, so two
	// waiters walking the slots in different orders could deadlock.
	takeWait = fmt.Sprintf(decrement, "slot", "")
)

func takeSlot(ctx context.Context, tx *sql.Tx, account, property, roomType string, night time.Time) (slot int, waited bool, err error) {
	n := night.Format(DateLayout)
	for {
		err = tx.QueryRowContext(ctx, takeFree, account, property, roomType, n).Scan(&slot)
		if err == nil {
			return slot, waited, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return 0, waited, err
		}
		// No unlocked slot with rooms. Either the night is sold out, or all slots with rooms are
		// busy. Waiting tells the two apart: after the lock, PostgreSQL re-checks remaining > 0
		// on the row's new version.
		waited = true
		err = tx.QueryRowContext(ctx, takeWait, account, property, roomType, n).Scan(&slot)
		if err == nil {
			return slot, waited, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return 0, waited, err
		}
		// The slot we waited for ran out. Sold out only if a fresh snapshot says so.
		var left int
		if err := tx.QueryRowContext(ctx, `SELECT coalesce(sum(remaining), 0) FROM availability WHERE account_id = $1 AND property_id = $2 AND room_type = $3 AND night = $4`,
			account, property, roomType, n).Scan(&left); err != nil {
			return 0, waited, err
		}
		if left == 0 {
			return 0, waited, fmt.Errorf("%w: %s %s on %s", ErrSoldOut, property, roomType, n)
		}
		if err := ctx.Err(); err != nil {
			return 0, waited, err
		}
	}
}

// Cancel cancels a reservation and gives its rooms back. Cancelling twice is not an error.
func Cancel(ctx context.Context, tx *sql.Tx, account, reservationID string) error {
	var property, roomType, status string
	err := tx.QueryRowContext(ctx, `SELECT property_id, room_type, status FROM reservations WHERE account_id = $1 AND reservation_id = $2 FOR UPDATE`,
		account, reservationID).Scan(&property, &roomType, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if status == "cancelled" {
		return nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT night, slot FROM reservation_nights WHERE account_id = $1 AND reservation_id = $2 ORDER BY night`, account, reservationID)
	if err != nil {
		return err
	}
	type taken struct {
		night time.Time
		slot  int
	}
	var nights []taken
	for rows.Next() {
		var n taken
		if err := rows.Scan(&n.night, &n.slot); err != nil {
			rows.Close()
			return err
		}
		nights = append(nights, n)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	// Give each room back in night order (the same order bookings lock in). Slots are
	// interchangeable: if inventory was re-sliced since, the room goes to the lowest slot.
	for _, n := range nights {
		if _, err := tx.ExecContext(ctx, `
			UPDATE availability SET remaining = remaining + 1
			WHERE (account_id, property_id, room_type, night, slot) = (
				SELECT account_id, property_id, room_type, night, slot FROM availability
				WHERE account_id = $1 AND property_id = $2 AND room_type = $3 AND night = $4
				ORDER BY (slot = $5) DESC, slot LIMIT 1)`,
			account, property, roomType, n.night.Format(DateLayout), n.slot); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `UPDATE reservations SET status = 'cancelled' WHERE account_id = $1 AND reservation_id = $2`, account, reservationID)
	return err
}

// Execer is a *sql.DB or a *sql.Tx.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// SetInventory sets the number of sellable rooms for a night, counting rooms already sold, and
// spreads what is left over the given number of slots. It waits for bookings in flight on that
// night, and refuses to go below what is already sold.
func SetInventory(ctx context.Context, q Execer, account, property, roomType string, night time.Time, rooms, slots int) error {
	if rooms < 0 || slots < 1 || slots > 256 {
		return fmt.Errorf("%w: rooms >= 0 and 1 <= slots <= 256", ErrInvalid)
	}
	n := night.Format(DateLayout)
	if _, err := q.ExecContext(ctx, `SELECT 1 FROM availability WHERE account_id = $1 AND property_id = $2 AND room_type = $3 AND night = $4 ORDER BY slot FOR UPDATE`, account, property, roomType, n); err != nil {
		return err
	}
	var sold int
	if err := q.QueryRowContext(ctx, `
		SELECT count(*) FROM reservation_nights rn JOIN reservations r USING (account_id, reservation_id)
		WHERE r.account_id = $1 AND r.property_id = $2 AND r.room_type = $3 AND rn.night = $4 AND r.status = 'confirmed'`,
		account, property, roomType, n).Scan(&sold); err != nil {
		return err
	}
	if rooms < sold {
		return fmt.Errorf("%w: %d rooms already sold on %s, cannot set %d", ErrInsufficient, sold, n, rooms)
	}
	if _, err := q.ExecContext(ctx, `DELETE FROM availability WHERE account_id = $1 AND property_id = $2 AND room_type = $3 AND night = $4`, account, property, roomType, n); err != nil {
		return err
	}
	_, err := q.ExecContext(ctx, `
		INSERT INTO availability (account_id, property_id, room_type, night, slot, remaining)
		SELECT $1, $2, $3, $4::date, s, $5::int / $6::int + CASE WHEN s < $5::int % $6::int THEN 1 ELSE 0 END
		FROM generate_series(0, $6::int - 1) AS s`, account, property, roomType, n, rooms-sold, slots)
	return err
}

// AdjustInventory adds rooms (delta > 0) or takes unsold rooms away (delta < 0) for a night that
// is already selling, for example when a room goes out of order.
func AdjustInventory(ctx context.Context, tx *sql.Tx, account, property, roomType string, night time.Time, delta int) error {
	n := night.Format(DateLayout)
	rows, err := tx.QueryContext(ctx, `SELECT slot, remaining FROM availability WHERE account_id = $1 AND property_id = $2 AND room_type = $3 AND night = $4 ORDER BY slot FOR UPDATE`, account, property, roomType, n)
	if err != nil {
		return err
	}
	type s struct{ slot, remaining int }
	var all []s
	total := 0
	for rows.Next() {
		var x s
		if err := rows.Scan(&x.slot, &x.remaining); err != nil {
			rows.Close()
			return err
		}
		all = append(all, x)
		total += x.remaining
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(all) == 0 {
		return fmt.Errorf("%w: no inventory loaded for %s", ErrInvalid, n)
	}
	if total+delta < 0 {
		return fmt.Errorf("%w: %d unsold, asked to remove %d", ErrInsufficient, total, -delta)
	}
	// Spread the new total evenly again; bookings already made are not touched.
	target := total + delta
	for i, x := range all {
		want := target / len(all)
		if i < target%len(all) {
			want++
		}
		if want == x.remaining {
			continue
		}
		if _, err := tx.ExecContext(ctx, `UPDATE availability SET remaining = $6 WHERE account_id = $1 AND property_id = $2 AND room_type = $3 AND night = $4 AND slot = $5`,
			account, property, roomType, n, x.slot, want); err != nil {
			return err
		}
	}
	return nil
}

// Available returns rooms left per night in [from, to).
func Available(ctx context.Context, q Execer, account, property, roomType string, from, to time.Time) (map[string]int, error) {
	rows, err := q.QueryContext(ctx, `SELECT night, sum(remaining) FROM availability WHERE account_id = $1 AND property_id = $2 AND room_type = $3 AND night >= $4 AND night < $5 GROUP BY night ORDER BY night`,
		account, property, roomType, from.Format(DateLayout), to.Format(DateLayout))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var night time.Time
		var left int
		if err := rows.Scan(&night, &left); err != nil {
			return nil, err
		}
		out[night.Format(DateLayout)] = left
	}
	return out, rows.Err()
}

// ParseDate parses a night in the API's format.
func ParseDate(s string) (time.Time, error) {
	t, err := time.Parse(DateLayout, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: date %q is not YYYY-MM-DD", ErrInvalid, s)
	}
	return t, nil
}
