// Package api is the HTTP interface of the booking write path.
//
//	POST   /v1/accounts/{account}/reservations        book (Idempotency-Key header required)
//	DELETE /v1/accounts/{account}/reservations/{id}   cancel
//	GET    /v1/accounts/{account}/availability        rooms left per night
//	PUT    /v1/accounts/{account}/inventory           load rooms for a range of nights
//	GET    /metrics, /healthz, /readyz
//
// While a tenant is being moved its writes wait (bounded by router.max_wait); if the wait runs
// out the client gets 503 with Retry-After and can retry with the same Idempotency-Key.
package api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/booking"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/cluster"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/directory"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/metrics"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/router"
)

// Server serves the API.
type Server struct {
	C            *cluster.Cluster
	Log          *slog.Logger
	Keys         map[string]string // sha256 hex -> account
	DefaultSlots int
	MaxBody      int64

	reg      *metrics.Registry
	requests *metrics.Counter
	latency  *metrics.Histogram
	retries  *metrics.Counter
	outcomes *metrics.Counter
}

// New builds a server and its metrics.
func New(c *cluster.Cluster, log *slog.Logger, keys map[string]string, defaultSlots int, maxBody int64) *Server {
	reg := metrics.NewRegistry()
	s := &Server{
		C: c, Log: log, Keys: keys, DefaultSlots: defaultSlots, MaxBody: maxBody, reg: reg,
		requests: reg.Counter("pms_http_requests_total", "HTTP requests by route and status.", "route", "status"),
		latency:  reg.Histogram("pms_http_request_seconds", "HTTP request latency.", metrics.LatencyBuckets, "route"),
		retries:  reg.Counter("pms_router_retries_total", "Transactions retried by the router, by reason (frozen, stale, deadlock, serialization).", "reason"),
		outcomes: reg.Counter("pms_bookings_total", "Booking outcomes (booked, replayed, sold_out, waited_night).", "outcome"),
	}
	c.Router.OnRetry = func(reason string) { s.retries.Inc(reason) }
	return s
}

// Metrics exposes the registry (for tests).
func (s *Server) Metrics() *metrics.Registry { return s.reg }

// Handler returns the routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/accounts/{account}/reservations", s.wrap("book", s.book))
	mux.HandleFunc("DELETE /v1/accounts/{account}/reservations/{id}", s.wrap("cancel", s.cancel))
	mux.HandleFunc("GET /v1/accounts/{account}/availability", s.wrap("availability", s.availability))
	mux.HandleFunc("PUT /v1/accounts/{account}/inventory", s.wrap("inventory", s.inventory))
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		s.reg.Write(w)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := s.C.Control.PingContext(ctx); err != nil {
			http.Error(w, "control database unreachable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) { w.status = code; w.ResponseWriter.WriteHeader(code) }

// wrap adds auth, body limits, metrics and one structured log line per request.
func (s *Server) wrap(route string, h func(http.ResponseWriter, *http.Request, string) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		t0 := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		account := r.PathValue("account")
		r.Body = http.MaxBytesReader(sw, r.Body, s.MaxBody)
		var err error
		if !s.authorised(r, account) {
			err = errForbidden
		} else {
			err = h(sw, r, account)
		}
		if err != nil {
			s.writeError(sw, err)
		}
		s.requests.Inc(route, http.StatusText(sw.status))
		s.latency.Since(t0, route)
		level := slog.LevelInfo
		if sw.status >= 500 {
			level = slog.LevelError
		}
		attrs := []any{"route", route, "account", account, "status", sw.status, "ms", float64(time.Since(t0).Microseconds()) / 1000}
		if err != nil {
			attrs = append(attrs, "error", err.Error())
		}
		s.Log.Log(r.Context(), level, "request", attrs...)
	}
}

var errForbidden = errors.New("this key cannot access this account")

func (s *Server) authorised(r *http.Request, account string) bool {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || token == "" {
		return false
	}
	sum := sha256.Sum256([]byte(token))
	want, ok := s.Keys[hex.EncodeToString(sum[:])]
	return ok && subtle.ConstantTimeCompare([]byte(want), []byte(account)) == 1
}

type problem struct {
	Error string `json:"error"`
}

func (s *Server) writeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	msg := "internal error"
	var maxBytes *http.MaxBytesError
	switch {
	case errors.Is(err, errForbidden):
		status, msg = http.StatusForbidden, err.Error()
	case errors.Is(err, booking.ErrInvalid), errors.As(err, &maxBytes), errors.Is(err, errBadJSON):
		status, msg = http.StatusBadRequest, err.Error()
	case errors.Is(err, booking.ErrSoldOut), errors.Is(err, booking.ErrInsufficient):
		status, msg = http.StatusConflict, err.Error()
	case errors.Is(err, booking.ErrNotFound), errors.Is(err, directory.ErrUnknownTenant):
		status, msg = http.StatusNotFound, err.Error()
	case errors.Is(err, router.ErrMoving):
		w.Header().Set("Retry-After", "1")
		status, msg = http.StatusServiceUnavailable, "account is being moved; retry with the same Idempotency-Key"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		status, msg = http.StatusServiceUnavailable, "request timed out"
	}
	writeJSON(w, status, problem{Error: msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

var errBadJSON = errors.New("request body is not valid JSON for this endpoint")

func decode(r *http.Request, v any) error {
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		var maxBytes *http.MaxBytesError
		if errors.As(err, &maxBytes) {
			return err
		}
		return errors.Join(errBadJSON, err)
	}
	return nil
}

type bookBody struct {
	PropertyID string `json:"property_id"`
	RoomType   string `json:"room_type"`
	Arrival    string `json:"arrival"`
	Departure  string `json:"departure"`
	GuestRef   string `json:"guest_ref"`
}

func (s *Server) book(w http.ResponseWriter, r *http.Request, account string) error {
	var b bookBody
	if err := decode(r, &b); err != nil {
		return err
	}
	arrival, err := booking.ParseDate(b.Arrival)
	if err != nil {
		return err
	}
	departure, err := booking.ParseDate(b.Departure)
	if err != nil {
		return err
	}
	req := booking.Request{AccountID: account, PropertyID: b.PropertyID, RoomType: b.RoomType, Arrival: arrival, Departure: departure, GuestRef: b.GuestRef, RequestKey: r.Header.Get("Idempotency-Key")}
	if err := req.Validate(); err != nil {
		return err
	}
	var res booking.Reservation
	_, err = s.C.Router.Tx(r.Context(), account, func(tx *sql.Tx) error {
		var err error
		res, err = booking.Book(r.Context(), tx, req)
		return err
	})
	switch {
	case errors.Is(err, booking.ErrSoldOut):
		s.outcomes.Inc("sold_out")
		return err
	case err != nil:
		return err
	}
	if res.Waited > 0 {
		s.outcomes.Add(float64(res.Waited), "waited_night")
	}
	if res.Replayed {
		s.outcomes.Inc("replayed")
		writeJSON(w, http.StatusOK, res)
		return nil
	}
	s.outcomes.Inc("booked")
	writeJSON(w, http.StatusCreated, res)
	return nil
}

func (s *Server) cancel(w http.ResponseWriter, r *http.Request, account string) error {
	id := r.PathValue("id")
	if len(id) != 36 {
		return errors.Join(booking.ErrInvalid, errors.New("reservation id must be a UUID"))
	}
	_, err := s.C.Router.Tx(r.Context(), account, func(tx *sql.Tx) error { return booking.Cancel(r.Context(), tx, account, id) })
	if err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func dateRange(r *http.Request) (from, to time.Time, err error) {
	from, err = booking.ParseDate(r.URL.Query().Get("from"))
	if err != nil {
		return
	}
	to, err = booking.ParseDate(r.URL.Query().Get("to"))
	if err != nil {
		return
	}
	if !to.After(from) || to.Sub(from) > 366*24*time.Hour {
		err = errors.Join(booking.ErrInvalid, errors.New("to must be after from, at most a year later"))
	}
	return
}

func (s *Server) availability(w http.ResponseWriter, r *http.Request, account string) error {
	from, to, err := dateRange(r)
	if err != nil {
		return err
	}
	q := r.URL.Query()
	var out map[string]int
	// Reads go through the fence too, so a reader never sees the old shard after a move.
	_, err = s.C.Router.Tx(r.Context(), account, func(tx *sql.Tx) error {
		var err error
		out, err = booking.Available(r.Context(), tx, account, q.Get("property_id"), q.Get("room_type"), from, to)
		return err
	})
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, map[string]any{"property_id": q.Get("property_id"), "room_type": q.Get("room_type"), "nights": out})
	return nil
}

type inventoryBody struct {
	PropertyID string `json:"property_id"`
	RoomType   string `json:"room_type"`
	From       string `json:"from"`
	To         string `json:"to"`
	Rooms      int    `json:"rooms"`
	Slots      int    `json:"slots"`
}

func (s *Server) inventory(w http.ResponseWriter, r *http.Request, account string) error {
	var b inventoryBody
	if err := decode(r, &b); err != nil {
		return err
	}
	from, err := booking.ParseDate(b.From)
	if err != nil {
		return err
	}
	to, err := booking.ParseDate(b.To)
	if err != nil {
		return err
	}
	if b.PropertyID == "" || b.RoomType == "" || !to.After(from) || to.Sub(from) > 366*24*time.Hour {
		return errors.Join(booking.ErrInvalid, errors.New("property_id, room_type and a range of at most a year are required"))
	}
	if b.Slots == 0 {
		b.Slots = s.DefaultSlots
	}
	_, err = s.C.Router.Tx(r.Context(), account, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(r.Context(), `INSERT INTO properties (account_id, property_id, name) VALUES ($1, $2, $2) ON CONFLICT DO NOTHING`, account, b.PropertyID); err != nil {
			return err
		}
		for n := from; n.Before(to); n = n.AddDate(0, 0, 1) {
			if err := booking.SetInventory(r.Context(), tx, account, b.PropertyID, b.RoomType, n, b.Rooms, b.Slots); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}
