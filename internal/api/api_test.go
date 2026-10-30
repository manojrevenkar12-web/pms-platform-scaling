package api_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/api"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/testcluster"
)

func hash(k string) string { s := sha256.Sum256([]byte(k)); return hex.EncodeToString(s[:]) }

func TestHTTP(t *testing.T) {
	c := testcluster.Start(t, 1)
	if err := c.Place(t.Context(), "acc-http", "shard-0"); err != nil {
		t.Fatal(err)
	}
	s := api.New(c, slog.New(slog.NewJSONHandler(io.Discard, nil)), map[string]string{hash("key-http"): "acc-http", hash("key-other"): "acc-other"}, 4, 1<<16)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	do := func(method, path, key, idem, body string) (int, map[string]any) {
		t.Helper()
		req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		if key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		if idem != "" {
			req.Header.Set("Idempotency-Key", idem)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(res.Body).Decode(&out)
		return res.StatusCode, out
	}
	base := "/v1/accounts/acc-http"
	steps := []struct {
		name, method, path, key, idem, body string
		want                                int
	}{
		{"no key", "GET", base + "/availability?from=2027-01-01&to=2027-01-03", "", "", "", 403},
		{"other account's key", "GET", base + "/availability?from=2027-01-01&to=2027-01-03", "key-other", "", "", 403},
		{"load rooms", "PUT", base + "/inventory", "key-http", "", `{"property_id":"MUC","room_type":"DBL","from":"2027-01-01","to":"2027-01-03","rooms":1}`, 204},
		{"unknown field", "PUT", base + "/inventory", "key-http", "", `{"property":"MUC"}`, 400},
		{"no idempotency key", "POST", base + "/reservations", "key-http", "", `{"property_id":"MUC","room_type":"DBL","arrival":"2027-01-01","departure":"2027-01-03","guest_ref":"g1"}`, 400},
		{"bad date", "POST", base + "/reservations", "key-http", "k1", `{"property_id":"MUC","room_type":"DBL","arrival":"01/01/2027","departure":"2027-01-03","guest_ref":"g1"}`, 400},
		{"book", "POST", base + "/reservations", "key-http", "k1", `{"property_id":"MUC","room_type":"DBL","arrival":"2027-01-01","departure":"2027-01-03","guest_ref":"g1"}`, 201},
		{"retry", "POST", base + "/reservations", "key-http", "k1", `{"property_id":"MUC","room_type":"DBL","arrival":"2027-01-01","departure":"2027-01-03","guest_ref":"g1"}`, 200},
		{"sold out", "POST", base + "/reservations", "key-http", "k2", `{"property_id":"MUC","room_type":"DBL","arrival":"2027-01-02","departure":"2027-01-03","guest_ref":"g2"}`, 409},
		{"lower below sold", "PUT", base + "/inventory", "key-http", "", `{"property_id":"MUC","room_type":"DBL","from":"2027-01-01","to":"2027-01-02","rooms":0}`, 409},
	}
	var id string
	for _, st := range steps {
		code, out := do(st.method, st.path, st.key, st.idem, st.body)
		if code != st.want {
			t.Fatalf("%s: status %d (%v), want %d", st.name, code, out, st.want)
		}
		if st.name == "book" {
			id, _ = out["id"].(string)
		}
		if st.name == "retry" && out["id"] != id {
			t.Fatalf("retry returned %v, want %s", out["id"], id)
		}
	}
	if code, _ := do("DELETE", base+"/reservations/"+id, "key-http", "", ""); code != 204 {
		t.Fatalf("cancel: %d", code)
	}
	code, out := do("GET", base+"/availability?property_id=MUC&room_type=DBL&from=2027-01-01&to=2027-01-03", "key-http", "", "")
	nights, _ := out["nights"].(map[string]any)
	if code != 200 || nights["2027-01-01"] != 1.0 || nights["2027-01-02"] != 1.0 {
		t.Fatalf("availability after cancel: %d %v", code, out)
	}
	if code, _ := do("DELETE", "/v1/accounts/acc-other/reservations/"+id, "key-other", "", ""); code != 404 {
		t.Fatalf("unknown tenant: %d", code)
	}
	res, _ := http.Get(srv.URL + "/metrics")
	body, _ := io.ReadAll(res.Body)
	for _, want := range []string{`pms_bookings_total{outcome="booked"} 1`, `pms_bookings_total{outcome="replayed"} 1`, `pms_bookings_total{outcome="sold_out"} 1`, `pms_http_request_seconds_count{route="book"}`} {
		if !strings.Contains(string(body), want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}
