package metrics

import (
	"strings"
	"testing"
)

func TestTextFormat(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("x_total", "Things.", "kind")
	c.Inc("a")
	c.Add(2, `b"q`)
	h := r.Histogram("y_seconds", "Latency.", []float64{0.1, 1}, "route")
	h.Observe(0.05, "book")
	h.Observe(0.1, "book")
	h.Observe(5, "book")
	var b strings.Builder
	r.Write(&b)
	want := `# HELP x_total Things.
# TYPE x_total counter
x_total{kind="a"} 1
x_total{kind="b\"q"} 2
# HELP y_seconds Latency.
# TYPE y_seconds histogram
y_seconds_bucket{route="book",le="0.1"} 2
y_seconds_bucket{route="book",le="1"} 2
y_seconds_bucket{route="book",le="+Inf"} 3
y_seconds_sum{route="book"} 5.15
y_seconds_count{route="book"} 3
`
	if b.String() != want {
		t.Fatalf("got\n%s\nwant\n%s", b.String(), want)
	}
	if c.Value("a") != 1 {
		t.Fatal("value")
	}
}

func TestWrongLabelCountPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic")
		}
	}()
	NewRegistry().Counter("z", "z", "a", "b").Inc("only-one")
}
