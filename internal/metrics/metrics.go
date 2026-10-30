// Package metrics is a small Prometheus text-format registry: counters and histograms with
// labels, safe for concurrent use. It has no dependencies, so the service builds from a
// standard Go toolchain alone.
package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Registry holds metrics in registration order.
type Registry struct {
	mu      sync.Mutex
	metrics []metric
}

type metric interface {
	write(w io.Writer)
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{} }

// Write writes every metric in Prometheus text format.
func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	ms := append([]metric(nil), r.metrics...)
	r.mu.Unlock()
	for _, m := range ms {
		m.write(w)
	}
}

func labelString(names, values []string) string {
	if len(names) == 0 {
		return ""
	}
	parts := make([]string, len(names))
	for i, n := range names {
		v := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(values[i])
		parts[i] = fmt.Sprintf(`%s="%s"`, n, v)
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// Counter is a monotonically increasing value per label set.
type Counter struct {
	name, help string
	labels     []string
	mu         sync.Mutex
	values     map[string]float64
	keys       map[string][]string
}

// Counter registers a counter.
func (r *Registry) Counter(name, help string, labels ...string) *Counter {
	c := &Counter{name: name, help: help, labels: labels, values: map[string]float64{}, keys: map[string][]string{}}
	r.mu.Lock()
	r.metrics = append(r.metrics, c)
	r.mu.Unlock()
	return c
}

// Add adds v (>= 0) for the label values.
func (c *Counter) Add(v float64, labelValues ...string) {
	if len(labelValues) != len(c.labels) {
		panic(fmt.Sprintf("%s: %d label values for %d labels", c.name, len(labelValues), len(c.labels)))
	}
	k := strings.Join(labelValues, "\x00")
	c.mu.Lock()
	c.values[k] += v
	if _, ok := c.keys[k]; !ok {
		c.keys[k] = append([]string(nil), labelValues...)
	}
	c.mu.Unlock()
}

// Inc adds one.
func (c *Counter) Inc(labelValues ...string) { c.Add(1, labelValues...) }

// Value returns the current value (for tests).
func (c *Counter) Value(labelValues ...string) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.values[strings.Join(labelValues, "\x00")]
}

func (c *Counter) write(w io.Writer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n", c.name, c.help, c.name)
	for _, k := range sortedKeys(c.values) {
		fmt.Fprintf(w, "%s%s %s\n", c.name, labelString(c.labels, c.keys[k]), fmtFloat(c.values[k]))
	}
}

// Histogram counts observations into cumulative buckets per label set.
type Histogram struct {
	name, help string
	labels     []string
	buckets    []float64
	mu         sync.Mutex
	series     map[string]*hseries
}

type hseries struct {
	labels []string
	counts []uint64 // per bucket, not cumulative
	count  uint64
	sum    float64
}

// LatencyBuckets are seconds from 1ms to 10s.
var LatencyBuckets = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// Histogram registers a histogram with the given upper bounds (sorted ascending).
func (r *Registry) Histogram(name, help string, buckets []float64, labels ...string) *Histogram {
	h := &Histogram{name: name, help: help, labels: labels, buckets: buckets, series: map[string]*hseries{}}
	r.mu.Lock()
	r.metrics = append(r.metrics, h)
	r.mu.Unlock()
	return h
}

// Observe records one value.
func (h *Histogram) Observe(v float64, labelValues ...string) {
	if len(labelValues) != len(h.labels) {
		panic(fmt.Sprintf("%s: %d label values for %d labels", h.name, len(labelValues), len(h.labels)))
	}
	k := strings.Join(labelValues, "\x00")
	h.mu.Lock()
	defer h.mu.Unlock()
	s, ok := h.series[k]
	if !ok {
		s = &hseries{labels: append([]string(nil), labelValues...), counts: make([]uint64, len(h.buckets))}
		h.series[k] = s
	}
	i := sort.SearchFloat64s(h.buckets, v)
	if i < len(h.buckets) {
		s.counts[i]++
	}
	s.count++
	s.sum += v
}

// Since observes the seconds elapsed since t0.
func (h *Histogram) Since(t0 time.Time, labelValues ...string) {
	h.Observe(time.Since(t0).Seconds(), labelValues...)
}

func (h *Histogram) write(w io.Writer) {
	h.mu.Lock()
	defer h.mu.Unlock()
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", h.name, h.help, h.name)
	keys := make([]string, 0, len(h.series))
	for k := range h.series {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		s := h.series[k]
		var cum uint64
		names := append(append([]string(nil), h.labels...), "le")
		for i, b := range h.buckets {
			cum += s.counts[i]
			fmt.Fprintf(w, "%s_bucket%s %d\n", h.name, labelString(names, append(append([]string(nil), s.labels...), fmtFloat(b))), cum)
		}
		fmt.Fprintf(w, "%s_bucket%s %d\n", h.name, labelString(names, append(append([]string(nil), s.labels...), "+Inf")), s.count)
		fmt.Fprintf(w, "%s_sum%s %s\n", h.name, labelString(h.labels, s.labels), fmtFloat(s.sum))
		fmt.Fprintf(w, "%s_count%s %d\n", h.name, labelString(h.labels, s.labels), s.count)
	}
}

func sortedKeys(m map[string]float64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func fmtFloat(v float64) string {
	if math.IsInf(v, 1) {
		return "+Inf"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}
