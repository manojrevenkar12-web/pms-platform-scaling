package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/cluster"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/pgtest"
)

// The commands an operator runs, end to end on real servers.
func TestCommands(t *testing.T) {
	control, shards := pgtest.StartCluster(t, 2)
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctl := control.DSN("postgres")
	cfg := []string{"-config", "../../config/platform.yaml", "-control", ctl}
	steps := [][]string{
		{"init", "-shard-dsn", "shard-0=" + shards[0].DSN("postgres"), "-shard-dsn", "shard-1=" + shards[1].DSN("postgres")},
		{"place", "-account", "acc-cli", "-shard", "shard-0"},
		{"move", "-account", "acc-cli", "-to", "shard-1", "-id", "cli-move"},
		{"status", "-account", "acc-cli"},
	}
	for _, s := range steps {
		if err := run(s[0], append(append([]string{}, cfg...), s[1:]...), log); err != nil {
			t.Fatalf("reshard %v: %v", s, err)
		}
	}
	c, err := cluster.Open(context.Background(), ctl, cluster.Options{DirectoryTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if owner, _ := c.Owner(context.Background(), "acc-cli"); owner != "shard-1" {
		t.Fatalf("owner after move = %s", owner)
	}
	if err := run("abort", append(append([]string{}, cfg...), "-id", "cli-move"), log); err == nil {
		t.Fatal("aborting a finished move should fail")
	}
	if err := run("move", append(append([]string{}, cfg...), "-account", "acc-cli"), log); err == nil {
		t.Fatal("move without -to should fail")
	}
}
