// Package testcluster starts a real platform (control database plus shards) for a test.
package testcluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/cluster"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/directory"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/pgtest"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/router"
)

// Start starts n shards and returns an open cluster; everything stops when the test ends.
func Start(t testing.TB, n int) *cluster.Cluster {
	t.Helper()
	control, servers := pgtest.StartCluster(t, n)
	c, err := Setup(context.Background(), control, servers, cluster.Options{DirectoryTTL: time.Second, Router: router.Options{MaxWait: 30 * time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

// Setup creates the schema on running servers and opens the cluster.
func Setup(ctx context.Context, control *pgtest.Server, servers []*pgtest.Server, o cluster.Options) (*cluster.Cluster, error) {
	var shards []directory.Shard
	for i, s := range servers {
		shards = append(shards, directory.Shard{ID: fmt.Sprintf("shard-%d", i), DSN: s.DSN("postgres"), ReplConn: s.ConnString("postgres")})
	}
	if err := cluster.Init(ctx, control.DSN("postgres"), shards); err != nil {
		return nil, err
	}
	return cluster.Open(ctx, control.DSN("postgres"), o)
}
