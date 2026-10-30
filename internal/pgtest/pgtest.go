// Package pgtest starts throwaway PostgreSQL servers for integration tests: real servers, with
// logical replication on, so the tests exercise exactly what production runs.
//
// It uses the server binaries on the machine (PG_BIN, default /usr/lib/postgresql/16/bin). When
// the tests run as root (as in some containers), the server runs as the postgres user, because
// PostgreSQL refuses to run as root.
package pgtest

import (
	"database/sql"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq" // driver
)

// Server is one running PostgreSQL instance.
type Server struct {
	Name string
	Port int
	Dir  string
	bin  string
	asPG bool
}

// DSN for a database on this server.
func (s *Server) DSN(db string) string {
	return fmt.Sprintf("host=127.0.0.1 port=%d user=postgres dbname=%s sslmode=disable", s.Port, db)
}

// ConnString as PostgreSQL itself needs it (for CREATE SUBSCRIPTION ... CONNECTION).
func (s *Server) ConnString(db string) string { return s.DSN(db) }

// Open a connection pool to a database on this server.
func (s *Server) Open(db string) (*sql.DB, error) {
	return sql.Open("postgres", s.DSN(db))
}

func (s *Server) run(args ...string) *exec.Cmd {
	if s.asPG {
		return exec.Command("runuser", append([]string{"-u", "postgres", "--"}, args...)...)
	}
	return exec.Command(args[0], args[1:]...)
}

// Stop shuts the server down fast.
func (s *Server) Stop() error {
	out, err := s.run(filepath.Join(s.bin, "pg_ctl"), "-D", filepath.Join(s.Dir, "data"), "-m", "fast", "-w", "stop").CombinedOutput()
	if err != nil {
		return fmt.Errorf("pg_ctl stop: %v: %s", err, out)
	}
	return nil
}

// Available reports whether PostgreSQL server binaries exist here.
func Available() bool {
	_, err := os.Stat(filepath.Join(binDir(), "initdb"))
	return err == nil
}

func binDir() string {
	if b := os.Getenv("PG_BIN"); b != "" {
		return b
	}
	return "/usr/lib/postgresql/16/bin"
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

var mu sync.Mutex

// Durable makes new servers flush commits to disk (fsync, synchronous_commit, full page writes),
// as production does. Tests leave it off for speed; the benchmark turns it on.
var Durable bool

// Start initialises and starts a server under base, with logical replication enabled.
func Start(base, name string) (*Server, error) {
	mu.Lock()
	defer mu.Unlock()
	port, err := freePort()
	if err != nil {
		return nil, err
	}
	s := &Server{Name: name, Port: port, Dir: filepath.Join(base, name), bin: binDir()}
	if u, _ := user.Current(); u != nil && u.Uid == "0" {
		s.asPG = true
	}
	if err := os.MkdirAll(s.Dir, 0o755); err != nil {
		return nil, err
	}
	if s.asPG {
		pg, err := user.Lookup("postgres")
		if err != nil {
			return nil, fmt.Errorf("running as root needs a postgres user: %w", err)
		}
		uid, _ := strconv.Atoi(pg.Uid)
		gid, _ := strconv.Atoi(pg.Gid)
		if err := os.Chown(s.Dir, uid, gid); err != nil {
			return nil, err
		}
	}
	data := filepath.Join(s.Dir, "data")
	if out, err := s.run(filepath.Join(s.bin, "initdb"), "-D", data, "-A", "trust", "-U", "postgres", "--no-sync").CombinedOutput(); err != nil {
		return nil, fmt.Errorf("initdb: %v: %s", err, out)
	}
	durability := "-c fsync=off -c synchronous_commit=off -c full_page_writes=off"
	if Durable {
		durability = "-c fsync=on -c synchronous_commit=on -c full_page_writes=on"
	}
	opts := fmt.Sprintf("-p %d -k %s -c listen_addresses=127.0.0.1 -c wal_level=logical -c max_replication_slots=32 -c max_wal_senders=32 -c max_logical_replication_workers=16 -c max_worker_processes=32 -c max_connections=300 -c shared_buffers=256MB %s", port, s.Dir, durability)
	if out, err := s.run(filepath.Join(s.bin, "pg_ctl"), "-D", data, "-o", opts, "-l", filepath.Join(s.Dir, "server.log"), "-w", "start").CombinedOutput(); err != nil {
		return nil, fmt.Errorf("pg_ctl start: %v: %s", err, out)
	}
	return s, nil
}

// StartServers starts a control server and n shard servers (shard-0 … shard-n-1) under a new
// directory inside parent ("" means the system temp dir). stop shuts them down and removes it.
func StartServers(parent string, n int) (control *Server, shards []*Server, stop func(), err error) {
	base, err := os.MkdirTemp(parent, "pgtest-")
	if err != nil {
		return nil, nil, nil, err
	}
	_ = os.Chmod(base, 0o755)
	var all []*Server
	stop = func() {
		for _, s := range all {
			_ = s.Stop()
		}
		_ = os.RemoveAll(base)
	}
	fail := func(e error) (*Server, []*Server, func(), error) {
		stop()
		return nil, nil, nil, e
	}
	control, err = Start(base, "control")
	if err != nil {
		return fail(err)
	}
	all = append(all, control)
	for i := 0; i < n; i++ {
		s, err := Start(base, fmt.Sprintf("shard-%d", i))
		if err != nil {
			return fail(err)
		}
		all = append(all, s)
		shards = append(shards, s)
	}
	for _, s := range all {
		if err := waitReady(s); err != nil {
			return fail(err)
		}
	}
	return control, shards, stop, nil
}

// StartCluster is StartServers for a test: servers stop when the test ends. Tests are skipped
// where PostgreSQL is not installed, unless PGTEST_REQUIRED=1.
func StartCluster(t testing.TB, n int) (control *Server, shards []*Server) {
	t.Helper()
	if !Available() {
		if os.Getenv("PGTEST_REQUIRED") == "1" {
			t.Fatalf("PostgreSQL binaries not found in %s", binDir())
		}
		t.Skip("PostgreSQL not installed")
	}
	control, shards, stop, err := StartServers(os.Getenv("PGTEST_DIR"), n)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)
	return control, shards
}

func waitReady(s *Server) error {
	db, err := s.Open("postgres")
	if err != nil {
		return err
	}
	defer db.Close()
	for i := 0; i < 100; i++ {
		if err = db.Ping(); err == nil {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("%s not ready: %w", s.Name, err)
}
