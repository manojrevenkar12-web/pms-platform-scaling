// Command reshard manages shards and moves tenants between them.
//
//	reshard init   -control DSN -shard-dsn shard-0=DSN [-shard-dsn shard-1=DSN ...]
//	reshard place  -control DSN -account ACC -shard shard-0
//	reshard move   -control DSN -account ACC -to shard-1 [-id MOVE_ID]
//	reshard abort  -control DSN -id MOVE_ID
//	reshard status -control DSN [-account ACC]
//
// A shard's DSN is also what other shards use to subscribe to it, unless given as
// shard-0=DSN|REPLICATION_DSN. Re-running "move" with the same -id resumes a crashed move.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/cluster"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/config"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/directory"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/mover"
)

type multi []string

func (m *multi) String() string     { return strings.Join(*m, ",") }
func (m *multi) Set(v string) error { *m = append(*m, v); return nil }

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: reshard init|place|move|abort|status [flags]")
		os.Exit(2)
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(os.Args[1], os.Args[2:], log); err != nil {
		log.Error("reshard failed", "command", os.Args[1], "error", err.Error())
		os.Exit(1)
	}
}

func run(cmd string, args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	cfgPath := fs.String("config", "config/platform.yaml", "settings file")
	control := fs.String("control", os.Getenv("PMS_CONTROL_DSN"), "control database DSN")
	account := fs.String("account", "", "account (tenant) id")
	shard := fs.String("shard", "", "shard id (place)")
	to := fs.String("to", "", "target shard (move)")
	id := fs.String("id", "", "move id (default: generated)")
	var shards multi
	fs.Var(&shards, "shard-dsn", "init: shard-0=DSN[|REPLICATION_DSN], repeatable")
	_ = fs.Parse(args)
	if *control == "" {
		return errors.New("no control database: pass -control or set PMS_CONTROL_DSN")
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if cmd == "init" {
		var list []directory.Shard
		for _, s := range shards {
			idPart, dsn, ok := strings.Cut(s, "=")
			if !ok {
				return fmt.Errorf("bad -shard-dsn %q, want shard-0=DSN", s)
			}
			repl := dsn
			if d, r, ok := strings.Cut(dsn, "|"); ok {
				dsn, repl = d, r
			}
			list = append(list, directory.Shard{ID: idPart, DSN: dsn, ReplConn: repl})
		}
		if len(list) == 0 {
			return errors.New("init needs at least one -shard-dsn")
		}
		return cluster.Init(ctx, *control, list)
	}

	c, err := cluster.Open(ctx, *control, cluster.Options{DirectoryTTL: cfg.Directory.TTL, Router: cfg.RouterOptions()})
	if err != nil {
		return err
	}
	defer c.Close()
	out := json.NewEncoder(os.Stdout)
	out.SetIndent("", "  ")

	switch cmd {
	case "place":
		if *account == "" || *shard == "" {
			return errors.New("place needs -account and -shard")
		}
		return c.Place(ctx, *account, *shard)
	case "move":
		if *account == "" || *to == "" {
			return errors.New("move needs -account and -to")
		}
		if *id == "" {
			*id = fmt.Sprintf("%s-%d", *account, time.Now().Unix())
		}
		mc := cfg.MoverConfig()
		mc.OnStep = func(step string, took time.Duration) {
			log.Info("move step", "move_id", *id, "account", *account, "step", step, "seconds", took.Seconds())
		}
		m, err := c.NewMove(ctx, *id, *account, *to, mc)
		if err != nil {
			return err
		}
		log.Info("move starting", "move_id", *id, "account", *account, "source", m.Source.ID, "target", *to)
		res, err := m.Run(ctx)
		if err != nil {
			return fmt.Errorf("%w (re-run with -id %s to resume, or reshard abort -id %s before it flips)", err, *id, *id)
		}
		return out.Encode(res)
	case "abort":
		if *id == "" {
			return errors.New("abort needs -id")
		}
		var acc, target string
		if err := c.Control.QueryRowContext(ctx, `SELECT account_id, target FROM moves WHERE move_id = $1`, *id).Scan(&acc, &target); err != nil {
			return fmt.Errorf("move %s: %w", *id, err)
		}
		m, err := c.NewMove(ctx, *id, acc, target, mover.Config{})
		if err != nil {
			return err
		}
		return mover.Abort(ctx, m)
	case "status":
		q := `SELECT move_id, account_id, source, target, step, started_at, updated_at, coalesce(error, '') FROM moves`
		var qargs []any
		if *account != "" {
			q += ` WHERE account_id = $1`
			qargs = append(qargs, *account)
		}
		rows, err := c.Control.QueryContext(ctx, q+` ORDER BY started_at DESC LIMIT 50`, qargs...)
		if err != nil {
			return err
		}
		defer rows.Close()
		type row struct {
			MoveID, Account, Source, Target, Step string
			Started, Updated                      time.Time
			Error                                 string `json:",omitempty"`
		}
		var all []row
		for rows.Next() {
			var r row
			if err := rows.Scan(&r.MoveID, &r.Account, &r.Source, &r.Target, &r.Step, &r.Started, &r.Updated, &r.Error); err != nil {
				return err
			}
			all = append(all, r)
		}
		return out.Encode(all)
	}
	return fmt.Errorf("unknown command %q", cmd)
}
