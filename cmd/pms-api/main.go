// Command pms-api serves the booking write path over HTTP.
//
//	pms-api -config config/platform.yaml -control "host=... dbname=control"
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/api"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/cluster"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/config"
)

func main() {
	cfgPath := flag.String("config", "config/platform.yaml", "settings file")
	control := flag.String("control", os.Getenv("PMS_CONTROL_DSN"), "control database DSN (or PMS_CONTROL_DSN)")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(*cfgPath, *control, log); err != nil {
		log.Error("pms-api stopped", "error", err.Error())
		os.Exit(1)
	}
}

func run(cfgPath, control string, log *slog.Logger) error {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return err
	}
	if control == "" {
		return errors.New("no control database: pass -control or set PMS_CONTROL_DSN")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	c, err := cluster.Open(ctx, control, cluster.Options{DirectoryTTL: cfg.Directory.TTL, Router: cfg.RouterOptions()})
	if err != nil {
		return err
	}
	defer c.Close()
	s := api.New(c, log, cfg.API.Keys, cfg.Booking.DefaultSlots, cfg.API.MaxBodyBytes)
	srv := &http.Server{Addr: cfg.API.Listen, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second, WriteTimeout: cfg.Router.MaxWait + 5*time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Info("listening", "addr", cfg.API.Listen, "shards", c.ShardIDs())
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), cfg.Router.MaxWait+5*time.Second)
	defer cancel()
	return srv.Shutdown(shutdown)
}
