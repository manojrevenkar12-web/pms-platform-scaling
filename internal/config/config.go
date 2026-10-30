// Package config loads config/platform.yaml.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/mover"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/router"
)

// Platform is the whole settings file.
type Platform struct {
	API struct {
		Listen       string            `yaml:"listen"`
		Keys         map[string]string `yaml:"keys"` // sha256 hex -> account id
		MaxBodyBytes int64             `yaml:"max_body_bytes"`
	} `yaml:"api"`
	Directory struct {
		TTL time.Duration `yaml:"ttl"`
	} `yaml:"directory"`
	Router struct {
		MaxWait      time.Duration `yaml:"max_wait"`
		Backoff      time.Duration `yaml:"backoff"`
		MaxBackoff   time.Duration `yaml:"max_backoff"`
		MaxOpenConns int           `yaml:"max_open_conns"`
	} `yaml:"router"`
	Booking struct {
		DefaultSlots int `yaml:"default_slots"`
	} `yaml:"booking"`
	Mover struct {
		PollEvery    time.Duration `yaml:"poll_every"`
		StepTimeout  time.Duration `yaml:"step_timeout"`
		CleanupAfter bool          `yaml:"cleanup_after"`
		SkipChecksum bool          `yaml:"skip_checksum"`
	} `yaml:"mover"`
}

// Load reads and checks the file.
func Load(path string) (Platform, error) {
	var p Platform
	b, err := os.ReadFile(path)
	if err != nil {
		return p, err
	}
	if err := yaml.Unmarshal(b, &p); err != nil {
		return p, fmt.Errorf("%s: %w", path, err)
	}
	switch {
	case p.API.Listen == "":
		return p, fmt.Errorf("%s: api.listen is required", path)
	case p.Booking.DefaultSlots < 1 || p.Booking.DefaultSlots > 256:
		return p, fmt.Errorf("%s: booking.default_slots must be 1..256", path)
	case p.Directory.TTL <= 0 || p.Router.MaxWait <= 0:
		return p, fmt.Errorf("%s: directory.ttl and router.max_wait must be positive", path)
	}
	for h := range p.API.Keys {
		if len(h) != 64 {
			return p, fmt.Errorf("%s: api.keys must be SHA-256 hex digests", path)
		}
	}
	return p, nil
}

// RouterOptions converts the router settings.
func (p Platform) RouterOptions() router.Options {
	return router.Options{MaxWait: p.Router.MaxWait, Backoff: p.Router.Backoff, MaxBackoff: p.Router.MaxBackoff, MaxOpenConns: p.Router.MaxOpenConns}
}

// MoverConfig converts the mover settings.
func (p Platform) MoverConfig() mover.Config {
	return mover.Config{PollEvery: p.Mover.PollEvery, StepTimeout: p.Mover.StepTimeout, CleanupAfter: p.Mover.CleanupAfter, SkipChecksum: p.Mover.SkipChecksum}
}
