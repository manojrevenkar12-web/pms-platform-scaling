package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestShippedSettingsLoad(t *testing.T) {
	p, err := Load("../../config/platform.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if p.Booking.DefaultSlots != 8 || p.Router.MaxWait.Seconds() != 10 || !p.Mover.CleanupAfter {
		t.Fatalf("unexpected settings: %+v", p)
	}
}

func TestRejectsBadSettings(t *testing.T) {
	dir := t.TempDir()
	for name, body := range map[string]string{
		"no listen":   "api: {}\ndirectory: {ttl: 1s}\nrouter: {max_wait: 1s}\nbooking: {default_slots: 8}\n",
		"zero slots":  "api: {listen: ':1'}\ndirectory: {ttl: 1s}\nrouter: {max_wait: 1s}\nbooking: {default_slots: 0}\n",
		"plain key":   "api: {listen: ':1', keys: {secret: acc}}\ndirectory: {ttl: 1s}\nrouter: {max_wait: 1s}\nbooking: {default_slots: 8}\n",
		"no max wait": "api: {listen: ':1'}\ndirectory: {ttl: 1s}\nbooking: {default_slots: 8}\n",
	} {
		path := filepath.Join(dir, "p.yaml")
		_ = os.WriteFile(path, []byte(body), 0o644)
		if _, err := Load(path); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
