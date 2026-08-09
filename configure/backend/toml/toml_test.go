package toml

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/appootb/substratum/v2/configure"
)

func TestParseAddr(t *testing.T) {
	tests := []struct {
		name       string
		addr       string
		wantPath   string
		wantPrefix string
		wantErr    bool
	}{
		{name: "raw path", addr: "/etc/app/config.toml", wantPath: "/etc/app/config.toml"},
		{name: "file url", addr: "file:///etc/app/config.toml", wantPath: "/etc/app/config.toml"},
		{name: "file url with prefix", addr: "file:///etc/app/config.toml#config/myapp", wantPath: "/etc/app/config.toml", wantPrefix: "config/myapp/"},
		{name: "empty", addr: "  ", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, prefix, err := parseAddr(tt.addr)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if path != tt.wantPath || prefix != tt.wantPrefix {
				t.Fatalf("path=%q prefix=%q, want path=%q prefix=%q", path, prefix, tt.wantPath, tt.wantPrefix)
			}
		})
	}
}

func TestProvider_SetGetWatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")

	p, err := newProvider(path, "config/myapp/")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	if p.Type() != "toml" {
		t.Fatalf("Type = %q", p.Type())
	}

	item := `{"type":"string","schema":"","value":"localhost","comment":"host"}`
	if err := p.Set("Host", item); err != nil {
		t.Fatal(err)
	}

	// File should exist and be non-empty.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("expected persisted file")
	}

	got, err := p.Get("Host", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.KVs) != 1 || got.KVs[0].Key != "Host" || got.KVs[0].Value != item {
		t.Fatalf("Get = %#v", got)
	}

	dirPairs, err := p.Get("", true)
	if err != nil {
		t.Fatal(err)
	}
	if len(dirPairs.KVs) != 1 {
		t.Fatalf("dir Get len = %d", len(dirPairs.KVs))
	}

	ch, err := p.Watch("", 0, true)
	if err != nil {
		t.Fatal(err)
	}

	// Sync may emit Refresh for existing keys when version advanced.
	// Also Set after Watch should emit Update.
	item2 := `{"type":"string","schema":"","value":"127.0.0.1","comment":"host"}`
	if err := p.Set("Host", item2); err != nil {
		t.Fatal(err)
	}

	deadline := time.After(2 * time.Second)
	var sawUpdate bool
	for !sawUpdate {
		select {
		case evt := <-ch:
			if evt.EventType == configure.Update && evt.Key == "Host" && evt.Value == item2 {
				sawUpdate = true
			}
		case <-deadline:
			t.Fatal("timeout waiting for watch update")
		}
	}
}

func TestProvider_GetMissing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.toml")
	p, err := newProvider(path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	got, err := p.Get("missing", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.KVs) != 0 {
		t.Fatalf("expected empty, got %#v", got)
	}
}
