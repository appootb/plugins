package sqlite

import (
	"testing"

	"github.com/appootb/substratum/v2/configure"
)

func TestOpen_UsesNameSpace(t *testing.T) {
	d := &dialect{}
	// Open returns a dialector; ensure it does not panic on empty params.
	dialector := d.Open(configure.Address{NameSpace: "file::memory:?cache=shared"})
	if dialector == nil {
		t.Fatal("expected dialector")
	}
	if dialector.Name() != "sqlite" {
		t.Fatalf("Name = %q", dialector.Name())
	}
}
