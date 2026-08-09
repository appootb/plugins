package postgres

import (
	"strings"
	"testing"

	"github.com/appootb/substratum/v2/configure"
)

func TestBuildDSN(t *testing.T) {
	dsn := buildDSN(configure.Address{
		Host: "h", Port: "5432", Username: "u", NameSpace: "db", Password: "p",
		Params: configure.AddrParams{"sslmode": "disable"},
	})
	wantParts := []string{"host=h", "port=5432", "user=u", "dbname=db", "password=p", "sslmode=disable"}
	for _, p := range wantParts {
		if !strings.Contains(dsn, p) {
			t.Fatalf("dsn missing %q: %s", p, dsn)
		}
	}
}
