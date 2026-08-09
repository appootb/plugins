package sqlserver

import (
	"strings"
	"testing"

	"github.com/appootb/substratum/v2/configure"
)

func TestBuildDSN(t *testing.T) {
	dsn := buildDSN(configure.Address{
		Username: "u", Password: "p", Host: "h", Port: "1433", NameSpace: "appdb",
	})
	if !strings.HasPrefix(dsn, "sqlserver://u:p@h:1433?") {
		t.Fatalf("dsn = %q", dsn)
	}
	if !strings.Contains(dsn, "database=appdb") {
		t.Fatalf("missing database: %q", dsn)
	}
}
