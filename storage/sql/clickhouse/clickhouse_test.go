package clickhouse

import (
	"strings"
	"testing"

	"github.com/appootb/substratum/v2/configure"
)

func TestBuildDSN(t *testing.T) {
	dsn := buildDSN(configure.Address{
		Host: "h", Port: "9000", NameSpace: "db", Username: "u", Password: "p",
		Params: configure.AddrParams{"read_timeout": "10"},
	})
	if !strings.HasPrefix(dsn, "tcp://h:9000?") {
		t.Fatalf("dsn = %q", dsn)
	}
	for _, p := range []string{"database=db", "username=u", "password=p", "read_timeout=10"} {
		if !strings.Contains(dsn, p) {
			t.Fatalf("missing %q in %s", p, dsn)
		}
	}
}
