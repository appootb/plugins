package mysql

import (
	"strings"
	"testing"

	"github.com/appootb/substratum/v2/configure"
)

func TestBuildDSN_Defaults(t *testing.T) {
	dsn := buildDSN(configure.Address{
		Username:  "u",
		Password:  "p",
		Host:      "127.0.0.1",
		Port:      "3306",
		NameSpace: "db",
	})
	if !strings.HasPrefix(dsn, "u:p@tcp(127.0.0.1:3306)/db?") {
		t.Fatalf("dsn = %q", dsn)
	}
	for _, want := range []string{"charset=utf8mb4", "parseTime=True", "loc=Local"} {
		if !strings.Contains(dsn, want) {
			t.Fatalf("dsn missing %q: %s", want, dsn)
		}
	}
}

func TestBuildDSN_CustomParams(t *testing.T) {
	dsn := buildDSN(configure.Address{
		Username:  "u",
		Password:  "p",
		Host:      "h",
		Port:      "1",
		NameSpace: "db",
		Params:    configure.AddrParams{ParamCharset: "utf8", "timeout": "5s"},
	})
	if !strings.Contains(dsn, "charset=utf8") {
		t.Fatalf("dsn = %q", dsn)
	}
	if strings.Contains(dsn, "charset=utf8mb4") {
		t.Fatalf("should not override custom charset: %q", dsn)
	}
}
