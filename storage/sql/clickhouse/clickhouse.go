// Package clickhouse registers a GORM SQL dialect for ClickHouse.
//
// Blank-import; only one SQL dialect implementor may be registered process-wide.
package clickhouse

import (
	"fmt"

	"github.com/appootb/substratum/v2/configure"
	"github.com/appootb/substratum/v2/storage"
	"gorm.io/driver/clickhouse"
	"gorm.io/gorm"
)

func init() {
	storage.RegisterSQLDialectImplementor(&dialect{})
}

type dialect struct{}

// buildDSN constructs the clickhouse-go DSN.
func buildDSN(cfg configure.Address) string {
	params := ""
	if cfg.Params != nil {
		params = cfg.Params.Encode("&")
	}
	return fmt.Sprintf("tcp://%s:%s?database=%s&username=%s&password=%s&%s",
		cfg.Host, cfg.Port, cfg.NameSpace, cfg.Username, cfg.Password, params)
}

func (s *dialect) Open(cfg configure.Address) gorm.Dialector {
	return clickhouse.Open(buildDSN(cfg))
}
