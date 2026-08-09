// Package postgres registers a GORM SQL dialect for PostgreSQL.
//
// Blank-import; only one SQL dialect implementor may be registered process-wide.
package postgres

import (
	"fmt"

	"github.com/appootb/substratum/v2/configure"
	"github.com/appootb/substratum/v2/storage"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func init() {
	storage.RegisterSQLDialectImplementor(&dialect{})
}

type dialect struct{}

// buildDSN constructs the pgx/libpq-style keyword DSN.
func buildDSN(cfg configure.Address) string {
	dsn := fmt.Sprintf("host=%s port=%s user=%s dbname=%s password=%s",
		cfg.Host, cfg.Port, cfg.Username, cfg.NameSpace, cfg.Password)
	if params := cfg.Params.Encode(" "); params != "" {
		dsn = fmt.Sprintf("%s %s", dsn, params)
	}
	return dsn
}

func (s *dialect) Open(cfg configure.Address) gorm.Dialector {
	return postgres.Open(buildDSN(cfg))
}
