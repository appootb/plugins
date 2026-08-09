// Package sqlserver registers a GORM SQL dialect for Microsoft SQL Server.
//
// Blank-import; only one SQL dialect implementor may be registered process-wide.
// Address.NameSpace is mapped to the database query parameter.
package sqlserver

import (
	"fmt"

	"github.com/appootb/substratum/v2/configure"
	"github.com/appootb/substratum/v2/storage"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
)

const (
	ParamDatabase = "database"
)

func init() {
	storage.RegisterSQLDialectImplementor(&dialect{})
}

type dialect struct{}

// buildDSN constructs the azure/go-mssqldb URL DSN.
func buildDSN(cfg configure.Address) string {
	if cfg.Params == nil {
		cfg.Params = configure.AddrParams{}
	}
	cfg.Params[ParamDatabase] = cfg.NameSpace
	return fmt.Sprintf("sqlserver://%s:%s@%s:%s?%s",
		cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Params.Encode("&"))
}

func (s *dialect) Open(cfg configure.Address) gorm.Dialector {
	return sqlserver.Open(buildDSN(cfg))
}
