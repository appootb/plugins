// Package mysql registers a GORM SQL dialect for MySQL.
//
// Blank-import; only one SQL dialect implementor may be registered process-wide.
// Default query params: charset=utf8mb4, parseTime=True, loc=Local.
package mysql

import (
	"fmt"

	"github.com/appootb/substratum/v2/configure"
	"github.com/appootb/substratum/v2/storage"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const (
	ParamCharset   = "charset"
	ParamParseTime = "parseTime"
	ParamLocal     = "loc"
)

func init() {
	storage.RegisterSQLDialectImplementor(&dialect{})
}

type dialect struct{}

// buildDSN constructs the go-sql-driver DSN for the address.
func buildDSN(cfg configure.Address) string {
	if cfg.Params == nil {
		cfg.Params = configure.AddrParams{}
	}
	if _, ok := cfg.Params[ParamCharset]; !ok {
		cfg.Params[ParamCharset] = "utf8mb4"
	}
	if _, ok := cfg.Params[ParamParseTime]; !ok {
		cfg.Params[ParamParseTime] = "True"
	}
	if _, ok := cfg.Params[ParamLocal]; !ok {
		cfg.Params[ParamLocal] = "Local"
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?%s",
		cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.NameSpace, cfg.Params.Encode("&"))
}

func (s *dialect) Open(cfg configure.Address) gorm.Dialector {
	return mysql.Open(buildDSN(cfg))
}
