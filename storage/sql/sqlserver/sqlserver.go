package sqlserver

import (
	"fmt"

	"github.com/appootb/substratum/storage"
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

func (s dialect) Open(cfg storage.Config) gorm.Dialector {
	cfg.Params[ParamDatabase] = cfg.Database
	dsn := fmt.Sprintf("sqlserver://%s:%s@%s:%s?%s",
		cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Params.Encode(cfg.Schema))
	return sqlserver.Open(dsn)
}
