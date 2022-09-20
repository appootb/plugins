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

func (s *dialect) Open(cfg configure.Address) gorm.Dialector {
	cfg.Params[ParamDatabase] = cfg.NameSpace
	dsn := fmt.Sprintf("sqlserver://%s:%s@%s:%s?%s",
		cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Params.Encode("&"))
	return sqlserver.Open(dsn)
}
