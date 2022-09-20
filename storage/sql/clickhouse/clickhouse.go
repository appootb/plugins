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

func (s *dialect) Open(cfg configure.Address) gorm.Dialector {
	dsn := fmt.Sprintf("tcp://%s:%s?database=%s&username=%s&password=%s&%s",
		cfg.Host, cfg.Port, cfg.NameSpace, cfg.Username, cfg.Password, cfg.Params.Encode("&"))
	return clickhouse.Open(dsn)
}
