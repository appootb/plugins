package clickhouse

import (
	"fmt"

	"github.com/appootb/substratum/storage"
	"gorm.io/driver/clickhouse"
	"gorm.io/gorm"
)

func init() {
	storage.RegisterSQLDialectImplementor(&dialect{})
}

type dialect struct{}

func (s dialect) Open(cfg storage.Config) gorm.Dialector {
	dsn := fmt.Sprintf("tcp://%s:%s?database=%s&username=%s&password=%s&%s",
		cfg.Host, cfg.Port, cfg.Database, cfg.Username, cfg.Password, cfg.Params.Encode(cfg.Schema))
	return clickhouse.Open(dsn)
}
