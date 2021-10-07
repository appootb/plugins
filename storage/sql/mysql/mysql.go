package mysql

import (
	"fmt"

	"github.com/appootb/substratum/storage"
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

func (s dialect) Open(cfg storage.Config) gorm.Dialector {
	if _, ok := cfg.Params[ParamCharset]; !ok {
		cfg.Params[ParamCharset] = "utf8mb4"
	}
	if _, ok := cfg.Params[ParamParseTime]; !ok {
		cfg.Params[ParamParseTime] = "True"
	}
	if _, ok := cfg.Params[ParamLocal]; !ok {
		cfg.Params[ParamLocal] = "Local"
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?%s",
		cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Database, cfg.Params.Encode(cfg.Schema))
	return mysql.Open(dsn)
}
