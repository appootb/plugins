package postgres

import (
	"fmt"

	"github.com/appootb/substratum/v2/storage"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func init() {
	storage.RegisterSQLDialectImplementor(&dialect{})
}

type dialect struct{}

func (s dialect) Open(cfg storage.Config) gorm.Dialector {
	dsn := fmt.Sprintf("host=%s port=%s user=%s dbname=%s password=%s",
		cfg.Host, cfg.Port, cfg.Username, cfg.Database, cfg.Password)
	if params := cfg.Params.Encode(cfg.Schema); params != "" {
		dsn = fmt.Sprintf("%s %s", dsn, params)
	}
	return postgres.Open(dsn)
}
