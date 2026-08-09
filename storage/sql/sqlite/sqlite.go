// Package sqlite registers a GORM SQL dialect for SQLite.
//
// Blank-import; only one SQL dialect implementor may be registered process-wide.
// The database file path is taken from Address.NameSpace.
package sqlite

import (
	"github.com/appootb/substratum/v2/configure"
	"github.com/appootb/substratum/v2/storage"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func init() {
	storage.RegisterSQLDialectImplementor(&dialect{})
}

type dialect struct{}

func (s *dialect) Open(cfg configure.Address) gorm.Dialector {
	return sqlite.Open(cfg.NameSpace)
}
