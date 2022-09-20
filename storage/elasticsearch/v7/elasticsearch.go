package v7

import (
	"fmt"
	"strings"

	"github.com/appootb/substratum/v2/configure"
	"github.com/appootb/substratum/v2/storage"
	es7 "github.com/elastic/go-elasticsearch/v7"
)

func init() {
	storage.RegisterCommonDialectImplementor(configure.ElasticSearch7, &dialect{})
}

type dialect struct{}

func (s *dialect) Open(cfg configure.Address) (interface{}, error) {
	schema := "http"
	if strings.ToLower(cfg.Params["ssl"]) == "true" {
		schema = "https"
	}
	hosts := strings.Split(cfg.Host, ",")
	addresses := make([]string, 0, len(hosts))
	for _, host := range hosts {
		if cfg.Port != "" {
			addresses = append(addresses, fmt.Sprintf("%s://%s:%s", schema, host, cfg.Port))
		} else {
			addresses = append(addresses, fmt.Sprintf("%s://%s", schema, host))
		}
	}
	cfg7 := es7.Config{
		Addresses: addresses,
		Username:  cfg.Username,
		Password:  cfg.Password,
	}
	return es7.NewClient(cfg7)
}
