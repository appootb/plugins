// Package v7 registers a common storage dialect for Elasticsearch 7.x.
//
// Blank-import. Host may be comma-separated; ssl=true selects https.
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

// buildAddresses expands host list into scheme://host[:port] URLs.
func buildAddresses(cfg configure.Address) []string {
	schema := "http"
	if strings.ToLower(cfg.Params["ssl"]) == "true" {
		schema = "https"
	}
	hosts := strings.Split(cfg.Host, ",")
	addresses := make([]string, 0, len(hosts))
	for _, host := range hosts {
		host = strings.TrimSpace(host)
		if host == "" {
			continue
		}
		if cfg.Port != "" {
			addresses = append(addresses, fmt.Sprintf("%s://%s:%s", schema, host, cfg.Port))
		} else {
			addresses = append(addresses, fmt.Sprintf("%s://%s", schema, host))
		}
	}
	return addresses
}

func (s *dialect) Open(cfg configure.Address) (interface{}, error) {
	cfg7 := es7.Config{
		Addresses: buildAddresses(cfg),
		Username:  cfg.Username,
		Password:  cfg.Password,
	}
	return es7.NewClient(cfg7)
}
