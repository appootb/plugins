package v7

import (
	"reflect"
	"testing"

	"github.com/appootb/substratum/v2/configure"
)

func TestBuildAddresses(t *testing.T) {
	tests := []struct {
		name string
		cfg  configure.Address
		want []string
	}{
		{
			name: "multi http",
			cfg:  configure.Address{Host: "a, b", Port: "9200"},
			want: []string{"http://a:9200", "http://b:9200"},
		},
		{
			name: "https",
			cfg: configure.Address{
				Host: "es.local", Port: "443",
				Params: configure.AddrParams{"ssl": "true"},
			},
			want: []string{"https://es.local:443"},
		},
		{
			name: "no port",
			cfg:  configure.Address{Host: "es.local"},
			want: []string{"http://es.local"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildAddresses(tt.cfg)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %v want %v", got, tt.want)
			}
		})
	}
}
