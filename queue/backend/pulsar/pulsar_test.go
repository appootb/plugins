package pulsar

import (
	"testing"
	"time"

	ppulsar "github.com/apache/pulsar-client-go/pulsar"
	"github.com/appootb/substratum/v2/configure"
)

func TestParseURL(t *testing.T) {
	tests := []struct {
		name string
		cfg  configure.Address
		want string
	}{
		{
			name: "default pulsar",
			cfg:  configure.Address{Host: "broker", Port: "6650"},
			want: "pulsar://broker:6650",
		},
		{
			name: "ssl",
			cfg: configure.Address{
				Host:   "broker",
				Port:   "6651",
				Params: configure.AddrParams{"ssl": "true"},
			},
			want: "pulsar+ssl://broker:6651",
		},
		{
			name: "tdmq http",
			cfg: configure.Address{
				Host:   "tdmq.example.com",
				Params: configure.AddrParams{"tdmq": "1"},
			},
			want: "http://tdmq.example.com",
		},
		{
			name: "tdmq https",
			cfg: configure.Address{
				Host:   "tdmq.example.com",
				Port:   "443",
				Params: configure.AddrParams{"tdmq": "1", "ssl": "true"},
			},
			want: "https://tdmq.example.com:443",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseURL(tt.cfg); got != tt.want {
				t.Fatalf("got %q want %q", got, tt.want)
			}
		})
	}
}

func TestTopicPath(t *testing.T) {
	if got := topicPath("public", "default", "orders"); got != "persistent://public/default/orders" {
		t.Fatalf("got %q", got)
	}
}

func TestMessageNotBefore(t *testing.T) {
	ts := time.Unix(1_700_000_000, 0)
	m := &message{timestamp: ts, delay: 5 * time.Second}
	if !m.NotBefore().Equal(ts.Add(5 * time.Second)) {
		t.Fatalf("NotBefore = %v", m.NotBefore())
	}
}

func TestMessageRetry_UsesReconsumeTimesProperty(t *testing.T) {
	m := &message{
		props: map[string]string{
			ppulsar.SysPropertyReconsumeTimes: "3",
		},
	}
	if got := m.Retry(); got != 3 {
		t.Fatalf("Retry = %d, want 3", got)
	}
}

func TestMessageRetry_EmptyWithoutRaw(t *testing.T) {
	m := &message{}
	if got := m.Retry(); got != 0 {
		t.Fatalf("Retry = %d, want 0", got)
	}
}

func TestType(t *testing.T) {
	s := &pulsarBackend{}
	if s.Type() != string(configure.Pulsar) {
		t.Fatalf("Type = %q", s.Type())
	}
}
