package kafka

import (
	"reflect"
	"testing"

	"github.com/appootb/substratum/v2/configure"
	"github.com/segmentio/kafka-go"
)

func TestParseBrokers(t *testing.T) {
	tests := []struct {
		name string
		cfg  configure.Address
		want []string
	}{
		{
			name: "single with port",
			cfg:  configure.Address{Host: "kafka.local", Port: "9092"},
			want: []string{"kafka.local:9092"},
		},
		{
			name: "multi host shared port",
			cfg:  configure.Address{Host: "a,b, c", Port: "9092"},
			want: []string{"a:9092", "b:9092", "c:9092"},
		},
		{
			name: "host already has port",
			cfg:  configure.Address{Host: "a:9092,b:9093"},
			want: []string{"a:9092", "b:9093"},
		},
		{
			name: "empty host entries skipped",
			cfg:  configure.Address{Host: "a,,b", Port: "1"},
			want: []string{"a:1", "b:1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseBrokers(tt.cfg)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %v want %v", got, tt.want)
			}
		})
	}
}

func TestPropsHeadersRoundTrip(t *testing.T) {
	in := map[string]string{PropertyRetry: "2", "k": "v"}
	headers := propsToHeaders(in)
	out := headersToProps(headers)
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("got %#v want %#v", out, in)
	}
	if propsToHeaders(nil) != nil {
		t.Fatal("nil props should yield nil headers")
	}
	if len(headersToProps(nil)) != 0 {
		t.Fatal("nil headers should yield empty map")
	}
}

func TestMessageRetry(t *testing.T) {
	m := &message{props: map[string]string{PropertyRetry: "3"}}
	if m.Retry() != 3 {
		t.Fatalf("Retry = %d", m.Retry())
	}
	m2 := &message{}
	if m2.Retry() != 0 {
		t.Fatalf("empty Retry = %d", m2.Retry())
	}
}

func TestOpenAndType(t *testing.T) {
	s := &kafkaBackend{}
	v, err := s.Open(configure.Address{Host: "h1,h2", Port: "9092"})
	if err != nil {
		t.Fatal(err)
	}
	w := v.(*wrapper)
	if !reflect.DeepEqual(w.brokers, []string{"h1:9092", "h2:9092"}) {
		t.Fatalf("brokers = %v", w.brokers)
	}
	if s.Type() != string(configure.Kafka) {
		t.Fatalf("Type = %q", s.Type())
	}
	// compile-time-ish check Header type still used
	_ = kafka.Header{}
}
