package etcd

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/appootb/substratum/v2/configure"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestParseETCDAddr(t *testing.T) {
	tests := []struct {
		name    string
		addr    string
		want    *etcdConfig
		wantErr bool
	}{
		{
			name: "single host with path",
			addr: "http://127.0.0.1:2379/config",
			want: &etcdConfig{
				Endpoints: []string{"http://127.0.0.1:2379"},
				Path:      "/config/",
			},
		},
		{
			name: "credentials and trailing slash",
			addr: "https://user:s3cret@etcd.example.com:2379/cfg/",
			want: &etcdConfig{
				Endpoints: []string{"https://etcd.example.com:2379"},
				Username:  "user",
				Password:  "s3cret",
				Path:      "/cfg/",
			},
		},
		{
			name: "multi host",
			addr: "http://u:p@h1:2379,h2:2379/config/app",
			want: &etcdConfig{
				Endpoints: []string{"http://h1:2379", "http://h2:2379"},
				Username:  "u",
				Password:  "p",
				Path:      "/config/app/",
			},
		},
		{
			name: "root path becomes slash",
			addr: "http://127.0.0.1:2379",
			want: &etcdConfig{
				Endpoints: []string{"http://127.0.0.1:2379"},
				Path:      "/",
			},
		},
		{
			name:    "empty",
			addr:    "  ",
			wantErr: true,
		},
		{
			name:    "missing scheme",
			addr:    "127.0.0.1:2379/config",
			wantErr: true,
		},
		{
			name:    "empty host entry",
			addr:    "http://h1:2379,,h2:2379/config",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseETCDAddr(tt.addr)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseETCDAddr: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %#v\nwant %#v", got, tt.want)
			}
		})
	}
}

func TestFullAndRelativeKey(t *testing.T) {
	p := &etcd{path: "/config/"}
	if got := p.fullKey("myapp/Host"); got != "/config/myapp/Host" {
		t.Fatalf("fullKey = %q", got)
	}
	if got := p.relativeKey("/config/myapp/Host"); got != "myapp/Host" {
		t.Fatalf("relativeKey = %q", got)
	}
}

type stubKV struct {
	putKey, putVal string
	putErr         error

	getKey  string
	getResp *clientv3.GetResponse
	getErr  error

	closed bool
	// unused watch
	watchChan clientv3.WatchChan
}

func (s *stubKV) Put(_ context.Context, key, val string, _ ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	s.putKey, s.putVal = key, val
	return &clientv3.PutResponse{}, s.putErr
}

func (s *stubKV) Get(_ context.Context, key string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	s.getKey = key
	return s.getResp, s.getErr
}

func (s *stubKV) Watch(context.Context, string, ...clientv3.OpOption) clientv3.WatchChan {
	return s.watchChan
}

func (s *stubKV) Close() error {
	s.closed = true
	return nil
}

func TestSetGet_UsesPrefix(t *testing.T) {
	stub := &stubKV{
		getResp: &clientv3.GetResponse{
			Header: &etcdserverpb.ResponseHeader{Revision: 9},
			Kvs: []*mvccpb.KeyValue{
				{Key: []byte("/config/app/Host"), Value: []byte("localhost")},
			},
		},
	}
	p := &etcd{path: "/config/", cli: stub}

	if err := p.Set("app/Host", "localhost"); err != nil {
		t.Fatal(err)
	}
	if stub.putKey != "/config/app/Host" || stub.putVal != "localhost" {
		t.Fatalf("Put = %q %q", stub.putKey, stub.putVal)
	}

	pairs, err := p.Get("app", true)
	if err != nil {
		t.Fatal(err)
	}
	if stub.getKey != "/config/app" {
		t.Fatalf("Get key = %q", stub.getKey)
	}
	if pairs.Version != 9 || len(pairs.KVs) != 1 {
		t.Fatalf("pairs = %#v", pairs)
	}
	if pairs.KVs[0].Key != "app/Host" || pairs.KVs[0].Value != "localhost" {
		t.Fatalf("kv = %#v", pairs.KVs[0])
	}
}

func TestSet_PropagatesError(t *testing.T) {
	want := errors.New("put failed")
	p := &etcd{path: "/", cli: &stubKV{putErr: want}}
	if err := p.Set("k", "v"); !errors.Is(err, want) {
		t.Fatalf("err = %v", err)
	}
}

func TestTypeAndClose(t *testing.T) {
	stub := &stubKV{}
	p := &etcd{cli: stub}
	if p.Type() != "etcd" {
		t.Fatalf("Type = %q", p.Type())
	}
	p.Close()
	if !stub.closed {
		t.Fatal("expected Close on client")
	}
}

func TestWatch_ReturnsBufferedChannel(t *testing.T) {
	// Watch starts a goroutine; use a never-closed empty channel so watch blocks
	// without panicking, then cancel is handled by process exit in short test.
	ch := make(chan clientv3.WatchResponse)
	p := &etcd{path: "/config/", cli: &stubKV{watchChan: ch}}
	events, err := p.Watch("app", 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if cap(events) != DefaultChanLen {
		t.Fatalf("cap = %d, want %d", cap(events), DefaultChanLen)
	}
	// Ensure type implements configure.Backend.
	var _ configure.Backend = p
}
