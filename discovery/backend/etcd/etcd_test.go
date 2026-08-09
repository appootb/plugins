package etcd

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/appootb/substratum/v2/discovery"
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
			name: "single",
			addr: "http://127.0.0.1:2379/discovery",
			want: &etcdConfig{
				Endpoints: []string{"http://127.0.0.1:2379"},
				Path:      "/discovery/",
			},
		},
		{
			name: "multi with auth",
			addr: "http://u:p@h1:2379,h2:2379/svc",
			want: &etcdConfig{
				Endpoints: []string{"http://h1:2379", "http://h2:2379"},
				Username:  "u",
				Password:  "p",
				Path:      "/svc/",
			},
		},
		{name: "empty", addr: "", wantErr: true},
		{name: "no scheme", addr: "host:2379", wantErr: true},
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
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %#v want %#v", got, tt.want)
			}
		})
	}
}

type stubClient struct {
	putKey, putVal string
	getResp        *clientv3.GetResponse
	getErr         error
	putErr         error
	closed         bool
}

func (s *stubClient) Put(_ context.Context, key, val string, _ ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	s.putKey, s.putVal = key, val
	return &clientv3.PutResponse{}, s.putErr
}
func (s *stubClient) Get(_ context.Context, _ string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return s.getResp, s.getErr
}
func (s *stubClient) Delete(context.Context, string, ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return &clientv3.DeleteResponse{}, nil
}
func (s *stubClient) Watch(context.Context, string, ...clientv3.OpOption) clientv3.WatchChan {
	return make(chan clientv3.WatchResponse)
}
func (s *stubClient) Grant(context.Context, int64) (*clientv3.LeaseGrantResponse, error) {
	return &clientv3.LeaseGrantResponse{ID: 1}, nil
}
func (s *stubClient) KeepAlive(context.Context, clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	ch := make(chan *clientv3.LeaseKeepAliveResponse)
	return ch, nil
}
func (s *stubClient) Close() error { s.closed = true; return nil }

func TestSetGet_Prefix(t *testing.T) {
	stub := &stubClient{
		getResp: &clientv3.GetResponse{
			Header: &etcdserverpb.ResponseHeader{Revision: 3},
			Kvs: []*mvccpb.KeyValue{
				{Key: []byte("/discovery/svc/a"), Value: []byte("1")},
			},
		},
	}
	p := &etcd{path: "/discovery/", cli: stub}

	if err := p.Set("svc/a", "1", 0); err != nil {
		t.Fatal(err)
	}
	if stub.putKey != "/discovery/svc/a" {
		t.Fatalf("put key = %q", stub.putKey)
	}

	pairs, err := p.Get("svc", true)
	if err != nil {
		t.Fatal(err)
	}
	if pairs.Version != 3 || len(pairs.KVs) != 1 || pairs.KVs[0].Key != "svc/a" {
		t.Fatalf("pairs = %#v", pairs)
	}
}

func TestSet_PropagatesError(t *testing.T) {
	want := errors.New("boom")
	p := &etcd{path: "/", cli: &stubClient{putErr: want}}
	if err := p.Set("k", "v", 0); !errors.Is(err, want) {
		t.Fatalf("err = %v", err)
	}
}

func TestTypeClose(t *testing.T) {
	stub := &stubClient{}
	p := &etcd{cli: stub}
	if p.Type() != "etcd" {
		t.Fatalf("Type = %q", p.Type())
	}
	p.Close()
	if !stub.closed {
		t.Fatal("expected close")
	}
	var _ discovery.Backend = p
}
