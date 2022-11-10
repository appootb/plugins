package etcd

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/appootb/substratum/v2/configure"
	sctx "github.com/appootb/substratum/v2/context"
	"github.com/appootb/substratum/v2/logger"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/v3"
)

const (
	DialTimeout   = time.Second * 3
	ReadTimeout   = time.Second * 3
	WriteTimeout  = time.Second * 3
	KeepAliveTime = time.Second * 5
)

const (
	DefaultChanLen = 1000
)

func init() {
	addr := os.Getenv("ETCD")
	if addr == "" {
		panic("empty etcd config addr")
	}
	//
	uri, err := url.Parse(addr)
	if err != nil {
		return
	}
	//
	cfg := clientv3.Config{
		DialTimeout:          DialTimeout,
		DialKeepAliveTime:    KeepAliveTime,
		DialKeepAliveTimeout: DialTimeout,
		Username:             uri.User.Username(),
	}
	cfg.Password, _ = uri.User.Password()
	//
	if strings.Contains(uri.Host, ",") {
		hosts := strings.Split(uri.Host, ",")
		cfg.Endpoints = make([]string, 0, len(hosts))
		for _, host := range hosts {
			if uri.Port() != "" {
				cfg.Endpoints = append(cfg.Endpoints, fmt.Sprintf("%s://%s:%s", uri.Scheme, host, uri.Port()))
			} else {
				cfg.Endpoints = append(cfg.Endpoints, fmt.Sprintf("%s://%s", uri.Scheme, host))
			}
		}
	} else {
		cfg.Endpoints = []string{fmt.Sprintf("%s://%s:%s", uri.Scheme, uri.Hostname(), uri.Port())}
	}
	//
	cli, err := clientv3.New(cfg)
	if err != nil {
		panic("initialize etcd failed: " + err.Error())
	}
	//
	configure.RegisterBackendImplementor(&etcd{
		path:   strings.TrimRight(uri.Path, "/") + "/",
		Client: cli,
	})
}

type etcd struct {
	path string
	*clientv3.Client
}

// Type returns the backend provider type.
func (p *etcd) Type() string {
	return "etcd"
}

// Set value for the specified key.
func (p *etcd) Set(key, value string) error {
	ctx, cancel := context.WithTimeout(sctx.Context(), WriteTimeout)
	defer cancel()
	key = fmt.Sprintf("%s/%s", p.path, key)
	_, err := p.Client.Put(ctx, p.path+key, value)
	return err
}

// Get the value of the specified key or directory.
func (p *etcd) Get(key string, dir bool) (*configure.KVPairs, error) {
	var options []clientv3.OpOption
	if dir {
		options = append(options, clientv3.WithPrefix())
	}

	ctx, cancel := context.WithTimeout(sctx.Context(), ReadTimeout)
	defer cancel()
	key = fmt.Sprintf("%s/%s", p.path, key)
	resp, err := p.Client.Get(ctx, p.path+key, options...)
	if err != nil {
		return nil, err
	}
	//
	version := uint64(resp.Header.GetRevision())
	kvs := make([]*configure.KVPair, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		kvs = append(kvs, &configure.KVPair{
			Key:     strings.TrimPrefix(string(kv.Key), p.path),
			Value:   string(kv.Value),
			Version: version,
		})
	}
	return &configure.KVPairs{
		KVs:     kvs,
		Version: version,
	}, nil
}

// Watch for changes of the specified key or directory.
func (p *etcd) Watch(key string, version uint64, dir bool) (configure.EventChan, error) {
	eventsChan := make(configure.EventChan, DefaultChanLen)
	//
	key = fmt.Sprintf("%s/%s", p.path, key)
	go p.watch(key, dir, int64(version), eventsChan)
	//
	return eventsChan, nil
}

// Close the provider connection.
func (p *etcd) Close() {
	err := p.Client.Close()
	if err != nil {
		logger.Error("discovery.etcd close error", logger.Content{
			"error": err.Error(),
		})
	}
}

func (p *etcd) sync(key string, dir bool, eventsChan configure.EventChan) (int64, error) {
	var options []clientv3.OpOption
	if dir {
		options = append(options, clientv3.WithPrefix())
	}

	ctx, cancel := context.WithTimeout(sctx.Context(), ReadTimeout)
	defer cancel()
	//
	resp, err := p.Client.Get(ctx, p.path+key, options...)
	if err != nil {
		return 0, err
	}
	//
	if eventsChan != nil {
		for _, kv := range resp.Kvs {
			eventsChan <- &configure.WatchEvent{
				EventType: configure.Refresh,
				KVPair: configure.KVPair{
					Key:     strings.TrimPrefix(string(kv.Key), p.path),
					Value:   string(kv.Value),
					Version: uint64(resp.Header.GetRevision()),
				},
			}
		}
	}
	//
	return resp.Header.Revision, nil
}

func (p *etcd) watch(key string, dir bool, revision int64, eventsChan configure.EventChan) {
Retry:
	options := []clientv3.OpOption{
		clientv3.WithRev(revision),
		clientv3.WithProgressNotify(),
	}
	if dir {
		options = append(options, clientv3.WithPrefix())
	}

	ctx, cancel := context.WithCancel(sctx.Context())
	etcdChan := p.Client.Watch(ctx, p.path+key, options...)

	for {
		select {
		case <-sctx.Context().Done():
			cancel()
			return

		case resp := <-etcdChan:
			if resp.CompactRevision > 0 {
				time.Sleep(time.Second)
				logger.Info("configure.etcd compacted", logger.Content{
					"compact_revision": resp.CompactRevision,
				})
				revision, _ = p.sync(key, dir, eventsChan)
				goto Retry
			} else if err := resp.Err(); err != nil {
				cancel()
				logger.Error("configure.etcd watch error", logger.Content{
					"error": err.Error(),
				})
				time.Sleep(time.Second * 5)
				goto Retry
			}
			//
			if resp.Header.Revision > 0 {
				revision = resp.Header.Revision
			}
			if resp.IsProgressNotify() {
				continue
			}

			for _, evt := range resp.Events {
				wEvent := &configure.WatchEvent{
					KVPair: configure.KVPair{
						Key:   strings.TrimPrefix(string(evt.Kv.Key), p.path),
						Value: string(evt.Kv.Value),
					},
				}
				if evt.Type == mvccpb.PUT {
					wEvent.EventType = configure.Update
				} else {
					wEvent.EventType = configure.Delete
				}
				eventsChan <- wEvent
			}
			//
			revision = resp.Header.GetRevision()
		}
	}
}
