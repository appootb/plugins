package etcd

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	sctx "github.com/appootb/substratum/v2/context"
	"github.com/appootb/substratum/v2/discovery"
	"github.com/appootb/substratum/v2/logger"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const (
	DialTimeout   = time.Second * 3
	RetryTimeout  = time.Second * 3
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
	discovery.RegisterBackendImplementor(&etcd{
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

// Set value for the specified key with a specified ttl.
func (p *etcd) Set(key, value string, ttl time.Duration) error {
	var options []clientv3.OpOption
	if ttl > 0 {
		leaseCtx, leaseCancel := context.WithTimeout(sctx.Context(), WriteTimeout)
		defer leaseCancel()
		lease, err := p.Grant(leaseCtx, int64(ttl.Seconds()))
		if err != nil {
			return err
		}
		options = append(options, clientv3.WithLease(lease.ID))
	}

	ctx, cancel := context.WithTimeout(sctx.Context(), WriteTimeout)
	defer cancel()
	_, err := p.Client.Put(ctx, p.path+key, value, options...)
	return err
}

// Get the value of the specified key or directory.
func (p *etcd) Get(key string, dir bool) (*discovery.KVPairs, error) {
	var options []clientv3.OpOption
	if dir {
		options = append(options, clientv3.WithPrefix())
	}

	ctx, cancel := context.WithTimeout(sctx.Context(), ReadTimeout)
	defer cancel()
	resp, err := p.Client.Get(ctx, p.path+key, options...)
	if err != nil {
		return nil, err
	}
	//
	version := uint64(resp.Header.GetRevision())
	kvs := make([]*discovery.KVPair, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		kvs = append(kvs, &discovery.KVPair{
			Key:     strings.TrimPrefix(string(kv.Key), p.path),
			Value:   string(kv.Value),
			Version: version,
		})
	}
	return &discovery.KVPairs{
		KVs:     kvs,
		Version: version,
	}, nil
}

// Incr invokes an atomic value increase for the specified key.
func (p *etcd) Incr(key string) (int64, error) {
	session, err := concurrency.NewSession(p.Client)
	if err != nil {
		return 0, err
	}
	defer session.Close()

	mutex := concurrency.NewMutex(session, p.path+key)
	ctx, cancel := context.WithTimeout(sctx.Context(), WriteTimeout*2)
	defer cancel()
	if err = mutex.Lock(ctx); err != nil {
		return 0, err
	}
	defer mutex.Unlock(sctx.Context())

	num := int64(0)
	pairs, err := p.Get(key, false)
	if err != nil {
		return 0, err
	} else if len(pairs.KVs) > 0 {
		num, _ = strconv.ParseInt(pairs.KVs[0].Value, 10, 64)
	}
	num++
	if err = p.Set(key, strconv.FormatInt(num, 10), 0); err != nil {
		return 0, err
	}
	return num, nil
}

// Watch for changes of the specified key or directory.
func (p *etcd) Watch(key string, version uint64, dir bool) (discovery.EventChan, error) {
	eventsChan := make(discovery.EventChan, DefaultChanLen)
	//
	go p.watch(key, dir, int64(version), eventsChan)
	//
	return eventsChan, nil
}

// KeepAlive sets value and updates the ttl for the specified key.
func (p *etcd) KeepAlive(key, value string, ttl time.Duration) error {
	ch, err := p.keepAlive(key, value, ttl, false)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case m := <-ch:
				// channel closed, retry
				if m == nil {
					ch, _ = p.keepAlive(key, value, ttl, true)
				}
			case <-sctx.Context().Done():
				_, err = p.Client.Delete(sctx.Context(), p.path+key)
				if err != nil {
					logger.Info("discovery.etcd keepalive stopping", logger.Content{
						"error": err.Error(),
					})
				}
				return
			}
		}
	}()
	return nil
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

func (p *etcd) keepAlive(key, value string, ttl time.Duration, withRetry bool) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
Retry:
	// grant lease
	ctx, cancel := context.WithTimeout(sctx.Context(), WriteTimeout)
	lease, err := p.Grant(ctx, int64(ttl.Seconds()))
	cancel()
	if err != nil {
		if withRetry {
			time.Sleep(RetryTimeout)
			goto Retry
		}
		return nil, err
	}

	// put value with lease
	ctx, cancel = context.WithTimeout(sctx.Context(), WriteTimeout)
	_, err = p.Client.Put(ctx, p.path+key, value, clientv3.WithLease(lease.ID))
	cancel()
	if err != nil {
		if withRetry {
			time.Sleep(RetryTimeout)
			goto Retry
		}
		return nil, err
	}

	// keep alive to etcd
	ch, err := p.Client.KeepAlive(sctx.Context(), lease.ID)
	if err != nil {
		if withRetry {
			time.Sleep(RetryTimeout)
			goto Retry
		}
		return nil, err
	}

	return ch, nil
}

func (p *etcd) sync(key string, dir bool, eventsChan discovery.EventChan) (int64, error) {
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
			eventsChan <- &discovery.WatchEvent{
				EventType: discovery.Refresh,
				KVPair: discovery.KVPair{
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

func (p *etcd) watch(key string, dir bool, revision int64, eventsChan discovery.EventChan) {
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
				logger.Info("discovery.etcd compacted", logger.Content{
					"compact_revision": resp.CompactRevision,
				})
				revision, _ = p.sync(key, dir, eventsChan)
				goto Retry
			} else if err := resp.Err(); err != nil {
				cancel()
				logger.Error("discovery.etcd watch error", logger.Content{
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
				wEvent := &discovery.WatchEvent{
					KVPair: discovery.KVPair{
						Key:   strings.TrimPrefix(string(evt.Kv.Key), p.path),
						Value: string(evt.Kv.Value),
					},
				}
				if evt.Type == mvccpb.PUT {
					wEvent.EventType = discovery.Update
				} else {
					wEvent.EventType = discovery.Delete
				}
				eventsChan <- wEvent
			}
			//
			revision = resp.Header.GetRevision()
		}
	}
}
