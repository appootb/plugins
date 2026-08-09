// Package etcd implements discovery.Backend backed by etcd v3.
//
// Blank-import when ETCD is set (same URL shape as configure/backend/etcd):
//
//	http://user:pass@127.0.0.1:2379/discovery
//	http://user:pass@h1:2379,h2:2379/discovery
//
// During `go test`, auto-registration is skipped.
package etcd

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	sctx "github.com/appootb/substratum/v2/context"
	"github.com/appootb/substratum/v2/discovery"
	"github.com/appootb/substratum/v2/logger"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
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

// discoveryClient is the etcd surface used by this backend.
type discoveryClient interface {
	Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error)
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
	Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error)
	Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan
	Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error)
	KeepAlive(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error)
	Close() error
}

// sessionFactory creates a concurrency session (for Incr locking).
type sessionFactory func(cli *clientv3.Client, opts ...concurrency.SessionOption) (*concurrency.Session, error)

func init() {
	if testing.Testing() {
		return
	}
	addr := os.Getenv("ETCD")
	if addr == "" {
		panic("empty etcd config addr")
	}
	backend, err := newFromAddr(addr)
	if err != nil {
		panic("initialize etcd failed: " + err.Error())
	}
	discovery.RegisterBackendImplementor(backend)
}

type etcdConfig struct {
	Endpoints []string
	Username  string
	Password  string
	Path      string
}

// parseETCDAddr parses discovery ETCD URL (multi-host authority supported).
func parseETCDAddr(addr string) (*etcdConfig, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil, fmt.Errorf("empty etcd address")
	}

	scheme, rest, ok := strings.Cut(addr, "://")
	if !ok || scheme == "" {
		return nil, fmt.Errorf("invalid etcd address %q: missing scheme", addr)
	}

	var userinfo, hostPath string
	if at := strings.LastIndex(rest, "@"); at >= 0 {
		userinfo = rest[:at]
		hostPath = rest[at+1:]
	} else {
		hostPath = rest
	}

	path := "/"
	hostsPart := hostPath
	if slash := strings.Index(hostPath, "/"); slash >= 0 {
		hostsPart = hostPath[:slash]
		path = hostPath[slash:]
	}
	if hostsPart == "" {
		return nil, fmt.Errorf("invalid etcd address %q: empty host", addr)
	}

	cfg := &etcdConfig{
		Path: strings.TrimRight(path, "/") + "/",
	}
	if userinfo != "" {
		u, err := url.Parse(scheme + "://" + userinfo + "@localhost/")
		if err != nil {
			return nil, fmt.Errorf("invalid etcd userinfo: %w", err)
		}
		cfg.Username = u.User.Username()
		cfg.Password, _ = u.User.Password()
	}

	for _, host := range strings.Split(hostsPart, ",") {
		host = strings.TrimSpace(host)
		if host == "" {
			return nil, fmt.Errorf("invalid etcd address %q: empty host entry", addr)
		}
		cfg.Endpoints = append(cfg.Endpoints, fmt.Sprintf("%s://%s", scheme, host))
	}
	return cfg, nil
}

func newFromAddr(addr string) (*etcd, error) {
	cfg, err := parseETCDAddr(addr)
	if err != nil {
		return nil, err
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:            cfg.Endpoints,
		DialTimeout:          DialTimeout,
		DialKeepAliveTime:    KeepAliveTime,
		DialKeepAliveTimeout: DialTimeout,
		Username:             cfg.Username,
		Password:             cfg.Password,
	})
	if err != nil {
		return nil, err
	}
	return &etcd{
		path:    cfg.Path,
		cli:     cli,
		raw:     cli,
		newSess: concurrency.NewSession,
	}, nil
}

type etcd struct {
	path string
	cli  discoveryClient
	// raw is required for concurrency.NewSession (needs *clientv3.Client).
	raw     *clientv3.Client
	newSess sessionFactory
}

func (p *etcd) fullKey(key string) string {
	return p.path + key
}

func (p *etcd) relativeKey(key string) string {
	return strings.TrimPrefix(key, p.path)
}

// Type returns the backend provider type.
func (p *etcd) Type() string {
	return "etcd"
}

// Set value for the specified key with an optional TTL lease.
func (p *etcd) Set(key, value string, ttl time.Duration) error {
	var options []clientv3.OpOption
	if ttl > 0 {
		leaseCtx, leaseCancel := context.WithTimeout(sctx.Context(), WriteTimeout)
		defer leaseCancel()
		lease, err := p.cli.Grant(leaseCtx, int64(ttl.Seconds()))
		if err != nil {
			return err
		}
		options = append(options, clientv3.WithLease(lease.ID))
	}

	ctx, cancel := context.WithTimeout(sctx.Context(), WriteTimeout)
	defer cancel()
	_, err := p.cli.Put(ctx, p.fullKey(key), value, options...)
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
	resp, err := p.cli.Get(ctx, p.fullKey(key), options...)
	if err != nil {
		return nil, err
	}

	version := uint64(resp.Header.GetRevision())
	kvs := make([]*discovery.KVPair, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		kvs = append(kvs, &discovery.KVPair{
			Key:     p.relativeKey(string(kv.Key)),
			Value:   string(kv.Value),
			Version: version,
		})
	}
	return &discovery.KVPairs{
		KVs:     kvs,
		Version: version,
	}, nil
}

// Incr atomically increments a numeric value under a distributed mutex.
func (p *etcd) Incr(key string) (int64, error) {
	if p.raw == nil {
		return 0, fmt.Errorf("discovery.etcd: incr requires real etcd client")
	}
	newSess := p.newSess
	if newSess == nil {
		newSess = concurrency.NewSession
	}
	session, err := newSess(p.raw)
	if err != nil {
		return 0, err
	}
	defer session.Close()

	mutex := concurrency.NewMutex(session, p.fullKey(key))
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
	go p.watch(key, dir, int64(version), eventsChan)
	return eventsChan, nil
}

// KeepAlive registers key/value with a TTL and renews the lease until shutdown.
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
				_, err = p.cli.Delete(sctx.Context(), p.fullKey(key))
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
	if err := p.cli.Close(); err != nil {
		logger.Error("discovery.etcd close error", logger.Content{
			"error": err.Error(),
		})
	}
}

func (p *etcd) keepAlive(key, value string, ttl time.Duration, withRetry bool) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
Retry:
	ctx, cancel := context.WithTimeout(sctx.Context(), WriteTimeout)
	lease, err := p.cli.Grant(ctx, int64(ttl.Seconds()))
	cancel()
	if err != nil {
		if withRetry {
			time.Sleep(RetryTimeout)
			goto Retry
		}
		return nil, err
	}

	ctx, cancel = context.WithTimeout(sctx.Context(), WriteTimeout)
	_, err = p.cli.Put(ctx, p.fullKey(key), value, clientv3.WithLease(lease.ID))
	cancel()
	if err != nil {
		if withRetry {
			time.Sleep(RetryTimeout)
			goto Retry
		}
		return nil, err
	}

	ch, err := p.cli.KeepAlive(sctx.Context(), lease.ID)
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

	resp, err := p.cli.Get(ctx, p.fullKey(key), options...)
	if err != nil {
		return 0, err
	}

	if eventsChan != nil {
		rev := uint64(resp.Header.GetRevision())
		for _, kv := range resp.Kvs {
			select {
			case eventsChan <- &discovery.WatchEvent{
				EventType: discovery.Refresh,
				KVPair: discovery.KVPair{
					Key:     p.relativeKey(string(kv.Key)),
					Value:   string(kv.Value),
					Version: rev,
				},
			}:
			default:
				logger.Error("discovery.etcd sync event dropped", logger.Content{
					"key": p.relativeKey(string(kv.Key)),
				})
			}
		}
	}
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
	etcdChan := p.cli.Watch(ctx, p.fullKey(key), options...)

	for {
		select {
		case <-sctx.Context().Done():
			cancel()
			return

		case resp, ok := <-etcdChan:
			if !ok {
				cancel()
				logger.Error("discovery.etcd watch channel closed", logger.Content{
					"key": key,
				})
				time.Sleep(time.Second * 5)
				goto Retry
			}
			if resp.CompactRevision > 0 {
				time.Sleep(time.Second)
				logger.Info("discovery.etcd compacted", logger.Content{
					"compact_revision": resp.CompactRevision,
				})
				revision, _ = p.sync(key, dir, eventsChan)
				cancel()
				goto Retry
			} else if err := resp.Err(); err != nil {
				cancel()
				logger.Error("discovery.etcd watch error", logger.Content{
					"error": err.Error(),
				})
				time.Sleep(time.Second * 5)
				goto Retry
			}

			if resp.Header.Revision > 0 {
				revision = resp.Header.Revision
			}
			if resp.IsProgressNotify() {
				continue
			}

			for _, evt := range resp.Events {
				if evt == nil || evt.Kv == nil {
					continue
				}
				wEvent := &discovery.WatchEvent{
					KVPair: discovery.KVPair{
						Key:     p.relativeKey(string(evt.Kv.Key)),
						Value:   string(evt.Kv.Value),
						Version: uint64(resp.Header.GetRevision()),
					},
				}
				if evt.Type == mvccpb.PUT {
					wEvent.EventType = discovery.Update
				} else {
					wEvent.EventType = discovery.Delete
				}
				select {
				case eventsChan <- wEvent:
				case <-sctx.Context().Done():
					cancel()
					return
				}
			}
			revision = resp.Header.GetRevision()
		}
	}
}
