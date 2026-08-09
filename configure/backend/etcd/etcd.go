// Package etcd implements configure.Backend backed by etcd v3.
//
// Blank-import this package when ETCD is set to a URL such as:
//
//	http://user:pass@127.0.0.1:2379/config
//	http://user:pass@h1:2379,h2:2379/config
//
// Path becomes the key prefix (trailing slash normalized). Multiple hosts in
// the authority are expanded to scheme://host endpoints.
//
// During `go test`, auto-registration is skipped so pure logic can be tested
// without a live etcd cluster; production blank-import still fails fast when
// ETCD is empty or unreachable.
package etcd

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/appootb/substratum/v2/configure"
	sctx "github.com/appootb/substratum/v2/context"
	"github.com/appootb/substratum/v2/logger"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
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

// kvAPI is the etcd client surface used by this backend (for tests).
type kvAPI interface {
	Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error)
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
	Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan
	Close() error
}

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
	configure.RegisterBackendImplementor(backend)
}

// etcdConfig is the result of parsing the ETCD connection URL.
type etcdConfig struct {
	Endpoints []string
	Username  string
	Password  string
	Path      string // key prefix, always ends with "/"
}

// parseETCDAddr parses an etcd configure URL into client endpoints and prefix.
//
// Supported forms:
//   - http://host:2379/config
//   - http://user:pass@host:2379/config/
//   - http://user:pass@h1:2379,h2:2379/config  (multi-host authority)
func parseETCDAddr(addr string) (*etcdConfig, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil, fmt.Errorf("empty etcd address")
	}

	// Multi-host authorities are not valid URL Host values; split hosts manually.
	// Example: http://u:p@h1:2379,h2:2379/path
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
		// url.UserPassword-compatible parse via temporary URL.
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
		path: cfg.Path,
		cli:  cli,
	}, nil
}

type etcd struct {
	path string
	cli  kvAPI
}

// fullKey joins the configured path prefix with a relative key.
func (p *etcd) fullKey(key string) string {
	return p.path + key
}

// relativeKey strips the path prefix from an absolute etcd key.
func (p *etcd) relativeKey(key string) string {
	return strings.TrimPrefix(key, p.path)
}

// Type returns the backend provider type.
func (p *etcd) Type() string {
	return "etcd"
}

// Set value for the specified key (relative to the ETCD URL path prefix).
func (p *etcd) Set(key, value string) error {
	ctx, cancel := context.WithTimeout(sctx.Context(), WriteTimeout)
	defer cancel()
	_, err := p.cli.Put(ctx, p.fullKey(key), value)
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
	resp, err := p.cli.Get(ctx, p.fullKey(key), options...)
	if err != nil {
		return nil, err
	}

	version := uint64(resp.Header.GetRevision())
	kvs := make([]*configure.KVPair, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		kvs = append(kvs, &configure.KVPair{
			Key:     p.relativeKey(string(kv.Key)),
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
// Events are delivered asynchronously on a buffered channel.
func (p *etcd) Watch(key string, version uint64, dir bool) (configure.EventChan, error) {
	eventsChan := make(configure.EventChan, DefaultChanLen)
	go p.watch(key, dir, int64(version), eventsChan)
	return eventsChan, nil
}

// Close the provider connection.
func (p *etcd) Close() {
	if err := p.cli.Close(); err != nil {
		logger.Error("configure.etcd close error", logger.Content{
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

	resp, err := p.cli.Get(ctx, p.fullKey(key), options...)
	if err != nil {
		return 0, err
	}

	if eventsChan != nil {
		rev := uint64(resp.Header.GetRevision())
		for _, kv := range resp.Kvs {
			// Non-blocking send: drop if consumer is slow to avoid stalling sync.
			select {
			case eventsChan <- &configure.WatchEvent{
				EventType: configure.Refresh,
				KVPair: configure.KVPair{
					Key:     p.relativeKey(string(kv.Key)),
					Value:   string(kv.Value),
					Version: rev,
				},
			}:
			default:
				logger.Error("configure.etcd sync event dropped", logger.Content{
					"key": p.relativeKey(string(kv.Key)),
				})
			}
		}
	}
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
	etcdChan := p.cli.Watch(ctx, p.fullKey(key), options...)

	for {
		select {
		case <-sctx.Context().Done():
			cancel()
			return

		case resp, ok := <-etcdChan:
			if !ok {
				cancel()
				logger.Error("configure.etcd watch channel closed", logger.Content{
					"key": key,
				})
				time.Sleep(time.Second * 5)
				goto Retry
			}
			if resp.CompactRevision > 0 {
				time.Sleep(time.Second)
				logger.Info("configure.etcd compacted", logger.Content{
					"compact_revision": resp.CompactRevision,
				})
				revision, _ = p.sync(key, dir, eventsChan)
				cancel()
				goto Retry
			} else if err := resp.Err(); err != nil {
				cancel()
				logger.Error("configure.etcd watch error", logger.Content{
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
				wEvent := &configure.WatchEvent{
					KVPair: configure.KVPair{
						Key:     p.relativeKey(string(evt.Kv.Key)),
						Value:   string(evt.Kv.Value),
						Version: uint64(resp.Header.GetRevision()),
					},
				}
				if evt.Type == mvccpb.PUT {
					wEvent.EventType = configure.Update
				} else {
					wEvent.EventType = configure.Delete
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
