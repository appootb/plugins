package toml

import (
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/appootb/substratum/v2/configure"
	sctx "github.com/appootb/substratum/v2/context"
	"github.com/appootb/substratum/v2/logger"
	"github.com/fsnotify/fsnotify"
	"github.com/pelletier/go-toml"
)

const (
	DefaultChanLen = 1000
	ReloadDelay    = time.Millisecond * 500
)

func init() {
	addr := os.Getenv("TOML")
	if addr == "" {
		panic("empty toml config addr")
	}
	//
	path, prefix, err := parseAddr(addr)
	if err != nil {
		panic("initialize toml failed: " + err.Error())
	}
	//
	p, err := newProvider(path, prefix)
	if err != nil {
		panic("initialize toml failed: " + err.Error())
	}
	//
	configure.RegisterBackendImplementor(p)
}

type watch struct {
	ch     configure.EventChan
	key    string
	prefix bool
}

type provider struct {
	path    string
	prefix  string
	kvs     map[string]string
	version uint64
	watches []*watch
	event   configure.EventChan
	watcher *fsnotify.Watcher
	mu      sync.RWMutex
}

func parseAddr(addr string) (path, prefix string, err error) {
	uri, err := url.Parse(addr)
	if err != nil {
		return addr, "", nil
	}
	//
	switch uri.Scheme {
	case "file":
		path = uri.Path
		prefix = strings.TrimRight(uri.Fragment, "/")
		if prefix != "" {
			prefix += "/"
		}
	case "":
		path = addr
	default:
		path = addr
	}
	return path, prefix, nil
}

func newProvider(path, prefix string) (*provider, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	//
	p := &provider{
		path:    path,
		prefix:  prefix,
		kvs:     make(map[string]string),
		event:   make(configure.EventChan, DefaultChanLen),
		watcher: watcher,
	}
	if err = p.load(); err != nil {
		_ = watcher.Close()
		return nil, err
	}
	//
	dir := filepath.Dir(path)
	if err = watcher.Add(dir); err != nil {
		_ = watcher.Close()
		return nil, err
	}
	//
	go p.dispatch()
	go p.watchFile()
	return p, nil
}

func (p *provider) fullKey(key string) string {
	return p.prefix + key
}

func (p *provider) trimKey(key string) string {
	return strings.TrimPrefix(key, p.prefix)
}

// Type returns the backend provider type.
func (p *provider) Type() string {
	return "toml"
}

// Set value for the specified key.
func (p *provider) Set(key, value string) error {
	key = p.fullKey(key)
	//
	p.mu.Lock()
	p.kvs[key] = value
	version := atomic.AddUint64(&p.version, 1)
	err := p.saveLocked()
	p.mu.Unlock()
	if err != nil {
		return err
	}
	//
	p.event <- &configure.WatchEvent{
		EventType: configure.Update,
		KVPair: configure.KVPair{
			Key:     p.trimKey(key),
			Value:   value,
			Version: version,
		},
	}
	return nil
}

// Get the value of the specified key or directory.
func (p *provider) Get(key string, dir bool) (*configure.KVPairs, error) {
	key = p.fullKey(key)
	//
	p.mu.RLock()
	defer p.mu.RUnlock()
	//
	version := atomic.LoadUint64(&p.version)
	if !dir {
		if value, ok := p.kvs[key]; !ok {
			return &configure.KVPairs{
				Version: version,
			}, nil
		} else {
			return &configure.KVPairs{
				KVs: []*configure.KVPair{
					{
						Key:     p.trimKey(key),
						Value:   value,
						Version: version,
					},
				},
				Version: version,
			}, nil
		}
	}
	//
	kvs := make([]*configure.KVPair, 0)
	for k, v := range p.kvs {
		if strings.HasPrefix(k, key) {
			kvs = append(kvs, &configure.KVPair{
				Key:     p.trimKey(k),
				Value:   v,
				Version: version,
			})
		}
	}
	return &configure.KVPairs{
		KVs:     kvs,
		Version: version,
	}, nil
}

// Watch for changes of the specified key or directory.
func (p *provider) Watch(key string, version uint64, dir bool) (configure.EventChan, error) {
	eventsChan := make(configure.EventChan, DefaultChanLen)
	//
	p.mu.Lock()
	p.watches = append(p.watches, &watch{
		ch:     eventsChan,
		key:    p.fullKey(key),
		prefix: dir,
	})
	p.mu.Unlock()
	//
	if version < atomic.LoadUint64(&p.version) {
		go p.sync(key, dir, eventsChan)
	}
	//
	return eventsChan, nil
}

// Close the provider connection.
func (p *provider) Close() {
	if p.watcher != nil {
		err := p.watcher.Close()
		if err != nil {
			logger.Error("configure.toml close error", logger.Content{
				"error": err.Error(),
			})
		}
	}
}

func (p *provider) load() error {
	data, err := os.ReadFile(p.path)
	if err != nil {
		if os.IsNotExist(err) {
			return p.save()
		}
		return err
	}
	if len(data) == 0 {
		return nil
	}
	//
	kvs := make(map[string]string)
	if err = toml.Unmarshal(data, &kvs); err != nil {
		return err
	}
	//
	p.mu.Lock()
	p.kvs = kvs
	atomic.AddUint64(&p.version, 1)
	p.mu.Unlock()
	return nil
}

func (p *provider) save() error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.saveLocked()
}

func (p *provider) saveLocked() error {
	data, err := toml.Marshal(p.kvs)
	if err != nil {
		return err
	}
	//
	dir := filepath.Dir(p.path)
	if err = os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	//
	tmp, err := os.CreateTemp(dir, filepath.Base(p.path)+".*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	//
	if _, err = tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err = tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return os.Rename(tmpPath, p.path)
}

func (p *provider) sync(key string, dir bool, eventsChan configure.EventChan) {
	key = p.fullKey(key)
	version := atomic.LoadUint64(&p.version)
	//
	p.mu.RLock()
	defer p.mu.RUnlock()
	//
	for k, v := range p.kvs {
		if k == key || dir && strings.HasPrefix(k, key) {
			eventsChan <- &configure.WatchEvent{
				EventType: configure.Refresh,
				KVPair: configure.KVPair{
					Key:     p.trimKey(k),
					Value:   v,
					Version: version,
				},
			}
		}
	}
}

func (p *provider) reload() error {
	before := make(map[string]string)
	p.mu.RLock()
	for k, v := range p.kvs {
		before[k] = v
	}
	p.mu.RUnlock()
	//
	if err := p.load(); err != nil {
		return err
	}
	//
	version := atomic.LoadUint64(&p.version)
	after := make(map[string]string)
	p.mu.RLock()
	for k, v := range p.kvs {
		after[k] = v
	}
	p.mu.RUnlock()
	//
	for k, v := range after {
		if before[k] == v {
			continue
		}
		p.event <- &configure.WatchEvent{
			EventType: configure.Update,
			KVPair: configure.KVPair{
				Key:     p.trimKey(k),
				Value:   v,
				Version: version,
			},
		}
	}
	for k, v := range before {
		if _, ok := after[k]; ok {
			continue
		}
		p.event <- &configure.WatchEvent{
			EventType: configure.Delete,
			KVPair: configure.KVPair{
				Key:     p.trimKey(k),
				Value:   v,
				Version: version,
			},
		}
	}
	return nil
}

func (p *provider) dispatch() {
	for {
		select {
		case <-sctx.Context().Done():
			return

		case evt := <-p.event:
			p.mu.RLock()
			for _, w := range p.watches {
				if evt.Key == p.trimKey(w.key) ||
					w.prefix && strings.HasPrefix(p.fullKey(evt.Key), w.key) {
					w.ch <- evt
				}
			}
			p.mu.RUnlock()
		}
	}
}

func (p *provider) watchFile() {
	var timer *time.Timer
	//
	for {
		select {
		case <-sctx.Context().Done():
			if timer != nil {
				timer.Stop()
			}
			return

		case evt, ok := <-p.watcher.Events:
			if !ok {
				return
			}
			if evt.Name != p.path {
				continue
			}
			if evt.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename|fsnotify.Remove) == 0 {
				continue
			}
			if timer != nil {
				timer.Stop()
			}
			timer = time.AfterFunc(ReloadDelay, func() {
				if err := p.reload(); err != nil {
					logger.Error("configure.toml reload error", logger.Content{
						"error": err.Error(),
						"path":  p.path,
					})
				}
			})

		case err, ok := <-p.watcher.Errors:
			if !ok {
				return
			}
			logger.Error("configure.toml watch error", logger.Content{
				"error": err.Error(),
				"path":  p.path,
			})
		}
	}
}
