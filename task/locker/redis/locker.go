// Package redis implements task.Locker with Redis SETNX + lease renewal.
//
// Key: task:scheduler:locker:{scheduler}
// Value: random token; Lua scripts renew/delete only if token matches.
// Lock blocks until acquired; a background touch extends the TTL until Unlock.
package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	sctx "github.com/appootb/substratum/v2/context"
	"github.com/appootb/substratum/v2/logger"
	"github.com/appootb/substratum/v2/storage"
	"github.com/appootb/substratum/v2/task"
	"github.com/appootb/substratum/v2/util/random"
	"github.com/go-redis/redis/v8"
)

const (
	RandomValueLength  = 20
	LockerTouchTimeout = time.Second * 3
)

const (
	TaskLockerKey = "task:scheduler:locker:%s"
)

var (
	touchScript = redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("expire", KEYS[1], ARGV[2])
		else
			return 0
		end
	`)

	deleteScript = redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`)
)

var (
	impl = &locker{
		component: os.Getenv("COMPONENT"),
	}
)

func init() {
	task.RegisterLockerImplementor(impl)
}

// InitComponent overrides the storage component used for Redis.
func InitComponent(component string) {
	impl.component = component
}

type mutexData struct {
	ctx    context.Context
	cancel context.CancelFunc

	key   string
	value string
}

type locker struct {
	mutex     sync.Map
	component string
	redisFor  func(key string) redis.Cmdable
}

func (l *locker) getRedis(key string) redis.Cmdable {
	if l.redisFor != nil {
		return l.redisFor(key)
	}
	return storage.Implementor().Get(l.component).GetRedis(key)
}

// lockerKey builds the Redis key for a scheduler name.
func lockerKey(scheduler string) string {
	return fmt.Sprintf(TaskLockerKey, scheduler)
}

// Lock tries to get the locker of the scheduler,
// blocking until acquired. Returns a child context canceled on Unlock or lease loss.
func (l *locker) Lock(ctx context.Context, scheduler string) context.Context {
	mutex := &mutexData{
		key:   lockerKey(scheduler),
		value: random.String(RandomValueLength),
	}
	mutex.ctx, mutex.cancel = context.WithCancel(ctx)
	rds := l.getRedis(mutex.key)

	for {
		// Respect parent cancellation while waiting for the lock.
		select {
		case <-ctx.Done():
			mutex.cancel()
			return mutex.ctx
		default:
		}

		reply, err := rds.SetNX(sctx.Context(), mutex.key, mutex.value, LockerTouchTimeout*2).Result()
		if err != nil || !reply {
			time.Sleep(LockerTouchTimeout)
			continue
		}
		l.mutex.Store(scheduler, mutex)
		go l.touch(mutex)
		break
	}

	return mutex.ctx
}

// Unlock gives up the schedule locker.
func (l *locker) Unlock(scheduler string) {
	v, ok := l.mutex.Load(scheduler)
	if !ok {
		return
	}
	mutex := v.(*mutexData)
	rds := l.getRedis(mutex.key)
	status, err := deleteScript.Run(sctx.Context(), rds, []string{mutex.key}, mutex.value).Bool()
	if err != nil {
		logger.Error("task.locker unlock redis key failed", logger.Content{
			"error": err.Error(),
		})
	} else if !status {
		logger.Error("task.locker unlock redis status error", logger.Content{
			"status": status,
		})
	}
	l.mutex.Delete(scheduler)
	mutex.cancel()
}

func (l *locker) touch(mutex *mutexData) {
	ticker := time.NewTicker(LockerTouchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-mutex.ctx.Done():
			return

		case <-ticker.C:
			if err := l.renew(mutex); err != nil {
				mutex.cancel()
				return
			}
		}
	}
}

func (l *locker) renew(mutex *mutexData) error {
	var (
		err   error
		reply bool
	)

	rds := l.getRedis(mutex.key)
	duration := fmt.Sprintf("%d", LockerTouchTimeout*2/time.Second)

	for i := 0; i < 3; i++ {
		reply, err = touchScript.Run(sctx.Context(), rds, []string{mutex.key}, mutex.value, duration).Bool()
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		if reply {
			return nil
		}
		return errors.New("unlocked")
	}

	return err
}
