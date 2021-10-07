package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/appootb/substratum/storage"
	"github.com/appootb/substratum/task"
	"github.com/appootb/substratum/util/hash"
	"github.com/appootb/substratum/util/random"
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
	touchScript = `
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("expire", KEYS[1], ARGV[2])
	else
		return 0
	end
`

	deleteScript = `
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("DEL", KEYS[1])
	else
		return 0
	end
`
)

var (
	Locker = &locker{
		ctx: context.Background(),
	}
)

func init() {
	task.RegisterLockerImplementor(Locker)
}

func InitRedis(s storage.Storage) {
	Locker.caches = s.GetRedisz()
}

type mutexData struct {
	ctx    context.Context
	cancel context.CancelFunc

	key   string
	value string
}

type locker struct {
	ctx    context.Context
	mutex  sync.Map
	caches []redis.Cmdable
}

// Lock tries to get the locker of the scheduler,
// should be blocked before acquired the locker.
func (l *locker) Lock(ctx context.Context, scheduler string) context.Context {
	mutex := &mutexData{
		key:   fmt.Sprintf(TaskLockerKey, scheduler),
		value: random.String(RandomValueLength),
	}
	mutex.ctx, mutex.cancel = context.WithCancel(ctx)
	idx := hash.Sum(mutex.key) % int64(len(l.caches))

	for {
		reply, err := l.caches[idx].SetNX(l.ctx, mutex.key, mutex.value, LockerTouchTimeout*2).Result()
		if err != nil || !reply {
			time.Sleep(LockerTouchTimeout)
		} else {
			l.mutex.Store(scheduler, mutex)
			go l.touch(mutex)
			break
		}
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
	idx := hash.Sum(mutex.key) % int64(len(l.caches))
	status, err := l.caches[idx].Eval(l.ctx, deleteScript, []string{mutex.key}, mutex.value).Bool()
	if err != nil {
		// TODO err
	}
	if !status {
		// TODO failed
	}
	mutex.cancel()
}

func (l *locker) touch(mutex *mutexData) {
	ticker := time.NewTicker(LockerTouchTimeout)

	for {
		select {
		// Unlocked or parent context canceled.
		case <-mutex.ctx.Done():
			return

		case <-ticker.C:
			err := l.renew(mutex)
			if err != nil {
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

	idx := hash.Sum(mutex.key) % int64(len(l.caches))
	duration := fmt.Sprintf("%d", LockerTouchTimeout*2/time.Second)

	for i := 0; i < 3; i++ {
		reply, err = l.caches[idx].Eval(l.ctx, touchScript, []string{mutex.key}, mutex.value, duration).Bool()
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		if reply {
			return nil
		} else {
			return errors.New("unlocked")
		}
	}

	return err
}
