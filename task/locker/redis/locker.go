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
}

// Lock tries to get the locker of the scheduler,
// should be blocked before acquired the locker.
func (l *locker) Lock(ctx context.Context, scheduler string) context.Context {
	mutex := &mutexData{
		key:   fmt.Sprintf(TaskLockerKey, scheduler),
		value: random.String(RandomValueLength),
	}
	mutex.ctx, mutex.cancel = context.WithCancel(ctx)
	rds := storage.Implementor().Get(l.component).GetRedis(mutex.key)

	for {
		reply, err := rds.SetNX(sctx.Context(), mutex.key, mutex.value, LockerTouchTimeout*2).Result()
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
	rds := storage.Implementor().Get(l.component).GetRedis(mutex.key)
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

	rds := storage.Implementor().Get(l.component).GetRedis(mutex.key)
	duration := fmt.Sprintf("%d", LockerTouchTimeout*2/time.Second)

	for i := 0; i < 3; i++ {
		reply, err = touchScript.Run(sctx.Context(), rds, []string{mutex.key}, mutex.value, duration).Bool()
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
