package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/appootb/substratum/queue"
	"github.com/appootb/substratum/storage"
	"github.com/appootb/substratum/util/hash"
	"github.com/go-redis/redis/v8"
)

const (
	QueueIdempotentExpire = time.Hour * 2
	QueueIdempotentKey    = "queue:plugin:id:%s:%s"
)

var (
	Queue = &idempotent{
		ctx: context.Background(),
	}
)

func init() {
	queue.RegisterIdempotentImplementor(Queue)
}

func InitRedis(s storage.Storage) {
	Queue.caches = s.GetRedisz()
}

type idempotent struct {
	ctx    context.Context
	caches []redis.Cmdable
}

func (r *idempotent) getRedis(key string) redis.Cmdable {
	return r.caches[hash.Sum(key)%int64(len(r.caches))]
}

// BeforeProcess invoked before process message.
// Returns true to continue the message processing.
// Returns false to invoke Cancel for the message.
func (r *idempotent) BeforeProcess(msg queue.Message) bool {
	key := fmt.Sprintf(QueueIdempotentKey, msg.UniqueID(), msg.Topic())
	locked, err := r.getRedis(key).SetNX(r.ctx, key, time.Now(), QueueIdempotentExpire).Result()
	if err != nil {
		return false
	}
	return locked
}

// AfterProcess invoked after processing.
func (r *idempotent) AfterProcess(msg queue.Message, status queue.ProcessStatus) {
	switch status {
	case queue.Canceled,
		queue.Succeeded:
		// do nothing
	case queue.Failed,
		queue.Requeued:
		key := fmt.Sprintf(QueueIdempotentKey, msg.UniqueID(), msg.Topic())
		r.getRedis(key).Del(r.ctx, key)
	}
}
