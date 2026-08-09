// Package redis implements queue.Idempotent with Redis SETNX keys.
//
// Key shape: queue:plugin:id:{topic}:{key}:{group}
// BeforeProcess acquires a 2h lock; AfterProcess deletes on Failed/Requeued
// so the message can be processed again.
package redis

import (
	"fmt"
	"os"
	"time"

	sctx "github.com/appootb/substratum/v2/context"
	"github.com/appootb/substratum/v2/queue"
	"github.com/appootb/substratum/v2/storage"
	"github.com/go-redis/redis/v8"
)

const (
	QueueIdempotentExpire = time.Hour * 2
	QueueIdempotentKey    = "queue:plugin:id:%s:%s:%s"
)

var (
	impl = &idempotent{
		component: os.Getenv("COMPONENT"),
	}
)

func init() {
	queue.RegisterIdempotentImplementor(impl)
}

// InitComponent overrides the storage component used for Redis.
func InitComponent(component string) {
	impl.component = component
}

type idempotent struct {
	component string
	redisFor  func(key string) redis.Cmdable
}

func (r *idempotent) getRedis(key string) redis.Cmdable {
	if r.redisFor != nil {
		return r.redisFor(key)
	}
	return storage.Implementor().Get(r.component).GetRedis(key)
}

// idempotentKey builds the Redis key for a message identity.
func idempotentKey(topic, key, group string) string {
	return fmt.Sprintf(QueueIdempotentKey, topic, key, group)
}

// BeforeProcess returns true if this process acquired the idempotent lock.
func (r *idempotent) BeforeProcess(msg queue.Message) bool {
	key := idempotentKey(msg.Topic(), msg.Key(), msg.Group())
	locked, err := r.getRedis(key).SetNX(sctx.Context(), key, time.Now().UnixNano(), QueueIdempotentExpire).Result()
	if err != nil {
		return false
	}
	return locked
}

// AfterProcess releases the lock when processing should be retryable.
func (r *idempotent) AfterProcess(msg queue.Message, status queue.ProcessStatus) {
	switch status {
	case queue.Canceled, queue.Succeeded:
		// keep key until TTL so duplicates within window are ignored
	case queue.Failed, queue.Requeued:
		key := idempotentKey(msg.Topic(), msg.Key(), msg.Group())
		_ = r.getRedis(key).Del(sctx.Context(), key).Err()
	default:
	}
}
