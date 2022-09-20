package redis

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/appootb/substratum/v2/queue"
	"github.com/appootb/substratum/v2/storage"
	"github.com/go-redis/redis/v8"
)

const (
	QueueIdempotentExpire = time.Hour * 2
	QueueIdempotentKey    = "queue:plugin:id:%s:%s"
)

var (
	impl = &idempotent{
		ctx:       context.Background(),
		component: os.Getenv("COMPONENT"),
	}
)

func init() {
	queue.RegisterIdempotentImplementor(impl)
}

func InitComponent(component string) {
	impl.component = component
}

type idempotent struct {
	ctx       context.Context
	component string
}

func (r *idempotent) getRedis(key string) redis.Cmdable {
	return storage.Implementor().Get(r.component).GetRedis(key)
}

// BeforeProcess invoked before process message.
// Returns true to continue the message processing.
// Returns false to invoke Cancel for the message.
func (r *idempotent) BeforeProcess(msg queue.Message) bool {
	key := fmt.Sprintf(QueueIdempotentKey, msg.Key(), msg.Topic())
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
		key := fmt.Sprintf(QueueIdempotentKey, msg.Key(), msg.Topic())
		r.getRedis(key).Del(r.ctx, key)
	}
}
