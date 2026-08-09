package redis

import (
	"testing"
	"time"

	"github.com/appootb/substratum/v2/queue"
)

type stubMsg struct {
	topic, key, group string
}

func (m stubMsg) Topic() string   { return m.topic }
func (m stubMsg) Group() string   { return m.group }
func (m stubMsg) Key() string     { return m.key }
func (m stubMsg) Content() []byte { return nil }
func (m stubMsg) Properties() map[string]string {
	return nil
}
func (m stubMsg) Timestamp() time.Time { return time.Time{} }
func (m stubMsg) NotBefore() time.Time { return time.Time{} }
func (m stubMsg) Retry() int           { return 0 }
func (m stubMsg) IsPing() bool         { return false }

func TestIdempotentKey(t *testing.T) {
	got := idempotentKey("orders", "msg-1", "workers")
	want := "queue:plugin:id:orders:msg-1:workers"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestInitComponent(t *testing.T) {
	prev := impl.component
	t.Cleanup(func() { impl.component = prev })
	InitComponent("queue-redis")
	if impl.component != "queue-redis" {
		t.Fatalf("component = %q", impl.component)
	}
}

func TestAfterProcess_StatusesNoPanic(t *testing.T) {
	// Without redisFor, AfterProcess for Succeeded/Canceled must not call Redis.
	r := &idempotent{}
	msg := stubMsg{topic: "t", key: "k", group: "g"}
	r.AfterProcess(msg, queue.Succeeded)
	r.AfterProcess(msg, queue.Canceled)
	// Failed/Requeued would hit storage — only exercise key helper path via status branch.
	var _ queue.Idempotent = r
}
