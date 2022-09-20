package pulsar

import (
	"context"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

type message struct {
	ctx context.Context
	svr pulsar.Consumer
	raw pulsar.Message

	topic string
	group string

	key       string
	content   []byte
	props     map[string]string
	timestamp time.Time
	delay     time.Duration
}

// Topic name of this message.
func (m *message) Topic() string {
	return m.topic
}

// Group name of this message.
func (m *message) Group() string {
	return m.group
}

// Key returns the unique key ID of this message.
func (m *message) Key() string {
	return m.key
}

// Content returns the message body content.
func (m *message) Content() []byte {
	return m.content
}

// Properties returns the properties of this message.
func (m *message) Properties() map[string]string {
	return m.props
}

// Timestamp indicates the creation time of the message.
func (m *message) Timestamp() time.Time {
	return m.timestamp
}

// NotBefore indicates the message should not be processed before this timestamp.
func (m *message) NotBefore() time.Time {
	return m.timestamp.Add(m.delay)
}

// Retry times.
func (m *message) Retry() int {
	return int(m.raw.RedeliveryCount())
}

// IsPing returns true for a ping message.
func (m *message) IsPing() bool {
	return false
}

// Begin to process the message.
func (m *message) Begin() {}

// Cancel indicates the message should be ignored.
func (m *message) Cancel() {
	m.svr.Ack(m.raw)
}

// End indicates a successful process.
func (m *message) End() {
	m.svr.Ack(m.raw)
}

// Requeue indicates the message should be retried.
func (m *message) Requeue() {
	m.svr.ReconsumeLater(m.raw, time.Duration(m.Retry())*time.Second)
}

// Fail indicates a failed process.
func (m *message) Fail() {
	m.svr.Nack(m.raw)
}
