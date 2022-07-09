package kafka

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

type message struct {
	ctx context.Context
	svr *kafkaBackend

	topic string
	group string

	key       string
	content   []byte
	props     map[string]string
	timestamp time.Time
	headers   []kafka.Header
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
	return time.Time{}
}

// Retry times.
func (m *message) Retry() int {
	for _, header := range m.headers {
		if header.Key == "retry" {
			retry, _ := strconv.Atoi(string(header.Value))
			return retry
		}
	}
	return 0
}

// IsPing returns true for a ping message.
func (m *message) IsPing() bool {
	return false
}

// Begin to process the message.
func (m *message) Begin() {}

// Cancel indicates the message should be ignored.
func (m *message) Cancel() {}

// End indicates a successful process.
func (m *message) End() {}

// Requeue indicates the message should be retried.
func (m *message) Requeue() {
	err := m.svr.writeMessage(m.ctx, m.topic, kafka.Message{
		Key:   []byte(m.key),
		Value: m.content,
		Headers: []kafka.Header{
			{
				Key:   "retry",
				Value: []byte(strconv.Itoa(m.Retry() + 1)),
			},
		},
		Time: m.timestamp,
	})
	if err != nil {
		log.Printf("kafka requeue err: %v", err)
	}
}

// Fail indicates a failed process.
func (m *message) Fail() {}
