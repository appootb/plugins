package kafka

import (
	"log"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
)

type message struct {
	svr *kafka

	topic string
	group string

	key       string
	content   []byte
	timestamp time.Time
	headers   []*sarama.RecordHeader
}

// Queue name of this message.
func (m *message) Queue() string {
	return m.topic
}

// Topic name of this message.
func (m *message) Topic() string {
	return m.group
}

// Unique ID of this message.
func (m *message) UniqueID() string {
	return m.key
}

// Message body content.
func (m *message) Content() []byte {
	return m.content
}

// The creation time of the message.
func (m *message) Timestamp() time.Time {
	return m.timestamp
}

// The message should not be processed before this timestamp.
func (m *message) NotBefore() time.Time {
	return time.Time{}
}

// Message retry times.
func (m *message) Retry() int {
	for _, header := range m.headers {
		if string(header.Key) == "retry" {
			retry, _ := strconv.Atoi(string(header.Value))
			return retry
		}
	}
	return 0
}

// Return true for a ping message.
func (m *message) IsPing() bool {
	return false
}

// Begin to process the message.
func (m *message) Begin() {}

// Indicate the message should be ignored.
func (m *message) Cancel() {}

// End indicates a successful process.
func (m *message) End() {}

// Requeue indicates the message should be retried.
func (m *message) Requeue() {
	err := m.svr.writeMessage(&sarama.ProducerMessage{
		Topic: m.topic,
		Key:   sarama.StringEncoder(m.key),
		Value: sarama.ByteEncoder(m.content),
		Headers: []sarama.RecordHeader{
			{
				Key:   []byte("retry"),
				Value: []byte(strconv.Itoa(m.Retry() + 1)),
			},
		},
		Timestamp: m.timestamp,
	})
	if err != nil {
		log.Printf("kafka requeue err: %v", err)
	}
}

// Fail indicates a failed process.
func (m *message) Fail() {}
