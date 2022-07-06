package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/appootb/substratum/v2/queue"
	"github.com/appootb/substratum/v2/util/snowflake"
	"github.com/segmentio/kafka-go"
)

var (
	Backend = &kafkaBackend{}
)

func init() {
	queue.RegisterBackendImplementor(Backend)
}

func Init(addrs []string) {
	Backend.addrs = addrs
}

type kafkaBackend struct {
	addrs    []string
	producer sync.Map
}

// Type returns backend type.
func (s *kafkaBackend) Type() string {
	return "kafka"
}

// Ping connect the backend server if not connected.
// Will be called before every Read/Write operation.
func (s *kafkaBackend) Ping() error {
	return nil
}

// MaxDelay returns the max delay duration supported by the backend.
// A negative value means no limitation.
// A zero value means delay operation is not supported.
func (s *kafkaBackend) MaxDelay() time.Duration {
	// Delay is unsupported
	return 0
}

// GetQueues returns all queue names in backend storage.
func (s *kafkaBackend) GetQueues() ([]string, error) {
	// Unsupported
	return []string{}, nil
}

// GetTopics returns all queue/topics in backend storage.
func (s *kafkaBackend) GetTopics() (map[string][]string, error) {
	// Unsupported
	return map[string][]string{}, nil
}

// GetQueueLength returns all topic length of specified queue in backend storage.
func (s *kafkaBackend) GetQueueLength(_ string) (map[string]int64, error) {
	// Unsupported
	return map[string]int64{}, nil
}

// GetTopicLength returns the specified queue/topic length in backend storage.
func (s *kafkaBackend) GetTopicLength(_, _ string) (int64, error) {
	// Unsupported
	return 0, nil
}

// Read subscribes the message of the specified queue and topic.
func (s *kafkaBackend) Read(ctx context.Context, topic, group string, ch chan<- queue.MessageWrapper) error {
	consumer := s.newConsumer(topic, group)

	var (
		err error
		msg kafka.Message
	)

	go func() {
		for {
			msg, err = consumer.ReadMessage(ctx)
			if errors.Is(err, io.EOF) {
				err = consumer.Close()
				if err != nil {
					log.Println("closing kafka consumer err:", err.Error())
				}
				consumer = s.newConsumer(topic, group)
				continue
			} else if err != nil {
				log.Fatal("error from kafka consumer:", err.Error())
			}

			ch <- &message{
				svr:       s,
				ctx:       ctx,
				topic:     topic,
				group:     group,
				key:       string(msg.Key),
				content:   msg.Value,
				timestamp: msg.Time,
				headers:   msg.Headers,
			}
		}
	}()

	return nil
}

// Write publishes content data to the specified queue.
func (s *kafkaBackend) Write(ctx context.Context, topic string, _ time.Duration, content []byte) error {
	keyID, err := snowflake.NextID()
	if err != nil {
		return err
	}
	return s.writeMessage(ctx, topic, kafka.Message{
		Key:   []byte(fmt.Sprintf("%s-%d", topic, keyID)),
		Value: content,
		Headers: []kafka.Header{
			{
				Key:   "retry",
				Value: []byte("0"),
			},
		},
	})
}

func (s *kafkaBackend) newConsumer(topic, group string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:                s.addrs,
		GroupID:                group,
		Topic:                  topic,
		MinBytes:               0,
		MaxWait:                200 * time.Millisecond,
		ReadLagInterval:        0,
		HeartbeatInterval:      3 * time.Second,
		CommitInterval:         0,
		PartitionWatchInterval: 5 * time.Second,
		WatchPartitionChanges:  true,
		SessionTimeout:         30 * time.Second,
		RebalanceTimeout:       30 * time.Second,
		JoinGroupBackoff:       5 * time.Second,
		RetentionTime:          24 * time.Hour,
		StartOffset:            kafka.LastOffset,
		ReadBackoffMin:         100 * time.Millisecond,
		ReadBackoffMax:         time.Second,
		Logger:                 &debugLogger{},
		ErrorLogger:            &errorLogger{},
		IsolationLevel:         kafka.ReadCommitted,
		MaxAttempts:            3,
	})
}

func (s *kafkaBackend) writeMessage(ctx context.Context, topic string, msg kafka.Message) error {
	var (
		producer *kafka.Writer
	)
	if p, ok := s.producer.Load(topic); ok {
		producer = p.(*kafka.Writer)
	} else {
		producer = &kafka.Writer{
			Addr:         kafka.TCP(s.addrs...),
			Topic:        topic,
			MaxAttempts:  10,
			BatchSize:    1,
			BatchTimeout: 200 * time.Millisecond,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
			RequiredAcks: kafka.RequireOne,
			Async:        false,
			Compression:  0,
			Logger:       &debugLogger{},
			ErrorLogger:  &errorLogger{},
			Transport:    kafka.DefaultTransport,
		}
		if pp, loaded := s.producer.LoadOrStore(topic, producer); loaded {
			_ = producer.Close()
			producer = pp.(*kafka.Writer)
		}
	}
	return producer.WriteMessages(ctx, msg)
}
