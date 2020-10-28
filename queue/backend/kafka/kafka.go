package kafka

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/appootb/substratum/queue"
	"github.com/appootb/substratum/util/snowflake"
)

var (
	Backend = &kafka{}
)

func init() {
	queue.RegisterBackendImplementor(Backend)
}

type ConfigOption func(*sarama.Config)

func Init(addrs []string, opts ...ConfigOption) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	for _, o := range opts {
		o(config)
	}
	Backend.addrs = addrs
	Backend.config = config
}

type kafka struct {
	addrs  []string
	config *sarama.Config

	producer sync.Map
}

// Backend type.
func (s *kafka) Type() string {
	return "kafka"
}

// Ping connect the backend server if not connected.
// Will be called before every Read/Write operation.
func (s *kafka) Ping() error {
	return nil
}

// Return the max delay duration supported by the backend.
// A negative value means no limitation.
// A zero value means delay operation is not supported.
func (s *kafka) MaxDelay() time.Duration {
	// Delay is unsupported
	return 0
}

// Return all queue names in backend storage.
func (s *kafka) GetQueues() ([]string, error) {
	// Unsupported
	return []string{}, nil
}

// Return all queue/topics in backend storage.
func (s *kafka) GetTopics() (map[string][]string, error) {
	// Unsupported
	return map[string][]string{}, nil
}

// Return all topic length of specified queue in backend storage.
func (s *kafka) GetQueueLength(_ string) (map[string]int64, error) {
	// Unsupported
	return map[string]int64{}, nil
}

// Return the specified queue/topic length in backend storage.
func (s *kafka) GetTopicLength(_, _ string) (int64, error) {
	// Unsupported
	return 0, nil
}

// Read subscribes the message of the specified queue and topic.
func (s *kafka) Read(ctx context.Context, topic, group string, ch chan<- queue.MessageWrapper) error {
	consumer, err := sarama.NewConsumerGroup(s.addrs, group, s.config)
	if err != nil {
		return err
	}
	//
	h := &handler{
		svr:   s,
		topic: topic,
		group: group,
		ch:    ch,
	}
	go func() {
		err := consumer.Consume(ctx, []string{topic}, h)
		if err != nil {
			log.Panicf("error from kafka consumer: %v", err)
		}
		// Exiting...
		if ctx.Err() != nil {
			return
		}
	}()
	return nil
}

// Write publishes content data to the specified queue.
func (s *kafka) Write(_ context.Context, topic string, _ time.Duration, content []byte) error {
	keyID, err := snowflake.NextID()
	if err != nil {
		return err
	}
	return s.writeMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(fmt.Sprintf("%s-%d", topic, keyID)),
		Value: sarama.ByteEncoder(content),
		Headers: []sarama.RecordHeader{
			{
				Key:   []byte("retry"),
				Value: []byte("0"),
			},
		},
	})
}

func (s *kafka) writeMessage(msg *sarama.ProducerMessage) error {
	var (
		err      error
		producer sarama.SyncProducer
	)
	if p, ok := s.producer.Load(msg.Topic); ok {
		producer = p.(sarama.SyncProducer)
	} else {
		producer, err = sarama.NewSyncProducer(s.addrs, s.config)
		if err != nil {
			return err
		}
		if p, loaded := s.producer.LoadOrStore(msg.Topic, producer); loaded {
			_ = producer.Close()
			producer = p.(sarama.SyncProducer)
		}
	}
	_, _, err = producer.SendMessage(msg)
	return err
}
