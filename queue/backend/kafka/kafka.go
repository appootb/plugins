package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/appootb/substratum/v2/configure"
	"github.com/appootb/substratum/v2/queue"
	"github.com/segmentio/kafka-go"
)

var (
	Backend = &kafkaBackend{}
)

func init() {
	queue.RegisterBackendImplementor(Backend)
}

func Init(configs []configure.Address) {
	addrs := make([]string, 0, len(configs))
	for _, cfg := range configs {
		addrs = append(addrs, fmt.Sprintf("%s:%s", cfg.Host, cfg.Port))
	}
	Backend.addrs = addrs
}

type kafkaBackend struct {
	addrs    []string
	producer sync.Map
}

// Type returns backend type.
func (s *kafkaBackend) Type() string {
	return string(configure.Kafka)
}

// Ping connect the backend server if not connected.
// Will be called before every Read/Write operation.
func (s *kafkaBackend) Ping() error {
	return nil
}

// Read subscribes the message of the specified topic.
func (s *kafkaBackend) Read(topic string, ch chan<- queue.MessageWrapper, opts *queue.SubscribeOptions) error {
	consumer := s.newConsumer(topic, opts.Group, opts.InitOffset)

	var (
		err error
		msg kafka.Message
	)

	go func() {
		for {
			msg, err = consumer.ReadMessage(opts.Context)
			if errors.Is(err, io.EOF) {
				err = consumer.Close()
				if err != nil {
					log.Println("closing kafka consumer err:", err.Error())
				}
				consumer = s.newConsumer(topic, opts.Group, opts.InitOffset)
				continue
			} else if err != nil {
				log.Fatal("error from kafka consumer:", err.Error())
			}

			//
			props := make(map[string]string, len(msg.Headers))
			for _, header := range msg.Headers {
				props[header.Key] = string(header.Value)
			}
			ch <- &message{
				svr:       s,
				ctx:       opts.Context,
				topic:     topic,
				group:     opts.Group,
				key:       string(msg.Key),
				content:   msg.Value,
				props:     props,
				timestamp: msg.Time,
				headers:   msg.Headers,
			}
		}
	}()

	return nil
}

// Write publishes content data to the specified queue.
func (s *kafkaBackend) Write(topic string, content []byte, opts *queue.PublishOptions) error {
	msg := kafka.Message{
		Key:   []byte(opts.Key),
		Value: content,
		Headers: []kafka.Header{
			{
				Key:   "retry",
				Value: []byte("0"),
			},
		},
	}
	for key, val := range opts.Properties {
		msg.Headers = append(msg.Headers, kafka.Header{
			Key:   key,
			Value: []byte(val),
		})
	}
	return s.writeMessage(opts.Context, topic, msg)
}

func (s *kafkaBackend) newConsumer(topic, group string, initOffset queue.ConsumeOffset) *kafka.Reader {
	startOffset := kafka.LastOffset
	if initOffset == queue.ConsumeFromEarliest {
		startOffset = kafka.FirstOffset
	}
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
		StartOffset:            startOffset,
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
