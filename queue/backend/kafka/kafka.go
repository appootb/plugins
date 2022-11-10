package kafka

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/appootb/substratum/v2/configure"
	sctx "github.com/appootb/substratum/v2/context"
	"github.com/appootb/substratum/v2/errors"
	"github.com/appootb/substratum/v2/queue"
	"github.com/appootb/substratum/v2/storage"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc/codes"
)

const (
	PropertyRetry = "PLUGIN.RETRY"
)

var (
	impl = &kafkaBackend{
		component: os.Getenv("COMPONENT"),
	}
)

func init() {
	// Queue
	queue.RegisterBackendImplementor(impl)
	// Storage
	storage.RegisterCommonDialectImplementor(configure.Kafka, impl)
}

func InitComponent(component string) {
	impl.component = component
}

type kafkaBackend struct {
	component string
	producer  sync.Map
}

func (s *kafkaBackend) Open(cfg configure.Address) (interface{}, error) {
	hosts := strings.Split(cfg.Host, ",")
	brokers := make([]string, 0, len(hosts))
	for _, host := range hosts {
		if cfg.Port != "" {
			brokers = append(brokers, fmt.Sprintf("%s:%s", host, cfg.Port))
		} else {
			brokers = append(brokers, host)
		}
	}
	return &wrapper{
		brokers: brokers,
	}, nil
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
	consumer, err := s.newConsumer(topic, opts.Group, opts.InitOffset)
	if err != nil {
		return err
	}

	go func() {
		var (
			msg kafka.Message
		)

		for {
			msg, err = consumer.ReadMessage(sctx.Context())
			if err == io.EOF {
				err = consumer.Close()
				if err != nil {
					log.Println("closing kafka consumer err:", err.Error())
				}
				consumer, err = s.newConsumer(topic, opts.Group, opts.InitOffset)
				if err != nil {
					time.Sleep(time.Second * 30)
				}
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
				ctx:       sctx.Context(),
				topic:     topic,
				group:     opts.Group,
				key:       string(msg.Key),
				content:   msg.Value,
				props:     props,
				timestamp: msg.Time.In(time.Local),
			}
		}
	}()

	return nil
}

// Write publishes content data to the specified queue.
func (s *kafkaBackend) Write(topic string, content []byte, opts *queue.PublishOptions) error {
	opts.Properties[PropertyRetry] = "0"
	//
	msg := kafka.Message{
		Key:     []byte(opts.Key),
		Value:   content,
		Headers: s.propsToHeaders(opts.Properties),
	}
	return s.writeMessage(opts.Context, topic, msg)
}

func (s *kafkaBackend) propsToHeaders(props map[string]string) []kafka.Header {
	headers := make([]kafka.Header, 0, len(props))
	for k, v := range props {
		headers = append(headers, kafka.Header{
			Key:   k,
			Value: []byte(v),
		})
	}
	return headers
}

func (s *kafkaBackend) getBrokers() ([]string, error) {
	client := storage.Implementor().Get(s.component).GetCommon(configure.Pulsar)
	if client == nil {
		return nil, errors.New(codes.FailedPrecondition, "kafka backend uninitialized")
	}
	return client.(*wrapper).brokers, nil
}

func (s *kafkaBackend) newConsumer(topic, group string, initOffset queue.ConsumeOffset) (*kafka.Reader, error) {
	startOffset := kafka.LastOffset
	if initOffset == queue.ConsumeFromEarliest {
		startOffset = kafka.FirstOffset
	}
	//
	brokers, err := s.getBrokers()
	if err != nil {
		return nil, err
	}
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:                brokers,
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
	}), nil
}

func (s *kafkaBackend) writeMessage(ctx context.Context, topic string, msg kafka.Message) error {
	var (
		producer *kafka.Writer
	)
	if p, ok := s.producer.Load(topic); ok {
		producer = p.(*kafka.Writer)
	} else {
		brokers, err := s.getBrokers()
		if err != nil {
			return err
		}
		producer = &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
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
