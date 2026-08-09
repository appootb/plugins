// Package kafka implements queue.Backend and a storage common dialect for Kafka.
//
// Blank-import registers the backend. Configure Kafka via storage address host
// (comma-separated brokers) and optional port. Call InitComponent when the
// storage component name is not COMPONENT.
//
// Note: only one queue.Backend implementor may be registered process-wide;
// do not blank-import both kafka and pulsar plugins.
package kafka

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/appootb/substratum/v2/configure"
	sctx "github.com/appootb/substratum/v2/context"
	"github.com/appootb/substratum/v2/errors"
	"github.com/appootb/substratum/v2/logger"
	"github.com/appootb/substratum/v2/queue"
	"github.com/appootb/substratum/v2/storage"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc/codes"
)

const (
	// PropertyRetry is the header key tracking requeue attempts.
	PropertyRetry = "PLUGIN.RETRY"
)

var (
	impl = &kafkaBackend{
		component: os.Getenv("COMPONENT"),
	}
)

func init() {
	queue.RegisterBackendImplementor(impl)
	storage.RegisterCommonDialectImplementor(configure.Kafka, impl)
}

// InitComponent overrides the storage component used to resolve Kafka brokers.
func InitComponent(component string) {
	impl.component = component
}

type kafkaBackend struct {
	component string
	producer  sync.Map
}

// parseBrokers builds host:port broker list from a storage address.
func parseBrokers(cfg configure.Address) []string {
	hosts := strings.Split(cfg.Host, ",")
	brokers := make([]string, 0, len(hosts))
	for _, host := range hosts {
		host = strings.TrimSpace(host)
		if host == "" {
			continue
		}
		if cfg.Port != "" {
			brokers = append(brokers, fmt.Sprintf("%s:%s", host, cfg.Port))
		} else {
			brokers = append(brokers, host)
		}
	}
	return brokers
}

func (s *kafkaBackend) Open(cfg configure.Address) (interface{}, error) {
	return &wrapper{brokers: parseBrokers(cfg)}, nil
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
		for {
			msg, rerr := consumer.ReadMessage(sctx.Context())
			if rerr == io.EOF {
				if cerr := consumer.Close(); cerr != nil {
					logger.Error("queue.kafka close consumer", logger.Content{"error": cerr.Error()})
				}
				consumer, rerr = s.newConsumer(topic, opts.Group, opts.InitOffset)
				if rerr != nil {
					logger.Error("queue.kafka recreate consumer", logger.Content{"error": rerr.Error()})
					time.Sleep(time.Second * 30)
				}
				continue
			} else if rerr != nil {
				// Do not process.Exit: keep retrying so a transient broker blip
				// does not take down the whole service.
				logger.Error("queue.kafka read", logger.Content{"error": rerr.Error()})
				time.Sleep(time.Second)
				continue
			}

			props := headersToProps(msg.Headers)
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
	if opts.Properties == nil {
		opts.Properties = make(map[string]string)
	}
	opts.Properties[PropertyRetry] = "0"
	msg := kafka.Message{
		Key:     []byte(opts.Key),
		Value:   content,
		Headers: propsToHeaders(opts.Properties),
	}
	return s.writeMessage(opts.Context, topic, msg)
}

func propsToHeaders(props map[string]string) []kafka.Header {
	if len(props) == 0 {
		return nil
	}
	headers := make([]kafka.Header, 0, len(props))
	for k, v := range props {
		headers = append(headers, kafka.Header{
			Key:   k,
			Value: []byte(v),
		})
	}
	return headers
}

func headersToProps(headers []kafka.Header) map[string]string {
	props := make(map[string]string, len(headers))
	for _, header := range headers {
		props[header.Key] = string(header.Value)
	}
	return props
}

func (s *kafkaBackend) getBrokers() ([]string, error) {
	client := storage.Implementor().Get(s.component).GetCommon(configure.Kafka)
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
