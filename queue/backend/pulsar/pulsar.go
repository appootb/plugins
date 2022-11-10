package pulsar

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/appootb/substratum/v2/configure"
	sctx "github.com/appootb/substratum/v2/context"
	"github.com/appootb/substratum/v2/errors"
	"github.com/appootb/substratum/v2/queue"
	"github.com/appootb/substratum/v2/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

const (
	PropertyDelay = "PLUGIN.DELAY"
)

var (
	impl = &pulsarBackend{
		component: os.Getenv("COMPONENT"),
	}
)

func init() {
	// Queue
	queue.RegisterBackendImplementor(impl)
	// Storage
	storage.RegisterCommonDialectImplementor(configure.Pulsar, impl)
}

func InitComponent(component string) {
	impl.component = component
}

type pulsarBackend struct {
	component string
	producer  sync.Map
}

func (s *pulsarBackend) Open(cfg configure.Address) (interface{}, error) {
	option := pulsar.ClientOptions{
		ConnectionTimeout:       time.Second * 5,
		OperationTimeout:        time.Second * 30,
		MaxConnectionsPerBroker: 1,
		MetricsCardinality:      pulsar.MetricsCardinalityNamespace,
		Logger:                  &logWrapper{},
	}
	//
	schema := "http"
	if strings.ToLower(cfg.Params["ssl"]) == "true" {
		schema = "https"
	}
	if cfg.Port != "" {
		option.URL = fmt.Sprintf("%s://%s:%s", schema, cfg.Host, cfg.Port)
	} else {
		option.URL = fmt.Sprintf("%s://%s", schema, cfg.Host)
	}
	if cfg.Password != "" {
		option.Authentication = pulsar.NewAuthenticationToken(cfg.Password)
	}
	//
	client, err := pulsar.NewClient(option)
	if err != nil {
		return nil, err
	}
	return &wrapper{
		Client:    client,
		cluster:   cfg.Params["cluster"],
		namespace: cfg.NameSpace,
	}, nil
}

// Type returns backend type.
func (s *pulsarBackend) Type() string {
	return string(configure.Pulsar)
}

// Ping connect the backend server if not connected.
// Will be called before every Read/Write operation.
func (s *pulsarBackend) Ping() error {
	return nil
}

// Read subscribes the message of the specified topic.
func (s *pulsarBackend) Read(topic string, ch chan<- queue.MessageWrapper, opts *queue.SubscribeOptions) error {
	consumer, err := s.newConsumer(topic, opts.Group, opts.InitOffset)
	if err != nil {
		return err
	}

	go func() {
		for {
			msg, cErr := consumer.Receive(sctx.Context())
			if cErr != nil {
				log.Fatal(cErr)
			}

			delay, _ := strconv.ParseInt(msg.Properties()[PropertyDelay], 10, 64)
			ch <- &message{
				svr:       consumer,
				raw:       msg,
				ctx:       sctx.Context(),
				topic:     topic,
				group:     opts.Group,
				key:       msg.Key(),
				content:   msg.Payload(),
				props:     msg.Properties(),
				timestamp: msg.PublishTime().In(time.Local),
				delay:     time.Duration(delay),
			}
		}
	}()

	return nil
}

// Write publishes content data to the specified queue.
func (s *pulsarBackend) Write(topic string, content []byte, opts *queue.PublishOptions) error {
	opts.Properties[PropertyDelay] = strconv.FormatInt(opts.Delay.Nanoseconds(), 10)
	return s.writeMessage(opts.Context, topic, &pulsar.ProducerMessage{
		Payload:      content,
		Key:          opts.Key,
		Properties:   opts.Properties,
		SequenceID:   proto.Int64(int64(opts.Sequence)),
		OrderingKey:  strconv.FormatUint(opts.Sequence, 10),
		DeliverAfter: opts.Delay,
	})
}

func (s *pulsarBackend) getClient() (*wrapper, error) {
	client := storage.Implementor().Get(s.component).GetCommon(configure.Pulsar)
	if client == nil {
		return nil, errors.New(codes.FailedPrecondition, "pulsar backend uninitialized")
	}
	return client.(*wrapper), nil
}

func (s *pulsarBackend) newConsumer(topic, group string, initOffset queue.ConsumeOffset) (pulsar.Consumer, error) {
	initPosition := pulsar.SubscriptionPositionLatest
	if initOffset == queue.ConsumeFromEarliest {
		initPosition = pulsar.SubscriptionPositionEarliest
	}
	//
	client, err := s.getClient()
	if err != nil {
		return nil, err
	}
	topic = fmt.Sprintf("persistent://%s/%s/%s", client.cluster, client.namespace, topic)
	return client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            group,
		Type:                        pulsar.Shared,
		SubscriptionInitialPosition: initPosition,
	})
}

func (s *pulsarBackend) writeMessage(ctx context.Context, topic string, msg *pulsar.ProducerMessage) error {
	var (
		producer pulsar.Producer
	)
	//
	client, err := s.getClient()
	if err != nil {
		return err
	}
	topic = fmt.Sprintf("persistent://%s/%s/%s", client.cluster, client.namespace, topic)
	if p, ok := s.producer.Load(topic); ok {
		producer = p.(pulsar.Producer)
	} else {
		producer, err = client.CreateProducer(pulsar.ProducerOptions{
			Topic:                   topic,
			SendTimeout:             time.Second * 30,
			DisableBlockIfQueueFull: true,
			DisableBatching:         true,
		})
		if err != nil {
			return err
		}
		if pp, loaded := s.producer.LoadOrStore(topic, producer); loaded {
			producer.Close()
			producer = pp.(pulsar.Producer)
		}
	}
	_, err = producer.Send(ctx, msg)
	return err
}
