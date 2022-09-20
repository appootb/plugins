package pulsar

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/appootb/substratum/v2/configure"
	"github.com/appootb/substratum/v2/queue"
	"google.golang.org/protobuf/proto"
)

const (
	PropertyDelay = "PLUGIN.DELAY"
)

var (
	impl = &pulsarBackend{}
)

func init() {
	queue.RegisterBackendImplementor(impl)
}

type pulsarBackend struct {
	client   pulsar.Client
	producer sync.Map
}

// Init queue backend instance.
func (s *pulsarBackend) Init(cfg configure.Address) (err error) {
	option := pulsar.ClientOptions{
		ConnectionTimeout:       time.Second * 5,
		OperationTimeout:        time.Second * 30,
		Authentication:          pulsar.NewAuthenticationToken(cfg.Password),
		MaxConnectionsPerBroker: 1,
		MetricsCardinality:      pulsar.MetricsCardinalityNamespace,
	}
	if cfg.Port != "" {
		option.URL = fmt.Sprintf("%s://%s:%s", cfg.Schema, cfg.Host, cfg.Port)
	} else {
		option.URL = fmt.Sprintf("%s://%s", cfg.Schema, cfg.Host)
	}
	//
	impl.client, err = pulsar.NewClient(option)
	return
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
			msg, cErr := consumer.Receive(opts.Context)
			if cErr != nil {
				log.Fatal(cErr)
			}

			delay, _ := strconv.ParseInt(msg.Properties()[PropertyDelay], 10, 64)
			ch <- &message{
				svr:       consumer,
				raw:       msg,
				ctx:       opts.Context,
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

func (s *pulsarBackend) newConsumer(topic, group string, initOffset queue.ConsumeOffset) (pulsar.Consumer, error) {
	initPosition := pulsar.SubscriptionPositionLatest
	if initOffset == queue.ConsumeFromEarliest {
		initPosition = pulsar.SubscriptionPositionEarliest
	}
	return s.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            group,
		Type:                        pulsar.Shared,
		SubscriptionInitialPosition: initPosition,
	})
}

func (s *pulsarBackend) writeMessage(ctx context.Context, topic string, msg *pulsar.ProducerMessage) (err error) {
	var (
		producer pulsar.Producer
	)
	if p, ok := s.producer.Load(topic); ok {
		producer = p.(pulsar.Producer)
	} else {
		producer, err = s.client.CreateProducer(pulsar.ProducerOptions{
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
	return
}
