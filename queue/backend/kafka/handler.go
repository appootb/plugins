package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/appootb/substratum/queue"
)

type handler struct {
	svr *kafka

	topic string
	group string

	ch chan<- queue.MessageWrapper
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (c *handler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
// but before the offsets are committed for the very last time.
func (c *handler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (c *handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for msg := range claim.Messages() {
		c.ch <- &message{
			svr:       c.svr,
			topic:     msg.Topic,
			group:     c.group,
			key:       string(msg.Key),
			content:   msg.Value,
			timestamp: msg.Timestamp,
			headers:   msg.Headers,
		}
		session.MarkMessage(msg, "")
	}

	return nil
}
