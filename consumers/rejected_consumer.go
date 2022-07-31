package consumers

import (
	"log"
	"time"

	"github.com/Yalm/go-dead-letter/config"
	"github.com/Yalm/go-dead-letter/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	RETRY_ATTEMPT  = "x-retry"
	DELAY_INTERVAL = "x-delay"
)

type rejectedConsumer struct {
	conf *config.RedeliveryConfig
}

func NewRejectedConsumer(conf *config.RedeliveryConfig) *rejectedConsumer {
	return &rejectedConsumer{conf}
}

func (ctx *rejectedConsumer) HandleRejectedMessage(message *amqp.Delivery, channel *amqp.Channel) {
	headers, err := utils.DecodeHeaders(message.Headers)

	if err != nil {
		log.Printf("Failed to decode headers: %s", err)
		return
	}

	if len(message.Headers) < 1 {
		log.Printf("Header x-death not configured, rejecting queue %s", message.RoutingKey)
		message.Reject(false)
		return
	}

	causeOfDeath := headers.XDeath[0]
	exchange := causeOfDeath.Exchange
	queue := causeOfDeath.Queue
	routingKey := causeOfDeath.RoutingKeys[0]

	log.Printf("Received a message: %s", message.Body)
	log.Printf("Failed to decode headers: %s", err)
	redeliveryPolicy := ctx.conf.RedeliveryPolicyEntries.RedeliveryPolicy[queue]

	if redeliveryPolicy == (config.RedeliveryPolicy{}) {
		log.Printf("Queue %s not configured, assigning default values", queue)
		redeliveryPolicy = ctx.conf.DefaultEntry
	}

	count := headers.XRetry + 1

	if count > redeliveryPolicy.MaximumRedeliveries {
		log.Printf("Maximum retry limit reached")
		message.Reject(false)
		return
	}

	message.Headers[RETRY_ATTEMPT] = count

	time.Sleep(time.Duration(redeliveryPolicy.RedeliveryDelay) * time.Millisecond)

	err = channel.Publish(exchange, routingKey, false, false, amqp.Publishing{
		Body:         message.Body,
		ContentType:  message.ContentType,
		DeliveryMode: message.DeliveryMode,
		Headers:      message.Headers,
	})
	log.Printf("Failed to publish a message: %s", err)
	log.Printf("Done")
	message.Ack(false)
}
