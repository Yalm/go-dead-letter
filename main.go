package main

import (
	"log"
	"os"
	"strconv"
	"time"

	"github.com/Yalm/go-dead-letter/config"
	"github.com/Yalm/go-dead-letter/utils"
	"github.com/mitchellh/mapstructure"
	"github.com/streadway/amqp"
)

type DeathHeader struct {
	Count int `json:"count"`
}

type Headers struct {
	Xdeath []DeathHeader `json:"x-death"`
}

var output = &Headers{}
var cfg = &mapstructure.DecoderConfig{
	Metadata: nil,
	Result:   &output,
	TagName:  "json",
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func decodeHeaders(headers amqp.Table) (*Headers, error) {
	decoder, error := mapstructure.NewDecoder(cfg)
	decoder.Decode(headers)
	return output, error
}

func main() {
	conf, err := config.ReadConf(os.Getenv("CONFIG_FILE"))
	failOnError(err, "Failed to load config file")

	conn, err := amqp.Dial(os.Getenv("RABBITMQ_URL"))
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()
	notifyClose := conn.NotifyClose(make(chan *amqp.Error))

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.Confirm(false)
	failOnError(err, "Failed to set confirm channel")

	amqPrefetch := os.Getenv("PREFETCH_COUNT")
	deadLetterQueue := utils.Getenv("DEAD_LETTER_QUEUE", "dead_letter")
	deadLetterExchange := utils.Getenv("DEAD_LETTER_EXCHANGE", "dead-letter-exchange")
	consumerName := utils.Getenv("CONSUMER_NAME", "dead-letter-broker")

	err = ch.ExchangeDeclare(
		deadLetterExchange, // name
		"fanout",           // type
		true,               // durable
		false,              // auto-deleted
		true,               // internal
		false,              // no-wait
		nil,                // arguments
	)
	failOnError(err, "Failed to declare exchange")

	_, err = ch.QueueDeclare(
		deadLetterQueue, // name
		true,            // durable
		false,           // auto-deleted
		false,           // exclusive
		false,           // no-wait
		nil,             // arguments
	)
	failOnError(err, "Failed to declare queue")

	err = ch.QueueBind(
		deadLetterQueue,    // name
		"",                 // key
		deadLetterExchange, // auto-deleted
		false,              // no-wait
		nil,                // arguments
	)
	failOnError(err, "Failed to bind queue")

	if len(amqPrefetch) > 0 {
		prefetchCount, err := strconv.Atoi(amqPrefetch)
		failOnError(err, "Failed to convert PREFETCH_COUNT to interger")

		err = ch.Qos(
			prefetchCount, // prefetch count
			0,             // prefetch size
			false,         // global
		)
		failOnError(err, "Failed to set QoS")
	}

	msgs, err := ch.Consume(
		deadLetterQueue, // queue
		consumerName,    // consumer
		false,           // auto-ack
		false,           // exclusive
		false,           // no-local
		false,           // no-wait
		nil,             // args
	)
	failOnError(err, "Failed to register a consumer")

	log.Printf("Application %s successfully started", consumerName)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			headers, err := decodeHeaders(d.Headers)
			failOnError(err, "Failed to decode headers")
			redeliveryPolicy := conf.RedeliveryPolicyEntries.RedeliveryPolicy[d.RoutingKey]

			if redeliveryPolicy == (config.RedeliveryPolicy{}) {
				log.Printf("Queue %s not configured, assigning default values", d.RoutingKey)
				redeliveryPolicy = conf.DefaultEntry
			}

			count := 0

			if len(headers.Xdeath) > 0 {
				count = headers.Xdeath[0].Count
			} else {
				log.Printf("Header x-death not configured, rejecting queue %s", d.RoutingKey)
				d.Ack(false)
				continue
			}

			if count > redeliveryPolicy.MaximumRedeliveries {
				log.Println("Maximum retry limit reached")
				d.Ack(false)
				continue
			}

			time.Sleep(time.Duration(redeliveryPolicy.RedeliveryDelay) * time.Millisecond)

			err = ch.Publish("", d.RoutingKey, false, false, amqp.Publishing{
				Body:    d.Body,
				Headers: d.Headers,
			})
			failOnError(err, "Failed to publish a message")
			log.Printf("Done")
			d.Ack(false)
		}
	}()

	<-notifyClose
}
