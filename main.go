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
	Count int64 `json:"count"`
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
	conf, err := config.ReadConf("./conf.yaml")
	failOnError(err, "Failed to load config file")

	conn, err := amqp.Dial(os.Getenv("AMQ_URL"))
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()
	notifyClose := conn.NotifyClose(make(chan *amqp.Error))

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	amqPrefetch := os.Getenv("AMQ_PREFETCH")

	if len(amqPrefetch) > 0 {
		prefecthCount, err := strconv.Atoi(amqPrefetch)
		failOnError(err, "Failed to convert AMQ_PREFETCH to interger")

		err = ch.Qos(
			prefecthCount, // prefetch count
			0,             // prefetch size
			false,         // global
		)
		failOnError(err, "Failed to set QoS")
	}

	msgs, err := ch.Consume(
		utils.Getenv("AMQ_DEAD_LETTER_QUEUE", "dead_letter"), // queue
		utils.Getenv("AMQ_CONSUMER_NAME", ""),                // consumer
		false,                                                // auto-ack
		false,                                                // exclusive
		false,                                                // no-local
		false,                                                // no-wait
		nil,                                                  // args
	)
	failOnError(err, "Failed to register a consumer")

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			headers, err := decodeHeaders(d.Headers)
			failOnError(err, "Failed to decode headers")
			redeliveryPolicy := conf.RedeliveryPolicyEntries.RedeliveryPolicy[d.RoutingKey]

			if redeliveryPolicy == (config.RedeliveryPolicy{}) {
				log.Printf("Queue %s not configured assign default values", d.RoutingKey)
				redeliveryPolicy = conf.DefaultEntry
			}

			var count int64 = 0
			if len(headers.Xdeath) > 0 {
				count = headers.Xdeath[0].Count
			} else {
				log.Printf("x-death header not configured, rejecting queue %s", d.RoutingKey)
				d.Ack(false)
				continue
			}

			if count > redeliveryPolicy.MaximumRedeliveries {
				log.Println("Maximum retry limit reached")
				d.Ack(false)
				continue
			}

			time.Sleep(time.Duration(redeliveryPolicy.RedeliveryDelay) * time.Millisecond)
			err = ch.Publish(d.Exchange, d.RoutingKey, false, false, amqp.Publishing{
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
