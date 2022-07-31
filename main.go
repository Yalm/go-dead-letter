package main

import (
	"log"
	"os"
	"strconv"

	"github.com/Yalm/go-dead-letter/config"
	"github.com/Yalm/go-dead-letter/consumers"
	"github.com/Yalm/go-dead-letter/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
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
		amqp.Table{
			"x-queue-mode": "lazy",
		}, // arguments
	)
	failOnError(err, "Failed to declare queue")

	err = ch.QueueBind(
		deadLetterQueue,              // name
		os.Getenv("DEAD_LETTER_KEY"), // key
		deadLetterExchange,           // exchange
		false,                        // no-wait
		nil,                          // arguments
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

	rejectedConsumer := consumers.NewRejectedConsumer(conf)

	for {
		select {
		case message := <-msgs:
			go rejectedConsumer.HandleRejectedMessage(&message, ch)
		case err := <-notifyClose:
			log.Fatalf(err.Error())
		}
	}
}
