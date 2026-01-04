package main

import (
	"fmt"
	"log"

	"github.com/rabbitmq/amqp091-go"
)

func subscriber1(msgs <-chan amqp091.Delivery) {
	for msg := range msgs {
		fmt.Println("Received message in queue1", string(msg.Body))
	}
}

func subscriber2(msgs <-chan amqp091.Delivery) {
	for msg := range msgs {
		fmt.Println("Received message in queue2", string(msg.Body))
	}
}

func main() {
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer ch.Close()

	err = ch.ExchangeDeclare("x-fanout", "fanout", true, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	q1, err := ch.QueueDeclare(
		"test_queue1", // name
		false,         // durable
		false,         // delete when unused
		true,          // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		log.Fatal(err)
	}

	q2, err := ch.QueueDeclare(
		"test_queue2", // name
		false,         // durable
		false,         // delete when unused
		true,          // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		log.Fatal(err)
	}

	err = ch.QueueBind(
		q1.Name,    // queue name
		"",         // routing key
		"x-fanout", // exchange
		false,
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}
	err = ch.QueueBind(
		q2.Name,    // queue name
		"",         // routing key
		"x-fanout", // exchange
		false,
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	msgs1, err := ch.Consume(
		q1.Name, // queue
		"",      // consumer
		true,    // auto-ack
		false,   // exclusive
		false,   // no-local
		false,   // no-wait
		nil,     // args
	)
	if err != nil {
		log.Fatal(err)
	}

	msgs2, err := ch.Consume(
		q2.Name, // queue
		"",      // consumer
		true,    // auto-ack
		false,   // exclusive
		false,   // no-local
		false,   // no-wait
		nil,     // args
	)
	if err != nil {
		log.Fatal(err)
	}

	go subscriber1(msgs1)
	go subscriber2(msgs2)

	fmt.Println(" [*] Waiting for messages. To exit press CTRL+C")
	select {}
}
