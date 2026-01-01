package main

import (
	"fmt"
	"log"

	"github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672")
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close()
	fmt.Println("Connected to RabbitMQ")
	ch1, err := conn.Channel()
	if err != nil {
		log.Fatalln(err)
	}
	defer ch1.Close()
	q1, err := ch1.QueueDeclare(
		"test_queue",
		true,
		false,
		false,
		false,
		amqp091.Table{"x-queue-type": "quorum"},
	)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("Queue declared", q1)
	msgs, err := ch1.Consume("test_queue", "", false, true, false, false, nil)

	if err != nil {
		log.Fatalln(err)
	}
	for msg := range msgs {
		fmt.Println("Received message", string(msg.Body))
		msg.Nack(false, true)
	}
}
