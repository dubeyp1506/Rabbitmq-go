package main

import (
	"fmt"
	"log"

	"github.com/rabbitmq/amqp091-go"
)

func queue1(msgs <-chan amqp091.Delivery) {
	for msg := range msgs {
		fmt.Println("Received message in queue1", string(msg.Body))
	}
}

func queue2(msgs <-chan amqp091.Delivery) {
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
	fmt.Println("Connected to RabbitMQ")

	ch1, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer ch1.Close()
	fmt.Println("Channel opened")

	q1, err := ch1.QueueDeclare(
		"test_queue1",
		true,
		false,
		false,
		false,
		amqp091.Table{"x-queue-type": "quorum"},
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Queue declared", q1)

	q2, err := ch1.QueueDeclare(
		"test_queue2",
		true,
		false,
		false,
		false,
		amqp091.Table{"x-queue-type": "quorum"},
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Queue declared", q2)

	msgs1, err := ch1.Consume("test_queue1", "", true, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}
	msgs2, err := ch1.Consume("test_queue2", "", true, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	go queue1(msgs1)
	go queue2(msgs2)

	select {}
}
