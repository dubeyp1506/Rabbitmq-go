package main

import (
	"fmt"
	"log"

	"github.com/rabbitmq/amqp091-go"
)

func consumer1(msgs <-chan amqp091.Delivery) {
	for msg := range msgs {
		fmt.Println("Received message from queue1:", string(msg.Body))
		msg.Ack(false)
	}
}

func consumer2(msgs <-chan amqp091.Delivery) {
	for msg := range msgs {
		fmt.Println("Received message from queue2:", string(msg.Body))
		msg.Ack(false)
	}
}

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
		"test_queue1",
		true,
		false,
		false,
		false,
		amqp091.Table{"x-queue-type": "quorum"},
	)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("Queue declared", q1.Name)

	q2, err := ch1.QueueDeclare(
		"test_queue2",
		true,
		false,
		false,
		false,
		amqp091.Table{"x-queue-type": "quorum"},
	)
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println("Queue declared", q2.Name)
	msgs1, err := ch1.Consume("test_queue1", "", false, true, false, false, nil)
	msgs2, err := ch1.Consume("test_queue2", "", false, true, false, false, nil)

	if err != nil {
		log.Fatalln(err)
	}

	go consumer1(msgs1)
	go consumer2(msgs2)

	// Keep the main function alive forever
	select {}
}
