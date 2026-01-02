package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	ch1, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer ch1.Close()

	err = ch1.ExchangeDeclare("x-topic", "topic", true, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

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

	err = ch1.QueueBind(q1.Name, "*.error", "x-topic", false, nil)
	if err != nil {
		log.Fatal("Queue bind failed", err)
	}
	err = ch1.QueueBind(q2.Name, "kern.#", "x-topic", false, nil)
	if err != nil {
		log.Fatal("Queue bind failed", err)
	}

	fmt.Println("Enter 'exit' in the Value to quit")

	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Enter the Message:- ")
		message, _ := reader.ReadString('\n')
		message = strings.TrimSpace(message)
		if message == "exit" {
			break
		}
		fmt.Print("Enter the Routing Key:- ")
		routingKey, _ := reader.ReadString('\n')
		routingKey = strings.TrimSpace(routingKey)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		err = ch1.PublishWithContext(ctx, "x-topic", routingKey, false, false, amqp091.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
		if err != nil {
			log.Fatal(err)
		}
		cancel()
	}

}
