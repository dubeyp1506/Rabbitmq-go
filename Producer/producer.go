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
	//Connect to rabbitmq
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672")
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close()
	fmt.Println("Connected to RabbitMQ")

	//Create a channel
	ch1, err := conn.Channel()
	if err != nil {
		log.Fatalln(err)
	}
	defer ch1.Close()
	fmt.Println("Channel opened")

	//Create an exchange
	err = ch1.ExchangeDeclare(
		"x-test",
		"direct",
		true,
		false,
		false,
		false,
		amqp091.Table{"x-queue-type": "quorum"},
	)
	if err != nil {
		log.Fatalln("Exchange declaration failed", err)
	}
	fmt.Println("Exchange declared")

	//Create a queue
	q1, err := ch1.QueueDeclare(
		"test_queue",
		true,
		false,
		false,
		false,
		amqp091.Table{"x-queue-type": "quorum"},
	)
	if err != nil {
		log.Fatalln("Queue declaration failed", err)
	}
	fmt.Println("Queue declared", q1)

	//Create a context for timeout
	ctx, cancle := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancle()

	//Bind the queue to the exchange
	ch1.QueueBind(q1.Name, "test", "x-test", false, nil)

	//Take the input from user
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Enter your message or type 'exit' to quit")
	for {

		fmt.Println("Enter your message")
		message, _ := reader.ReadString('\n')
		message = strings.TrimSpace(message) // Remove \n or \r\n
		lowerMsg := strings.ToLower(message)

		if lowerMsg == "exit" {
			break
		}

		//Publish a message
		err = ch1.PublishWithContext(ctx, "x-test", "test", false, false, amqp091.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Println("Message sent", message)
	}

}
