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
		// "x-test",
		"x-fanout",
		// "direct",
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalln("Exchange declaration failed", err)
	}
	fmt.Println("Exchange declared")

	//Create a queue
	q1, err := ch1.QueueDeclare(
		"test_queue1",
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

	q2, err := ch1.QueueDeclare(
		"test_queue2",
		true,
		false,
		false,
		false,
		amqp091.Table{"x-queue-type": "quorum"},
	)
	if err != nil {
		log.Fatalln("Queue declaration failed", err)
	}
	fmt.Println("Queue declared", q2)

	//Bind the queue to the exchange
	//in the case of fanout exchange it igonres the routing key
	//it is here because of easyness of code practice
	ch1.QueueBind(q1.Name, "test", "x-fanout", false, nil)
	ch1.QueueBind(q2.Name, "test", "x-fanout", false, nil)

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

		//Create a context for timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		//Publish a message
		err = ch1.PublishWithContext(ctx, "x-fanout", "test", false, false, amqp091.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
		cancel()
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Println("Message sent", message)
	}

}
