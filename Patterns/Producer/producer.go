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
	// connect to rabbitmq
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672/")

	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// create channel
	ch, err := conn.Channel()

	if err != nil {
		log.Fatal(err)
	}
	defer ch.Close()

	// declare queue
	queue, err := ch.QueueDeclare("task", true, false, false, false, nil)

	if err != nil {
		log.Fatal(err)
	}

	// create reader
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Type your taks or enter 'exit' to exit from the program")

	for {
		// read input from user
		task, _ := reader.ReadString('\n')
		task = strings.TrimSpace(task)
		if task == "exit" {
			break
		}
		// create context for timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		// publish message
		err = ch.PublishWithContext(ctx, "", queue.Name, false, false, amqp091.Publishing{
			ContentType: "text/plain",
			Body:        []byte(task),
		})
		if err != nil {
			log.Fatal("Publish Failed", err)
		}
		cancel()

	}

}
