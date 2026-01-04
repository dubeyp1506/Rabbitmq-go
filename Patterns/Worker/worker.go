package main

import (
	"fmt"
	"log"

	"github.com/rabbitmq/amqp091-go"
)

func processTask(task []byte) error {
	return nil
}

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

	//consume messages from channel
	tasks, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	// process messages
	for task := range tasks {
		fmt.Println("Task:", string(task.Body))

		err = processTask(task.Body)
		if err != nil {
			task.Nack(false, true)
			log.Fatal(err)
			continue
		}
		task.Ack(false)
	}
}
