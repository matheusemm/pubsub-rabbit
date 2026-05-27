package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func ConnectToRabbitMQ() (*amqp.Connection, *amqp.Channel) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("failed to connect to RabbitMQ server: %s", err)
	}

	log.Println("connected successfully to RabitMQ server")

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("failed to open a channel to RabbitMQ server: %s", err)
	}

	return conn, ch
}

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	body, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("failed to encode value as JSON: %w", err)
	}

	if err := ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		}); err != nil {
		return fmt.Errorf("failed to publish a message: %w", err)
	}
	return nil
}

type SimpleQueueType string

const (
	DurableQueueType   SimpleQueueType = "durable"
	TransientQueueType SimpleQueueType = "transient"
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to open a channel to RabbitMQ server: %w", err)
	}

	queue, err := ch.QueueDeclare(
		queueName,
		queueType == DurableQueueType,
		queueType == TransientQueueType,
		queueType == TransientQueueType,
		false,
		nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to create a queue: %w", err)
	}

	if err := ch.QueueBind(queueName, key, exchange, false, nil); err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to bind queue to exchange: %w", err)
	}

	return ch, queue, nil
}
