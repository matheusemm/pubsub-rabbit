package main

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	connString := "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(connString)
	if err != nil {
		log.Fatalf("failed to connect to RabbitMQ server: %s", err)
	}
	defer conn.Close()

	log.Println("connected successfully to RabitMQ server")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	<-ctx.Done()
	fmt.Println("shutting down")
}
