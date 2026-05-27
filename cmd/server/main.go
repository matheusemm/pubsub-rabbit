package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func main() {
	conn, ch := pubsub.ConnectToRabbitMQ()
	defer conn.Close()
	defer ch.Close()

	gamelogic.PrintServerHelp()

	if err := pubsub.PublishJSON(
		ch,
		routing.ExchangePerilDirect,
		routing.PauseKey,
		routing.PlayingState{
			IsPaused: true,
		},
	); err != nil {
		log.Printf("could not publish time: %v", err)
	}

	topicCh, _, err := pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilTopic,
		"game_logs",
		"game_logs.*",
		pubsub.DurableQueueType,
	)
	if err != nil {
		log.Fatalf("%s\n", err)
	}
	defer topicCh.Close()

gameloop:
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "pause":
			log.Println("Pausing the game")
			if err := pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true}); err != nil {
				fmt.Printf("failed to pause the game: %v\n", err)
			}
		case "resume":
			log.Println("Resuming the game")
			if err := pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false}); err != nil {
				fmt.Printf("failed to resume the game: %v\n", err)
			}
		case "quit":
			log.Println("Exiting")
			break gameloop
		default:
			log.Printf("unknown command: %q", words[0])
		}
	}
}
