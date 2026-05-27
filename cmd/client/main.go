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

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("%s\n", err)
	}

	pauseCh, _, err := pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, username),
		routing.PauseKey,
		pubsub.TransientQueueType,
	)
	if err != nil {
		log.Fatalf("%s\n", err)
	}
	defer pauseCh.Close()

	state := gamelogic.NewGameState(username)

gameloop:
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "spawn":
			if err := state.CommandSpawn(words); err != nil {
				fmt.Printf("failed to spawn a new unit: %v\n", err)
			}
		case "move":
			if mv, err := state.CommandMove(words); err != nil {
				fmt.Printf("failed to move: %v\n", err)
			} else {
				fmt.Printf("Move successful: %+v\n", mv)
			}
		case "status":
			state.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			log.Println("Exiting")
			break gameloop
		default:
			log.Printf("unknown command: %q", words[0])
		}
	}
}
