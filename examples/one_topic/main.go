package main

import (
	"context"
	"time"

	"github.com/tinybluerobots/pogomq"
)

type msg struct{}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	connectionString := "host=localhost port=5432 user=postgres password=postgres dbname=postgres"

	// Create a new client
	client, err := pogomq.NewClient[msg](ctx, connectionString, pogomq.WithAutoComplete(), pogomq.WithTopic("pingpong"))
	if err != nil {
		panic(err)
	}

	defer client.Close()

	// The message handler receives messages from the queue and returns a result indicating whether processing completed or failed, with optional messages to be sent back to the queue.
	messageHandler := func(ctx context.Context, message pogomq.Message[msg]) pogomq.MessageResult {
		switch message.GetId() {
		case "ping":
			println("Received ping message")
			scheduled := time.Now().Add(time.Second)
			pongMsg := pogomq.NewMessage("pong", msg{}).SetScheduled(scheduled)

			return message.Complete().Delete().Publish(pongMsg)
		default:
			println("Received pong message")
			scheduled := time.Now().Add(time.Second)
			pingMsg := pogomq.NewMessage("ping", msg{}).SetScheduled(scheduled)

			return message.Complete().Delete().Publish(pingMsg)
		}
	}

	// Start the subscriber with the message handler
	errs, err := client.Subscribe(ctx, messageHandler)
	if err != nil {
		panic(err)
	}

	// Create a new message with a unique ID
	ping := pogomq.NewMessage("ping", msg{})

	// Publish the message to the queue
	if err := client.Publish(ctx, ping); err != nil {
		cancel()
		panic(err)
	}

	// Catch subscriber errors
	select {
	case err = <-errs:
		panic(err)
	}
}
