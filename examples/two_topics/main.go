package main

import (
	"context"
	"time"

	"github.com/tinybluerobots/pogomq"
)

type ping struct{}

type pong struct{}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	connectionString := "host=localhost port=5432 user=postgres password=postgres dbname=postgres"

	// Create new clients for each topic
	pingClient, err := pogomq.NewClient[ping](ctx, connectionString, pogomq.WithAutoComplete(), pogomq.WithTopic("ping"))
	if err != nil {
		panic(err)
	}

	defer pingClient.Close()

	pongClient, err := pogomq.NewClient[pong](ctx, connectionString, pogomq.WithAutoComplete(), pogomq.WithTopic("pong"))
	if err != nil {
		panic(err)
	}

	defer pongClient.Close()

	var pingPublisher func(context.Context, ...pogomq.Message[ping]) error

	var pongPublisher func(context.Context, ...pogomq.Message[pong]) error

	pingHandler := func(ctx context.Context, message pogomq.Message[ping]) pogomq.MessageResult {
		println("Received ping message")
		scheduled := time.Now().Add(time.Second)
		pongMsg := pogomq.NewScheduledMessage("pong", pong{}, scheduled)
		pongPublisher(ctx, pongMsg)

		return message.Complete().Delete()
	}

	pongHandler := func(ctx context.Context, message pogomq.Message[pong]) pogomq.MessageResult {
		println("Received pong message")
		scheduled := time.Now().Add(time.Second)
		pingMsg := pogomq.NewScheduledMessage("ping", ping{}, scheduled)
		pingPublisher(ctx, pingMsg)

		return message.Complete().Delete()
	}

	pingCtx, err := pingClient.Subscribe(ctx, pingHandler)
	if err != nil {
		panic(err)
	}

	pongCtx, err := pongClient.Subscribe(ctx, pongHandler)
	if err != nil {
		panic(err)
	}

	// Assign the publish functions
	pingPublisher = pingClient.Publish
	pongPublisher = pongClient.Publish

	// Create a new message with a unique ID
	ping := pogomq.NewMessage("ping", ping{})

	// Publish the message to the queue
	if err := pingClient.Publish(ctx, ping); err != nil {
		cancel()
		panic(err)
	}

	// Wait for the subscriber to finish
	select {
	case <-pingCtx.Done():
		if err := pingCtx.Err(); err != nil {
			cancel()
			panic(err)
		}
	case <-pongCtx.Done():
		if err := pongCtx.Err(); err != nil {
			cancel()
			panic(err)
		}
	}
}
