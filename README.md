# PogoMQ - A Postgres Message Queue for Go

A lightweight, type-safe, reliable message queue system built on top of LISTEN/NOTIFY. It supports multiple topics, retries, and concurrent processing.

### Database Setup
You can use the [provided migrations](/internal/sql/migrations/001_initial_schema.sql) with [goose](https://github.com/pressly/goose) to set up your database.

## Quick Start
See the full [example](/examples/one_topic/main.go)
```go
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
            pongMsg := pogomq.NewScheduledMessage("pong", msg{}, scheduled)

            return message.Complete().Delete().Publish(pongMsg)
        default:
            println("Received pong message")
            scheduled := time.Now().Add(time.Second)
            pingMsg := pogomq.NewScheduledMessage("ping", msg{}, scheduled)

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
```

## Configuration Options
PogoMQ clients can be configured with several options:

| Option | Description | Default |
|--------|-------------|---------|
| `WithAutoComplete` | Automatically complete messages after successful delivery | `false` |
| `WithMaxDeliveryCount` | Maximum number of delivery attempts | `1` |
| `WithTopic` | Topic name for the client | `"default"` |
| `WithTTL` | Time-to-live duration for messages | `none` |
| `WithWorkerCount` | Number of concurrent workers | `1` |

## Client Operations
- `client.Close()`: Close the client and release resources
- `client.MessageCounts(ctx)`: Get counts of messages by status, returns a struct with `Active`, `Completed`, `Failed`, and `Scheduled` integer fields
- `client.Publish(ctx, message)`: Publish a message to the queue
- `client.PurgeAllMessages(ctx)`: Delete all messages
- `client.PurgeCompletedMessages(ctx)`: Delete all completed messages
- `client.PurgeFailedMessages(ctx)`: Delete all failed messages
- `client.PurgeTTLMessages(ctx)`: Delete all messages with expired TTL
- `client.ReadFailedMessages(ctx, limit)`: Read failed messages up to the specified limit
- `client.ResetFailedMessages(ctx)`: Reschedule all failed messages that exceeded max delivery count for reprocessing
- `client.ResetFailedMessage(ctx, id)`: Reschedule an individual failed message that exceeded max delivery count for reprocessing
- `client.Subscribe(ctx, messageHandler)`: Start listening for messages, returns an error chan

## Message Operations
- `msg.Complete()`: Mark the message as completed, returns a `messageResultCompleted`
- `msg.GetBody()`: The message body
- `msg.GetDeliveryCount()`: The number of times the message has been delivered, useful for calculating retry intervals
- `msg.GetId()`: The message id
- `msg.GetScheduled()`: Schedule time for delivery
- `msg.GetTTL()`: Get the TTL timestamp, if set
- `msg.Fail()`: Mark the message as failed and to be retried immediately, returns a `messageResultFailed`
- `msg.SetScheduled(time)`: Set the time for delivery
- `msg.SetTTL(duration)`: Set the TTL differently to the default set in the client

### Message Result Operations
#### Completed Result
- `result.Delete()`: Delete the message from the queue instead of marking it as completed
- `result.Publish(messages...)`: Publish additional messages to the queue

#### Failed Result
- `result.Delete()`: Delete the message from the queue instead of marking it as failed
- `result.Publish(messages...)`: Publish additional messages to the queue
- `result.Reschedule(time)`: Reschedule the message to be processed at a specified time
