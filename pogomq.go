package pogomq

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tinybluerobots/pogomq/internal/sql"
	"golang.org/x/sync/errgroup"
)

type messageHandler[T any] func(context.Context, Message[T]) MessageResult

func now() time.Time {
	return time.Now().UTC()
}

type Message[T any] struct {
	body          T
	deliveryCount int
	id            string
	scheduled     time.Time
}

// Creates a new message with the specified ID and body.
func NewMessage[T any](id string, body T) Message[T] {
	return Message[T]{
		body: body,
		id:   id,
	}
}

// Creates a new message with the specified ID, body, and scheduled time.
func NewScheduledMessage[T any](id string, body T, scheduled time.Time) Message[T] {
	return Message[T]{
		body:      body,
		id:        id,
		scheduled: scheduled,
	}
}

func (m Message[T]) GetBody() T {
	return m.body
}

func (m Message[T]) GetDeliveryCount() int {
	return m.deliveryCount
}

func (m Message[T]) GetId() string {
	return m.id
}

func (m Message[T]) GetScheduled() time.Time {
	return m.scheduled
}

// Marks the message as completed.
func (m Message[T]) Complete() messageResultCompleted[T] {
	return messageResultCompleted[T]{}
}

// Marks the message as failed and to be retried immediately.
func (m Message[T]) Fail() messageResultFailed[T] {
	return messageResultFailed[T]{scheduled: now()}
}

type MessageResult interface {
	messageResult()
}

type messageResultCompleted[T any] struct {
	delete   bool
	messages []Message[T]
}

func (messageResultCompleted[T]) messageResult() {}

// Deletes the message from the queue.
func (m messageResultCompleted[T]) Delete() messageResultCompleted[T] {
	m.delete = true
	return m
}

// Publishes additional messages to the queue.
func (m messageResultCompleted[T]) Publish(messages ...Message[T]) messageResultCompleted[T] {
	m.messages = append(m.messages, messages...)
	return m
}

type messageResultFailed[T any] struct {
	delete    bool
	messages  []Message[T]
	scheduled time.Time
}

func (messageResultFailed[T]) messageResult() {}

// Deletes the message from the queue.
func (m messageResultFailed[T]) Delete() messageResultFailed[T] {
	m.delete = true
	return m
}

// Publishes additional messages to the queue.
func (m messageResultFailed[T]) Publish(messages ...Message[T]) messageResultFailed[T] {
	m.messages = append(m.messages, messages...)
	return m
}

// Reschedules the message to be processed later.
func (m messageResultFailed[T]) Reschedule(scheduled time.Time) messageResultFailed[T] {
	m.scheduled = scheduled
	return m
}

type clientOptions struct {
	autoComplete     bool
	maxDeliveryCount int
	topic            string
	workerCount      int
}

type clientOption func(*clientOptions)

// Sets the autoComplete option for the client.
// If set to true, messages will be automatically marked as completed when dequeued.
func WithAutoComplete() clientOption {
	return func(c *clientOptions) {
		c.autoComplete = true
	}
}

// Sets the maxDeliveryCount option for the client, defaults to 1.
// This is the maximum number of times a message can be delivered before it is considered failed.
func WithMaxDeliveryCount(maxDeliveryCount int) clientOption {
	return func(c *clientOptions) {
		c.maxDeliveryCount = maxDeliveryCount
	}
}

// Sets the topic name for the client, defaults to "default".
func WithTopic(topic string) clientOption {
	return func(c *clientOptions) {
		c.topic = topic
	}
}

// Sets the worker count for the client, defaults to 1.
// This is the number of concurrent workers that will process messages in the subscriber.
func WithWorkerCount(workerCount int) clientOption {
	return func(c *clientOptions) {
		c.workerCount = workerCount
	}
}

type client[T any] struct {
	close   func()
	options clientOptions
	pool    *pgxpool.Pool
	queries *sql.Queries
}

// Closes the client and releases any resources.
func (c *client[T]) Close() {
	c.close()
}

type messageCounts struct {
	Active    int
	Completed int
	Failed    int
	Scheduled int
}

// Returns the counts of messages in the queue.
func (c *client[T]) MessageCounts(ctx context.Context) (messageCounts, error) {
	row, err := c.queries.MessageCounts(ctx, c.options.topic)
	if err != nil {
		return messageCounts{}, err
	}

	return messageCounts{
		Active:    int(row.ActiveCount),
		Completed: int(row.CompletedCount),
		Failed:    int(row.FailedCount),
		Scheduled: int(row.ScheduledCount),
	}, nil
}

// Publish messages to the queue.
// WARNING: Do not publish a message until a subscriber is listening otherwise the first delivery attempt will be lost.
func (c *client[T]) Publish(ctx context.Context, messages ...Message[T]) error {
	if len(messages) == 0 {
		return nil
	}

	rows := make([]sql.EnqueueMessagesParams, len(messages))

	for i, message := range messages {
		bytes, err := json.Marshal(message.body)
		if err != nil {
			return err
		}

		rows[i] = sql.EnqueueMessagesParams{
			AutoComplete:     c.options.autoComplete,
			Body:             bytes,
			ID:               message.id,
			MaxDeliveryCount: int32(c.options.maxDeliveryCount),
			Scheduled:        pgtype.Timestamptz{Time: message.scheduled.UTC(), Valid: true},
			Topic:            c.options.topic,
		}
	}

	_, err := c.queries.EnqueueMessages(ctx, rows)

	return err
}

// Deletes completed messages older than the specified time.
func (c *client[T]) PurgeMessages(ctx context.Context, olderThan time.Time) error {
	return c.queries.PurgeMessages(ctx, sql.PurgeMessagesParams{
		Before: pgtype.Timestamptz{Time: olderThan.UTC(), Valid: true},
		Topic:  c.options.topic,
	})
}

// Reschedules failed messages that exceeded max delivery count, allowing them to be processed again
func (c *client[T]) ResetFailedMessages(ctx context.Context) error {
	return c.queries.ResetFailedMessages(ctx, c.options.topic)
}

// Listens to the topic for incoming messages and processes them concurrently using the handler and worker count.
// It returns a context that will be canceled when the subscription is closed or an error occurs.
func (c *client[T]) Subscribe(ctx context.Context, messageHandler messageHandler[T]) (context.Context, error) {
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return ctx, err
	}

	messageChannelName := "pogomq_message_" + c.options.topic
	scheduleChannelName := "pogomq_schedule_" + c.options.topic

	_, err = conn.Exec(ctx, "LISTEN "+messageChannelName)
	if err != nil {
		conn.Release()
		return ctx, err
	}

	_, err = conn.Exec(ctx, "LISTEN "+scheduleChannelName)
	if err != nil {
		conn.Release()
		return ctx, err
	}

	var earliestScheduled *time.Time

	eg, egCtx := errgroup.WithContext(ctx)
	pollTimer := time.NewTimer(0)

	eg.Go(func() error {
		for {
			select {
			case <-egCtx.Done():
				return nil
			case <-pollTimer.C:
				if err := c.queries.RescheduleMessages(egCtx, c.options.topic); err != nil {
					return err
				}

				earliestScheduled = nil
			}
		}
	})

	eg.Go(func() error {
		defer func() {
			pollTimer.Stop()
			conn.Release()
		}()

		workerGroup, workerCtx := errgroup.WithContext(egCtx)
		workerGroup.SetLimit(c.options.workerCount)

		for {
			select {
			case <-workerCtx.Done():
				return workerGroup.Wait()
			default:
				notification, err := conn.Conn().WaitForNotification(workerCtx)
				if err != nil {
					return err
				}

				var messageNotification struct {
					Body          json.RawMessage    `json:"body"`
					DeliveryCount int32              `json:"delivery_count"`
					Id            string             `json:"id"`
					Scheduled     pgtype.Timestamptz `json:"scheduled"`
				}

				var scheduleNotification struct {
					Scheduled pgtype.Timestamptz `json:"scheduled"`
				}

				switch notification.Channel {
				case messageChannelName:
					if err := json.Unmarshal([]byte(notification.Payload), &messageNotification); err != nil {
						return err
					}

					workerGroup.Go(func() error {
						var body T
						if err := json.Unmarshal(messageNotification.Body, &body); err != nil {
							return err
						}

						message := Message[T]{
							body:          body,
							deliveryCount: int(messageNotification.DeliveryCount),
							id:            messageNotification.Id,
							scheduled:     messageNotification.Scheduled.Time.UTC(),
						}

						messageResult := messageHandler(workerCtx, message)

						switch messageResult := messageResult.(type) {
						case messageResultCompleted[T]:
							if messageResult.delete {
								if err := c.queries.DeleteMessage(workerCtx, message.id); err != nil {
									return err
								}
							} else if !c.options.autoComplete {
								if err := c.queries.CompleteMessage(workerCtx, message.id); err != nil {
									return err
								}
							}

							if err := c.Publish(workerCtx, messageResult.messages...); err != nil {
								return err
							}

						case messageResultFailed[T]:
							if messageResult.delete {
								if err := c.queries.DeleteMessage(workerCtx, message.id); err != nil {
									return err
								}
							} else if err := c.queries.FailMessage(workerCtx, sql.FailMessageParams{
								ID:        message.id,
								Scheduled: pgtype.Timestamptz{Time: messageResult.scheduled.UTC(), Valid: true},
							}); err != nil {
								return err
							}

							if err := c.Publish(workerCtx, messageResult.messages...); err != nil {
								return err
							}
						}

						return nil
					})
				case scheduleChannelName:
					if err := json.Unmarshal([]byte(notification.Payload), &scheduleNotification); err != nil {
						return err
					}

					if earliestScheduled == nil || scheduleNotification.Scheduled.Time.Before(*earliestScheduled) {
						earliestScheduled = &scheduleNotification.Scheduled.Time
						interval := earliestScheduled.Sub(now())
						pollTimer.Reset(interval)
					}
				}
			}
		}
	})

	return egCtx, nil
}

// Creates a new client with the specified context, connection string, and options.
func NewClient[T any](ctx context.Context, connectionString string, opts ...clientOption) (*client[T], error) {
	pgxConfig, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.NewWithConfig(ctx, pgxConfig)
	if err != nil {
		return nil, err
	}

	clientOptions := clientOptions{
		autoComplete:     false,
		maxDeliveryCount: 1,
		topic:            "default",
		workerCount:      1,
	}

	for _, opt := range opts {
		opt(&clientOptions)
	}

	queries := sql.New(pool)
	client := &client[T]{
		close:   pool.Close,
		options: clientOptions,
		pool:    pool,
		queries: queries,
	}

	return client, nil
}
