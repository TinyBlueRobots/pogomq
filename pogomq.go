package pogomq

import (
	"context"
	"encoding/json"
	"sync/atomic"
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
	ttl           *time.Time
}

// Creates a new message with the specified ID and body.
func NewMessage[T any](id string, body T) Message[T] {
	return Message[T]{
		body: body,
		id:   id,
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

func (m Message[T]) GetTTL() *time.Time {
	return m.ttl
}

func (m Message[T]) SetScheduled(scheduled time.Time) Message[T] {
	m.scheduled = scheduled.UTC()
	return m
}

func (m Message[T]) SetTTL(ttl time.Time) Message[T] {
	m.ttl = &ttl
	return m
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
	ttl              int64
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
		c.maxDeliveryCount = max(1, maxDeliveryCount)
	}
}

// Sets the topic name for the client, defaults to "default".
func WithTopic(topic string) clientOption {
	return func(c *clientOptions) {
		c.topic = topic
	}
}

// Sets the time-to-live (TTL) for messages in the queue, defaults to none.
func WithTTL(ttl time.Duration) clientOption {
	return func(c *clientOptions) {
		c.ttl = max(0, int64(ttl.Seconds()))
	}
}

// Sets the worker count for the client, defaults to 1.
// This is the number of concurrent workers that will process messages in the subscriber.
func WithWorkerCount(workerCount int) clientOption {
	return func(c *clientOptions) {
		c.workerCount = max(1, workerCount)
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
			Ttl:              pgtype.Timestamptz{Time: now().Add(time.Duration(c.options.ttl) * time.Second).UTC(), Valid: c.options.ttl > 0},
			TtlSeconds:       pgtype.Int8{Int64: c.options.ttl, Valid: c.options.ttl > 0},
		}
	}

	_, err := c.queries.EnqueueMessages(ctx, rows)

	return err
}

// Deletes all messages from the queue.
func (c *client[T]) PurgeAllMessages(ctx context.Context) error {
	return c.queries.PurgeAllMessages(ctx, c.options.topic)
}

// Deletes completed messages from the queue.
func (c *client[T]) PurgeCompletedMessages(ctx context.Context) error {
	return c.queries.PurgeCompletedMessages(ctx, c.options.topic)
}

// Deletes failed messages from the queue.
func (c *client[T]) PurgeFailedMessages(ctx context.Context) error {
	return c.queries.PurgeFailedMessages(ctx, c.options.topic)
}

// Deletes message that have been in the queue longer than their TTL.
func (c *client[T]) PurgeTTLMessages(ctx context.Context) error {
	return c.queries.PurgeTTLMessages(ctx, c.options.topic)
}

// Reads failed messages from the queue, up to the specified limit.
func (c *client[T]) ReadFailedMessages(ctx context.Context, limit int) ([]Message[T], error) {
	rows, err := c.queries.ReadFailedMessages(ctx, sql.ReadFailedMessagesParams{
		Limit: int32(limit),
		Topic: c.options.topic,
	})
	if err != nil {
		return nil, err
	}

	messages := make([]Message[T], len(rows))

	for i, row := range rows {
		var body T
		if err := json.Unmarshal(row.Body, &body); err != nil {
			return nil, err
		}

		messages[i] = Message[T]{
			body:          body,
			deliveryCount: int(row.DeliveryCount),
			id:            row.ID,
			scheduled:     row.Scheduled.Time.UTC(),
		}
	}

	return messages, nil
}

// Reschedules failed messages that exceeded max delivery count, allowing them to be processed again
func (c *client[T]) ResetFailedMessages(ctx context.Context) error {
	return c.queries.ResetFailedMessages(ctx, c.options.topic)
}

// Reschedules a specific failed message to be processed again.
func (c *client[T]) ResetFailedMessage(ctx context.Context, id string) error {
	return c.queries.ResetFailedMessage(ctx, sql.ResetFailedMessageParams{
		ID:    id,
		Topic: c.options.topic,
	})
}

// Listens to the topic for incoming messages and processes them concurrently using the handler and worker count.
// It returns a channel of errors that may occur during async processing.
func (c *client[T]) Subscribe(ctx context.Context, messageHandler messageHandler[T]) (<-chan error, error) {
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	pollingChannel := "pogomq_polling_" + c.options.topic

	_, err = conn.Exec(ctx, "LISTEN "+pollingChannel)
	if err != nil {
		conn.Release()
		return nil, err
	}

	nextPollTime := atomic.Int64{}
	pollTimer := time.NewTimer(0)
	pollTimerChan := make(chan any, 1)
	errChan := make(chan error, 1)

	// Update the poll timer, but debounce it to avoid excessive resets
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-pollTimerChan:
				time.Sleep(100 * time.Millisecond)

				interval := max(nextPollTime.Load()-now().UnixMilli(), 0)
				nextPollTime.Store(0)
				pollTimer.Reset(time.Duration(interval) * time.Millisecond)
			}
		}
	}()

	// Listen for notifications on the polling channel and update the next poll time
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				notification, err := conn.Conn().WaitForNotification(ctx)
				if err != nil {
					errChan <- err
				}

				if notification == nil {
					continue
				}

				scheduled, err := time.Parse(time.RFC3339, notification.Payload)
				if err != nil {
					errChan <- err
				}

				if nextPollTime.Load() == 0 || scheduled.UnixMilli() < nextPollTime.Load() {
					nextPollTime.Store(scheduled.UnixMilli())
					select {
					case pollTimerChan <- struct{}{}:
					default:
					}
				}
			}
		}
	}()

	// Poll for messages from the queue
	go func() {
		defer conn.Release()
		defer pollTimer.Stop()

		workerGroup, workerCtx := errgroup.WithContext(ctx)
		workerGroup.SetLimit(c.options.workerCount)

		for {
			select {
			case <-ctx.Done():
				errChan <- workerGroup.Wait()

			case <-pollTimer.C:
				rows, err := c.queries.ReadMessages(ctx, sql.ReadMessagesParams{
					Limit: int32(c.options.workerCount),
					Topic: c.options.topic,
				})
				if err != nil {
					errChan <- err
					continue
				}

				// If there's a message scheduled for the future, set the next poll time
				if rows[0].NextTime.Valid {
					nextPollTime.Store(rows[0].NextTime.Time.UnixMilli())
					select {
					case pollTimerChan <- struct{}{}:
					default:
					}
				}

				// There are no messages, wait for the next poll
				if rows[0].ID == "" {
					continue
				}

				// Process each message concurrently
				for _, row := range rows {
					workerGroup.Go(func() error {
						var body T
						if err := json.Unmarshal(row.Body, &body); err != nil {
							return err
						}

						message := Message[T]{
							body:          body,
							deliveryCount: int(row.DeliveryCount),
							id:            row.ID,
							scheduled:     row.Scheduled.Time.UTC(),
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
				}
			}
		}
	}()

	return errChan, nil
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
