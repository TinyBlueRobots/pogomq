package pogomq

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var errMigrationFailed = errors.New("goose migration failed")

func randomString() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func runGooseMigrations(connStr string) error {
	migrationsDir := "./internal/sql/migrations"
	cmd := exec.Command("go", "tool", "goose", "-dir", migrationsDir, "postgres", connStr, "up")

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %v, output: %s", errMigrationFailed, err, output)
	}

	return nil
}

type testMessage struct {
	Name string
}

func getMessageCounts(ctx context.Context, c *client[testMessage]) messageCounts {
	messageCounts, err := c.MessageCounts(ctx)
	if err != nil {
		panic(err)
	}

	return messageCounts
}

func Test_client(t *testing.T) {
	ctx := t.Context()
	containerRequest := testcontainers.ContainerRequest{
		Image:        "postgres:latest",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_PASSWORD": "postgres",
		},
		WaitingFor: wait.ForExposedPort(),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: containerRequest,
		Started:          true,
	})
	if err != nil {
		t.Fatal(err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}

	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err := container.Terminate(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}()

	connStr := fmt.Sprintf("host=%s port=%d user=postgres password=postgres dbname=postgres", host, port.Int())

	if err := runGooseMigrations(connStr); err != nil {
		t.Fatal(err)
	}

	newClient := func(opts ...clientOption) *client[testMessage] {
		topic := fmt.Sprintf("%d", time.Now().UnixNano())
		opts = append(opts, WithTopic(topic))

		client, err := NewClient[testMessage](ctx, connStr, opts...)
		if err != nil {
			panic(err)
		}

		return client
	}

	testMsg := testMessage{Name: "test"}

	t.Run("auto complete, fail then succeed", func(t *testing.T) {
		var actualMessage Message[testMessage]

		sem := make(chan any)
		handler := func(_ context.Context, message Message[testMessage]) MessageResult {
			actualMessage = message

			if message.GetDeliveryCount() == 1 {
				return message.Fail()
			}

			close(sem)

			return message.Complete()
		}
		client := newClient(WithAutoComplete(), WithMaxDeliveryCount(2))

		errs, err := client.Subscribe(ctx, handler)
		if err != nil {
			t.Fatal(err)
		}

		if err := client.Publish(ctx, NewMessage(randomString(), testMsg)); err != nil {
			t.Fatal(err)
		}

		select {
		case <-time.After(5 * time.Second):
			t.Log("timeout")
			t.Fail()
		case err = <-errs:
			t.Log(err)
			t.Fail()
		case <-sem:
		}

		assert.Equal(t, testMsg, actualMessage.GetBody())
		assert.Equal(t, 2, actualMessage.GetDeliveryCount())

		counts := getMessageCounts(ctx, client)
		assert.Equal(t, messageCounts{Completed: 1}, counts)
	})

	t.Run("scheduled, two messages, manual complete", func(t *testing.T) {
		var actualMessage Message[testMessage]

		sem := make(chan any)
		receivedCount := atomic.Int32{}
		handler := func(_ context.Context, message Message[testMessage]) MessageResult {
			actualMessage = message

			if receivedCount.Add(1) == 2 {
				close(sem)
			}

			return message.Complete()
		}
		client := newClient()

		errs, err := client.Subscribe(ctx, handler)
		if err != nil {
			t.Fatal(err)
		}

		messages := []Message[testMessage]{
			NewMessage(randomString(), testMsg).SetScheduled(time.Now().Add(time.Second)),
			NewMessage(randomString(), testMsg).SetScheduled(time.Now().Add(2 * time.Second)),
		}

		if err := client.Publish(ctx, messages...); err != nil {
			t.Fatal(err)
		}

		counts := getMessageCounts(ctx, client)
		assert.Equal(t, messageCounts{Scheduled: 2}, counts)

		select {
		case <-time.After(5 * time.Second):
			t.Log("timeout")
			t.Fail()
		case err = <-errs:
			t.Log(err)
			t.Fail()
		case <-sem:
		}

		assert.Equal(t, testMsg, actualMessage.GetBody())
		assert.Equal(t, 1, actualMessage.GetDeliveryCount())
	})

	t.Run("fails and reaches max delivery count", func(t *testing.T) {
		var actualMessage Message[testMessage]

		sem := make(chan any)
		handler := func(_ context.Context, message Message[testMessage]) MessageResult {
			actualMessage = message

			if message.GetDeliveryCount() == 2 {
				close(sem)
			}

			return message.Fail()
		}
		client := newClient(WithMaxDeliveryCount(2))

		errs, err := client.Subscribe(ctx, handler)
		if err != nil {
			t.Fatal(err)
		}

		if err := client.Publish(ctx, NewMessage(randomString(), testMsg)); err != nil {
			t.Fatal(err)
		}

		counts := getMessageCounts(ctx, client)
		assert.Equal(t, messageCounts{Active: 1}, counts)

		select {
		case <-time.After(5 * time.Second):
			t.Log("timeout")
			t.Fail()
		case err = <-errs:
			t.Log(err)
			t.Fail()
		case <-sem:
		}

		counts = getMessageCounts(ctx, client)
		assert.Equal(t, messageCounts{Failed: 1}, counts)
		assert.Equal(t, 2, actualMessage.GetDeliveryCount())
	})

	t.Run("duplicate message", func(t *testing.T) {
		sem := make(chan any)
		handler := func(_ context.Context, message Message[testMessage]) MessageResult {
			close(sem)

			return message.Complete()
		}
		client := newClient()

		_, err := client.Subscribe(ctx, handler)
		if err != nil {
			t.Fatal(err)
		}

		message := NewMessage(randomString(), testMsg)
		err = client.Publish(ctx, message, message)
		require.ErrorContains(t, err, "duplicate key value violates unique constraint")
	})

	t.Run("handler creates a new message", func(t *testing.T) {
		var actualMessage Message[testMessage]

		sem := make(chan any)
		receivedCount := atomic.Int32{}
		secondMsg := NewMessage(randomString(), testMessage{Name: "seconds"})
		handler := func(_ context.Context, message Message[testMessage]) MessageResult {
			if receivedCount.Add(1) == 2 {
				actualMessage = message

				close(sem)
			}

			return message.Complete().Publish(secondMsg)
		}
		client := newClient()

		errs, err := client.Subscribe(ctx, handler)
		if err != nil {
			t.Fatal(err)
		}

		if err := client.Publish(ctx, NewMessage(randomString(), testMsg)); err != nil {
			t.Fatal(err)
		}

		select {
		case <-time.After(5 * time.Second):
			t.Log("timeout")
			t.Fail()
		case err = <-errs:
			t.Log(err)
			t.Fail()
		case <-sem:
		}

		assert.Equal(t, secondMsg.GetBody(), actualMessage.GetBody())
	})

	t.Run("publish before subscribe", func(t *testing.T) {
		sem := make(chan any)
		handler := func(_ context.Context, message Message[testMessage]) MessageResult {
			close(sem)

			return message.Complete()
		}

		//message is delivered on the second attempt, first attempt fails because subscriber is not running
		client := newClient(WithMaxDeliveryCount(2))
		if err := client.Publish(ctx, NewMessage(randomString(), testMsg)); err != nil {
			t.Fatal(err)
		}

		errs, err := client.Subscribe(ctx, handler)
		if err != nil {
			t.Fatal(err)
		}

		select {
		case <-time.After(5 * time.Second):
			t.Log("timeout")
			t.Fail()
		case err = <-errs:
			t.Log(err)
			t.Fail()
		case <-sem:
		}
	})

	t.Run("1000 messages", func(t *testing.T) {
		sem := make(chan any)
		receivedCount := atomic.Int32{}
		handler := func(_ context.Context, message Message[testMessage]) MessageResult {
			if receivedCount.Add(1) == 1000 {
				close(sem)
			}

			return message.Complete()
		}
		client := newClient(WithWorkerCount(1000))

		errs, err := client.Subscribe(ctx, handler)
		if err != nil {
			t.Fatal(err)
		}

		messages := make([]Message[testMessage], 1000)

		for i := range 1000 {
			scheduled := time.Now().Add(time.Millisecond * time.Duration(i))
			messages[i] = NewMessage(randomString(), testMessage{Name: randomString()}).SetScheduled(scheduled)
		}

		if err := client.Publish(ctx, messages...); err != nil {
			t.Fatal(err)
		}

		select {
		case <-time.After(5 * time.Second):
			t.Log("timeout")
			t.Fail()
		case err = <-errs:
			t.Log(err)
			t.Fail()
		case <-sem:
		}
	})

	t.Run("ttl", func(t *testing.T) {
		sem := make(chan any)
		handler := func(_ context.Context, message Message[testMessage]) MessageResult {
			close(sem)

			return message.Complete()
		}
		client := newClient(WithTTL(time.Second))

		errs, err := client.Subscribe(ctx, handler)
		if err != nil {
			t.Fatal(err)
		}

		if err := client.Publish(ctx, NewMessage(randomString(), testMsg)); err != nil {
			t.Fatal(err)
		}

		select {
		case <-time.After(5 * time.Second):
			t.Log("timeout")
			t.Fail()
		case err = <-errs:
			t.Log(err)
			t.Fail()
		case <-sem:
			time.Sleep(time.Second)
		}

		if err := client.PurgeTTLMessages(ctx); err != nil {
			t.Fatal(err)
		}

		counts := getMessageCounts(ctx, client)
		assert.Equal(t, messageCounts{Active: 0}, counts)
	})
}
