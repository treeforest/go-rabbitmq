//go:build integration

// Package mq 的集成测试需要外部 RabbitMQ 服务。
// 运行示例：RABBITMQ_URL=amqp://test:test123@127.0.0.1:5672/ go test -tags=integration . -v
package mq

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func newIntegrationClient(t *testing.T) *Client {
	t.Helper()

	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		t.Skip("RABBITMQ_URL is required for integration test")
	}

	client, err := NewClient(Config{
		URL:      url,
		Name:     "test-rabbitmq-integration-test",
		Exchange: integrationExchange(t),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})
	return client
}

func integrationExchange(t *testing.T) string {
	t.Helper()
	return "test.it." + uuid.NewString()[:8]
}

func integrationNames(t *testing.T) (string, string) {
	t.Helper()
	suffix := uuid.NewString()
	return fmt.Sprintf("test.key.%s.created", suffix), "consumer_" + suffix[:8]
}

// TestIntegrationPublishSubscribe 验证发布、订阅和手动 ack 主链路。
func TestIntegrationPublishSubscribe(t *testing.T) {
	client := newIntegrationClient(t)
	routingKey, queue := integrationNames(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	received := make(chan Message, 1)
	sub, err := client.Subscribe(ctx, SubscribeOptions{
		Queue:       queue,
		RoutingKey:  routingKey,
		BindingKeys: []string{routingKey},
	}, func(_ context.Context, msg Message) error {
		received <- msg
		return nil
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, sub.Close())
	})

	require.NoError(t, client.Publish(ctx, routingKey, []byte("hello"), WithMessageID("msg-1")))

	select {
	case msg := <-received:
		require.Equal(t, "msg-1", msg.ID)
		require.Equal(t, []byte("hello"), msg.Data)
		require.Equal(t, queue, msg.Queue)
		require.Equal(t, routingKey, msg.RoutingKey)
	case <-ctx.Done():
		t.Fatalf("wait message failed: %v", ctx.Err())
	}
}

// TestIntegrationFailureRetriesThenSucceeds 验证 handler 失败后会进入延迟重试并再次投递。
func TestIntegrationFailureRetriesThenSucceeds(t *testing.T) {
	client := newIntegrationClient(t)
	routingKey, queue := integrationNames(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	var attempts atomic.Int32
	done := make(chan struct{})
	sub, err := client.Subscribe(ctx, SubscribeOptions{
		Queue:       queue,
		RoutingKey:  routingKey,
		BindingKeys: []string{routingKey},
		MaxDeliver:  3,
		RetryDelay:  200 * time.Millisecond,
	}, func(_ context.Context, _ Message) error {
		if attempts.Add(1) == 1 {
			// 测试首次处理失败，模拟进行再次投递
			return fmt.Errorf("transient failure")
		}
		close(done)
		// 测试后续处理成功
		return nil
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, sub.Close())
	})

	require.NoError(t, client.Publish(ctx, routingKey, []byte("retry"), WithMessageID("retry-1")))

	select {
	case <-done:
		require.Equal(t, int32(2), attempts.Load())
	case <-ctx.Done():
		t.Fatalf("wait retry failed: %v", ctx.Err())
	}
}

// TestIntegrationFailureGoesToDLQ 验证超过最大投递次数后消息进入 DLQ。
func TestIntegrationFailureGoesToDLQ(t *testing.T) {
	client := newIntegrationClient(t)
	routingKey, queue := integrationNames(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	sub, err := client.Subscribe(ctx, SubscribeOptions{
		Queue:       queue,
		RoutingKey:  routingKey,
		BindingKeys: []string{routingKey},
		MaxDeliver:  1,
		RetryDelay:  100 * time.Millisecond,
	}, func(context.Context, Message) error {
		// 总是返回错误，导致投递次数超过 MaxDeliver 进入 DLQ
		return fmt.Errorf("permanent failure")
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, sub.Close())
	})

	require.NoError(t, client.Publish(ctx, routingKey, []byte("dlq"), WithMessageID("dlq-1")))

	nc, err := amqp.Dial(os.Getenv("RABBITMQ_URL"))
	require.NoError(t, err)
	defer nc.Close()

	ch, err := nc.Channel()
	require.NoError(t, err)
	defer ch.Close()

	deadline := time.After(10 * time.Second)
	for {
		msg, ok, err := ch.Get(dlqName(queue), false)
		require.NoError(t, err)
		if ok {
			require.Equal(t, "dlq-1", msg.MessageId)
			require.NoError(t, msg.Ack(false))
			return
		}

		select {
		case <-deadline:
			t.Fatalf("wait dlq message timeout")
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// TestIntegrationEnsureTopologyPreservesMessageBeforeSubscribe 验证预建拓扑后订阅未启动也不会丢消息。
func TestIntegrationEnsureTopologyPreservesMessageBeforeSubscribe(t *testing.T) {
	client := newIntegrationClient(t)
	routingKey, queue := integrationNames(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	opts := SubscribeOptions{
		Queue:       queue,
		RoutingKey:  routingKey,
		BindingKeys: []string{routingKey},
	}
	require.NoError(t, client.EnsureTopology(ctx, opts))
	require.NoError(t, client.Publish(ctx, routingKey, []byte("stored"), WithMessageID("stored-1")))

	received := make(chan Message, 1)
	sub, err := client.Subscribe(ctx, opts, func(_ context.Context, msg Message) error {
		received <- msg
		return nil
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, sub.Close())
	})

	select {
	case msg := <-received:
		require.Equal(t, "stored-1", msg.ID)
	case <-ctx.Done():
		t.Fatalf("wait stored message failed: %v", ctx.Err())
	}
}
