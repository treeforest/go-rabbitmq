//go:build integration

// Package mq 的集成测试需要外部 RabbitMQ 服务。
// 运行示例：RABBITMQ_URL=amqp://test:test123@127.0.0.1:5672/ go test -tags=integration . -v
package mq

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

// TestIntegrationPublishRecoversAfterPublishChannelClose 验证 publish channel 关闭后会自动重建并发布成功。
func TestIntegrationPublishRecoversAfterPublishChannelClose(t *testing.T) {
	client := newIntegrationClient(t)
	routingKey, _ := integrationNames(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, client.publishCh.Close())
	client.invalidateTransport()

	require.NoError(t, client.Publish(ctx, routingKey, []byte("after-channel-close"), WithMessageID("recover-ch-1"), WithMandatory(false)))
	require.False(t, client.publishCh.IsClosed())
}

// TestIntegrationPublishRecoversAfterConnectionClose 验证连接关闭后会自动重连并发布成功。
func TestIntegrationPublishRecoversAfterConnectionClose(t *testing.T) {
	client := newIntegrationClient(t)
	routingKey, _ := integrationNames(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, client.conn.Close())
	client.invalidateTransport()

	require.NoError(t, client.Publish(ctx, routingKey, []byte("after-conn-close"), WithMessageID("recover-conn-1"), WithMandatory(false)))
	require.False(t, client.conn.IsClosed())
	require.False(t, client.publishCh.IsClosed())
}

// TestIntegrationSubscribeRecoversAfterConnectionClose 验证订阅在断连后自动恢复，并能继续消费。
func TestIntegrationSubscribeRecoversAfterConnectionClose(t *testing.T) {
	client := newIntegrationClient(t)
	routingKey, queue := integrationNames(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	received := make(chan Message, 2)
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

	require.NoError(t, client.Publish(ctx, routingKey, []byte("before"), WithMessageID("before-1")))
	select {
	case msg := <-received:
		require.Equal(t, "before-1", msg.ID)
	case <-ctx.Done():
		t.Fatalf("wait before message failed: %v", ctx.Err())
	}

	require.NoError(t, client.conn.Close())
	client.invalidateTransport()

	require.NoError(t, client.Publish(ctx, routingKey, []byte("after"), WithMessageID("after-1")))
	select {
	case msg := <-received:
		require.Equal(t, "after-1", msg.ID)
	case <-ctx.Done():
		t.Fatalf("wait after message failed: %v", ctx.Err())
	}
}

// TestIntegrationRecoversAfterBrokerRestart 验证 RabbitMQ 宕机重启后客户端可自动重连，并继续发布与消费。
//
// 该测试会执行 docker compose down/up，会中断共享 broker，默认跳过。
// 本地运行示例：
//
//	docker compose up -d rabbitmq
//	RABBITMQ_URL=amqp://test:test123@127.0.0.1:5672/ \
//	RABBITMQ_COMPOSE_RESTART=1 \
//	  go test -tags=integration . -run TestIntegrationRecoversAfterBrokerRestart -v -count=1
func TestIntegrationRecoversAfterBrokerRestart(t *testing.T) {
	if os.Getenv("RABBITMQ_COMPOSE_RESTART") != "1" {
		t.Skip("set RABBITMQ_COMPOSE_RESTART=1 to run docker compose broker restart test")
	}
	requireDockerCompose(t)

	client := newIntegrationClient(t)
	routingKey, queue := integrationNames(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	received := make(chan Message, 4)
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

	require.NoError(t, client.Publish(ctx, routingKey, []byte("before-restart"), WithMessageID("before-restart-1")))
	select {
	case msg := <-received:
		require.Equal(t, "before-restart-1", msg.ID)
		require.Equal(t, []byte("before-restart"), msg.Data)
	case <-ctx.Done():
		t.Fatalf("wait before-restart message failed: %v", ctx.Err())
	}

	restartRabbitMQBroker(t)
	waitForRabbitMQReady(t, ctx, os.Getenv("RABBITMQ_URL"))

	var publishedAfterID string
	require.Eventually(t, func() bool {
		messageID := "after-restart-" + uuid.NewString()[:8]
		err := client.Publish(ctx, routingKey, []byte("after-restart"), WithMessageID(messageID))
		if err != nil {
			t.Logf("publish after restart still failing: %v", err)
			return false
		}
		publishedAfterID = messageID
		return true
	}, 90*time.Second, 500*time.Millisecond, "publish after broker restart should succeed")

	select {
	case msg := <-received:
		require.Equal(t, publishedAfterID, msg.ID)
		require.Equal(t, []byte("after-restart"), msg.Data)
	case <-ctx.Done():
		t.Fatalf("wait after-restart message failed: %v", ctx.Err())
	}

	require.False(t, client.conn.IsClosed())
	require.False(t, client.publishCh.IsClosed())
}

func requireDockerCompose(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is required for broker restart test")
	}
	cmd := exec.Command("docker", "compose", "version")
	if err := cmd.Run(); err != nil {
		t.Skip("docker compose is required for broker restart test")
	}
}

func restartRabbitMQBroker(t *testing.T) {
	t.Helper()

	composeFile := findDockerComposeFile(t)
	t.Logf("restarting rabbitmq via docker compose file=%s", composeFile)

	down := exec.Command("docker", "compose", "-f", composeFile, "down", "--remove-orphans")
	down.Stdout = os.Stdout
	down.Stderr = os.Stderr
	require.NoError(t, down.Run(), "docker compose down failed")

	// 给旧连接明确一段断开窗口，避免立刻 up 时时序过短。
	time.Sleep(2 * time.Second)

	up := exec.Command("docker", "compose", "-f", composeFile, "up", "-d", "rabbitmq")
	up.Stdout = os.Stdout
	up.Stderr = os.Stderr
	require.NoError(t, up.Run(), "docker compose up failed")
}

func findDockerComposeFile(t *testing.T) string {
	t.Helper()

	if custom := strings.TrimSpace(os.Getenv("RABBITMQ_COMPOSE_FILE")); custom != "" {
		if _, err := os.Stat(custom); err != nil {
			t.Fatalf("RABBITMQ_COMPOSE_FILE=%s is not accessible: %v", custom, err)
		}
		return custom
	}

	candidates := []string{
		"docker-compose.yml",
		filepath.Join("..", "docker-compose.yml"),
	}
	if wd, err := os.Getwd(); err == nil {
		candidates = append(candidates, filepath.Join(wd, "docker-compose.yml"))
	}
	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			abs, err := filepath.Abs(candidate)
			require.NoError(t, err)
			return abs
		}
	}
	t.Fatal("docker-compose.yml not found; set RABBITMQ_COMPOSE_FILE")
	return ""
}

func waitForRabbitMQReady(t *testing.T, ctx context.Context, url string) {
	t.Helper()
	require.NotEmpty(t, url)

	deadline, hasDeadline := ctx.Deadline()
	if !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 90*time.Second)
		t.Cleanup(cancel)
		deadline, _ = ctx.Deadline()
	}

	for {
		conn, err := amqp.Dial(url)
		if err == nil {
			_ = conn.Close()
			t.Log("rabbitmq is ready after restart")
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("wait rabbitmq ready timeout: last error: %v", err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait rabbitmq ready canceled: %v (last error: %v)", ctx.Err(), err)
		case <-time.After(500 * time.Millisecond):
		}
	}
}
