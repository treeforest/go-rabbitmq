// Package mq 的 example_test 展示 RabbitMQ 官方术语 API 的基本用法。
package mq_test

import (
	"context"
	"fmt"
	"log"
	"time"

	mq "github.com/treeforest/go-rabbitmq"
)

// Example 展示一个服务如何声明队列拓扑、订阅消息并发布事件。
func Example() {
	ctx := context.Background()

	if false {
		if err := runExample(ctx); err != nil {
			log.Fatal(err)
		}
	}
}

func runExample(ctx context.Context) error {
	client, err := mq.NewClient(mq.Config{
		URL:      "amqp://test:test123@127.0.0.1:5672/",
		Name:     "example-app",
		Exchange: "test.events",
	})
	if err != nil {
		return err
	}

	subscription, err := client.Subscribe(ctx, mq.SubscribeOptions{
		Queue:       "example-service.order-events",
		RoutingKey:  "test.order.created",
		BindingKeys: []string{"test.order.*"},
		Prefetch:    16,
		MaxDeliver:  3,
		RetryDelay:  2 * time.Second,
	}, func(_ context.Context, msg mq.Message) error {
		fmt.Printf("收到消息: queue=%s routing_key=%s id=%s\n", msg.Queue, msg.RoutingKey, msg.ID)
		return nil
	})
	if err != nil {
		if closeErr := client.Close(); closeErr != nil {
			return fmt.Errorf("subscribe failed: %w; close client: %w", err, closeErr)
		}
		return err
	}

	defer func() {
		if err := subscription.Close(); err != nil {
			log.Printf("close subscription: %v", err)
		}
		if err := client.Close(); err != nil {
			log.Printf("close client: %v", err)
		}
	}()

	return client.Publish(
		ctx,
		"test.order.created",
		[]byte(`{"order_id":"order-1"}`),
		mq.WithMessageID("order-created-1"),
		mq.WithHeader("event_type", "order.created"),
	)
}
