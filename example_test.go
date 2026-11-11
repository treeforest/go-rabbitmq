package mq_test

import (
	"context"
	"fmt"
	"strings"
	"time"

	mq "github.com/treeforest/go-rabbitmq"
)

// ExampleNewClient 演示如何初始化一个带默认重连与 Publisher Confirm 能力的 RabbitMQ 客户端。
//
//nolint:errcheck // Examples intentionally omit error handling.
func ExampleNewClient() {
	client, err := mq.NewClient(mq.Config{
		URL:            "amqp://test:test123@127.0.0.1:5672/",
		Name:           "order-service",
		Exchange:       "shop.events",
		ExchangeType:   "topic",
		ConnectTimeout: 5 * time.Second,
		PublishTimeout: 5 * time.Second,
		CloseTimeout:   5 * time.Second,
	})
	if err != nil {
		return
	}
	defer func() {
		_ = client.Close()
	}()
}

// ExampleClient_EnsureTopology 演示如何在消费者尚未上线前预声明交换机、主队列、延迟重试队列与死信队列，
// 防止发布端早期发送的 mandatory 消息因无队列绑定而被 Broker 退回。
//
//nolint:errcheck // Examples intentionally omit error handling.
func ExampleClient_EnsureTopology() {
	client, err := mq.NewClient(mq.Config{
		URL:      "amqp://test:test123@127.0.0.1:5672/",
		Name:     "topology-initializer",
		Exchange: "shop.events",
	})
	if err != nil {
		return
	}
	defer func() {
		_ = client.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = client.EnsureTopology(ctx, mq.SubscribeOptions{
		Queue:         "order-service.order-events",
		BindingKeys:   []string{"shop.order.*"},
		MaxDeliveries: 5,
		RetryDelay:    2 * time.Second,
	})
}

// ExampleClient_Publish 演示如何发布持久化消息，并区分可安全重试错误与投递结果未知（delivery outcome unknown）错误。
// routingKey 是这条消息的具体路由标签，不能带 * 或 #；订阅端用 SubscribeOptions.BindingKeys 配置匹配规则。direct 下两者填同一个值，topic 下 BindingKeys 可以使用 * 与 #。
//
//nolint:errcheck // Examples intentionally omit error handling.
func ExampleClient_Publish() {
	client, err := mq.NewClient(mq.Config{
		URL:      "amqp://test:test123@127.0.0.1:5672/",
		Name:     "order-publisher",
		Exchange: "shop.events",
	})
	if err != nil {
		return
	}
	defer func() {
		_ = client.Close()
	}()

	err = client.Publish(
		context.Background(),
		"shop.order.created",
		[]byte(`{"order_id":"ord-1001"}`),
		mq.WithMessageID("ord-1001-created"),
		mq.WithHeader("trace_id", "tr-9981"),
		mq.WithMandatory(true),
	)
	if err != nil && strings.Contains(err.Error(), "delivery outcome unknown") {
		// 消息已写出但确认超时或断连，重发时务必保持相同的 MessageID 依赖消费端幂等去重。
		return
	}
}

// ExampleClient_Subscribe 演示如何并发消费消息、利用 msg.ID 做业务幂等、读取 msg.DeliveryCount，
// 以及在服务停机时通过 Drain 等待在途消息处理完成后再 Close。
//
//nolint:errcheck // Examples intentionally omit error handling.
func ExampleClient_Subscribe() {
	client, err := mq.NewClient(mq.Config{
		URL:      "amqp://test:test123@127.0.0.1:5672/",
		Name:     "order-consumer",
		Exchange: "shop.events",
	})
	if err != nil {
		return
	}
	defer func() {
		_ = client.Close()
	}()

	startupCtx, cancelStartup := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelStartup()

	sub, err := client.Subscribe(startupCtx, mq.SubscribeOptions{
		Queue:         "order-service.order-events",
		BindingKeys:   []string{"shop.order.*"},
		Prefetch:      32,
		Concurrency:   8,
		MaxDeliveries: 3,
		RetryDelay:    2 * time.Second,
	}, func(_ context.Context, msg mq.Message) error {
		// 1. 使用 msg.ID 执行业务幂等校验；msg.DeliveryCount 为 1-based 投递次数（首次为 1，达到 MaxDeliveries 后进入 DLQ）。
		if msg.ID == "" {
			return fmt.Errorf("missing message id")
		}
		// 2. 返回 nil 自动 Ack；返回 error（或发生 panic）自动按 RetryDelay 延迟重试，达到 MaxDeliveries 进入 DLQ。
		return nil
	})
	if err != nil {
		return
	}

	// 3. 停机时先调用 Drain 停止拉取新消息并等待在途 handler 完成，最后调用 Close 释放资源。
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelDrain()
	_ = sub.Drain(drainCtx)
	_ = sub.Close()
}
