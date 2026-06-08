// Package mq 基于 RabbitMQ AMQP 0-9-1 提供服务间异步消息发布与订阅能力。
// 该包直接采用 RabbitMQ 官方 exchange、routing key、binding key、queue 和 ack 术语。
package mq

import (
	"context"
	"time"
)

// Message 表示从 RabbitMQ 收到的一条消息。
type Message struct {
	// Exchange 表示消息来源 exchange。
	Exchange string

	// Queue 表示当前消费消息的队列名称。
	Queue string

	// RoutingKey 表示消息发布时使用的 routing key。
	RoutingKey string

	// Data 表示消息体原始字节。
	Data []byte

	// Headers 表示消息头。
	Headers map[string]string

	// ID 表示消息唯一标识，用于业务幂等或追踪。
	ID string

	// PublishedAt 表示消息发布时间。
	PublishedAt time.Time
}

// Handler 表示订阅消息处理函数。
// 返回 nil 表示处理成功并 ack；返回 error 表示处理失败并按 retry/DLQ 策略转发。
type Handler func(ctx context.Context, msg Message) error

// Subscription 表示已建立的 RabbitMQ 订阅。
type Subscription interface {
	// Drain 优雅停止订阅，并等待已投递消息处理完成。
	Drain(ctx context.Context) error

	// Close 立即停止订阅并释放相关资源。
	Close() error
}
