// Package mq 基于 RabbitMQ AMQP 0-9-1 提供服务间异步消息发布与订阅能力。
// 该文件定义发布、订阅选项及 RabbitMQ routing/binding key 校验。
package mq

import (
	"fmt"
	"maps"
	"strings"
	"time"
)

const (
	// DefaultPrefetch 表示默认消费者预取消息数量。
	DefaultPrefetch = 32

	// DefaultMaxDeliver 表示默认最大投递次数；达到后消息进入 DLQ。
	DefaultMaxDeliver = 5

	// DefaultRetryDelay 表示默认失败后进入 retry queue 的延迟时间。
	DefaultRetryDelay = time.Second
)

// PublishOptions 表示发布消息时的可选参数。
type PublishOptions struct {
	// MessageID 表示消息唯一标识；非空时写入 RabbitMQ message_id。
	MessageID string

	// Headers 表示消息头。
	Headers map[string]string

	// Mandatory 表示消息不可路由时是否由 broker 返回错误。
	Mandatory bool
}

// PublishOption 表示发布消息的函数式选项。
type PublishOption func(*PublishOptions)

// SubscribeOptions 表示订阅 RabbitMQ 队列的参数。
type SubscribeOptions struct {
	// Queue 表示 durable queue 名称。
	Queue string

	// RoutingKey 表示当前订阅关注的主 routing key。
	RoutingKey string

	// BindingKeys 表示绑定到 exchange 的 binding key 列表；为空时使用 RoutingKey。
	BindingKeys []string

	// Prefetch 表示 RabbitMQ Qos prefetch count。
	Prefetch int

	// MaxDeliver 表示最大投递次数；达到后消息进入 DLQ。
	MaxDeliver int

	// RetryDelay 表示 handler 失败后进入 retry queue 的延迟时间。
	RetryDelay time.Duration
}

// WithMessageID 为消息设置唯一标识。
func WithMessageID(messageID string) PublishOption {
	return func(opts *PublishOptions) {
		opts.MessageID = strings.TrimSpace(messageID)
	}
}

// WithHeader 为消息增加一个头字段。
func WithHeader(key, value string) PublishOption {
	return func(opts *PublishOptions) {
		if opts.Headers == nil {
			opts.Headers = make(map[string]string)
		}
		opts.Headers[key] = value
	}
}

// WithHeaders 批量覆盖消息头。
func WithHeaders(headers map[string]string) PublishOption {
	return func(opts *PublishOptions) {
		if len(headers) == 0 {
			return
		}
		if opts.Headers == nil {
			opts.Headers = make(map[string]string, len(headers))
		}
		maps.Copy(opts.Headers, headers)
	}
}

// WithMandatory 设置发布消息的 mandatory 标志。
func WithMandatory(mandatory bool) PublishOption {
	return func(opts *PublishOptions) {
		opts.Mandatory = mandatory
	}
}

// BuildPublishOptions 根据函数式选项构建发布配置。
func BuildPublishOptions(opts ...PublishOption) PublishOptions {
	cfg := PublishOptions{
		Headers:   make(map[string]string),
		Mandatory: true,
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(&cfg)
	}
	return cfg
}

// NormalizeSubscribeOptions 补齐订阅参数默认值并执行基础校验。
func NormalizeSubscribeOptions(opts SubscribeOptions) (SubscribeOptions, error) {
	opts.Queue = strings.TrimSpace(opts.Queue)
	opts.RoutingKey = strings.TrimSpace(opts.RoutingKey)

	if opts.Queue == "" {
		return SubscribeOptions{}, fmt.Errorf("normalize subscribe options failed: queue is required")
	}
	if opts.RoutingKey == "" {
		return SubscribeOptions{}, fmt.Errorf("normalize subscribe options failed: routing key is required")
	}
	if opts.BindingKeys == nil {
		opts.BindingKeys = []string{opts.RoutingKey}
	}
	if opts.Prefetch <= 0 {
		opts.Prefetch = DefaultPrefetch
	}
	if opts.MaxDeliver <= 0 {
		opts.MaxDeliver = DefaultMaxDeliver
	}
	if opts.RetryDelay <= 0 {
		opts.RetryDelay = DefaultRetryDelay
	}

	for i, bindingKey := range opts.BindingKeys {
		trimmed := strings.TrimSpace(bindingKey)
		if err := validateBindingKey(trimmed, i); err != nil {
			return SubscribeOptions{}, err
		}
		opts.BindingKeys[i] = trimmed
	}

	return opts, nil
}

// validateBindingKey 校验 RabbitMQ topic binding key。
// RabbitMQ 通配符必须按点号分隔为独立单词：* 匹配一个单词，# 匹配零个或多个单词。
func validateBindingKey(bindingKey string, index int) error {
	if bindingKey == "" {
		return fmt.Errorf("normalize subscribe options failed: binding key at index %d is empty", index)
	}
	if strings.Contains(bindingKey, ">") {
		return fmt.Errorf("normalize subscribe options failed: binding key %q uses unsupported wildcard >", bindingKey)
	}

	for word := range strings.SplitSeq(bindingKey, ".") {
		if word == "" {
			return fmt.Errorf("normalize subscribe options failed: binding key %q contains empty word", bindingKey)
		}
		if (strings.ContainsAny(word, "*#")) && word != "*" && word != "#" {
			return fmt.Errorf("normalize subscribe options failed: binding key %q contains invalid wildcard word %q", bindingKey, word)
		}
	}
	return nil
}
