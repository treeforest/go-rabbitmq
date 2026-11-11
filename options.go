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

	// DefaultConcurrency 表示默认单个订阅并发处理消息的 worker 数量。
	DefaultConcurrency = 1

	// DefaultMaxDeliveries 表示默认最大投递次数（包含首次投递与重试）；达到后消息进入 DLQ。
	DefaultMaxDeliveries = 5

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

	// BindingKeys 表示队列绑定到 exchange 的匹配规则列表。
	// RabbitMQ 术语区分：Publish 使用具体 routing key（不可含通配符），订阅端使用 binding key（topic 模式支持 * 与 #）。
	// topic 与 direct 模式下至少需要一个非空 binding key；fanout 模式下可为空。
	BindingKeys []string

	// Prefetch 表示 RabbitMQ Qos prefetch count；小于 Concurrency 时会自动提升至 Concurrency。
	Prefetch int

	// Concurrency 表示并发执行 handler 的 worker 数量；小于等于 0 时默认 1。
	Concurrency int

	// MaxDeliveries 表示单条消息的最大投递次数（1-based，含首次投递；例如 1 表示失败不重试直接进 DLQ）；
	// 小于等于 0 时使用 DefaultMaxDeliveries，当 Message.DeliveryCount >= MaxDeliveries 且处理失败时进入 DLQ。
	MaxDeliveries int

	// RetryDelay 表示 handler 失败后进入 retry queue 的延迟时间（最小粒度为 1ms）。
	RetryDelay time.Duration
}

// WithMessageID 为消息设置唯一标识。
func WithMessageID(messageID string) PublishOption {
	return func(opts *PublishOptions) {
		opts.MessageID = strings.TrimSpace(messageID)
	}
}

// WithHeader 为消息增加或覆盖一个头字段（请避免使用包内部保留前缀 x-mq-*）。
func WithHeader(key, value string) PublishOption {
	return func(opts *PublishOptions) {
		if opts.Headers == nil {
			opts.Headers = make(map[string]string)
		}
		opts.Headers[key] = value
	}
}

// WithHeaders 批量合并消息头（同名 key 覆盖，其余保留；请避免使用包内部保留前缀 x-mq-*）。
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

// buildPublishOptions 根据函数式选项构建发布配置。
func buildPublishOptions(opts ...PublishOption) PublishOptions {
	cfg := PublishOptions{
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

// normalizeSubscribeOptions 按默认 topic exchange 规则补齐订阅参数默认值并执行基础校验。
func normalizeSubscribeOptions(opts SubscribeOptions) (SubscribeOptions, error) {
	return normalizeSubscribeOptionsForExchange(opts, DefaultExchangeType)
}

// normalizeSubscribeOptionsForExchange 按指定 exchange 类型补齐订阅参数默认值、对齐并发度并校验路由绑定键。
func normalizeSubscribeOptionsForExchange(opts SubscribeOptions, exchangeType string) (SubscribeOptions, error) {
	// 1. 清理队列名空白并校验队列名必填。
	opts.Queue = strings.TrimSpace(opts.Queue)

	if opts.Queue == "" {
		return SubscribeOptions{}, fmt.Errorf("normalize subscribe options failed: queue is required")
	}

	// 2. 补齐 Prefetch、Concurrency、MaxDeliveries、RetryDelay 默认值，并确保 Prefetch >= Concurrency 防止并发 Worker 饥饿。
	if opts.Prefetch <= 0 {
		opts.Prefetch = DefaultPrefetch
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = DefaultConcurrency
	}
	if opts.Prefetch < opts.Concurrency {
		opts.Prefetch = opts.Concurrency
	}
	if opts.MaxDeliveries <= 0 {
		opts.MaxDeliveries = DefaultMaxDeliveries
	}
	if opts.RetryDelay <= 0 {
		opts.RetryDelay = DefaultRetryDelay
	}

	// 3. 按交换机类型（topic/direct/fanout）规范化并校验所有 BindingKeys。
	if err := normalizeSubscribeBindingKeys(&opts, exchangeType); err != nil {
		return SubscribeOptions{}, err
	}
	return opts, nil
}

// normalizeSubscribeBindingKeys 按交换机类型克隆、清理并校验订阅绑定键列表。
func normalizeSubscribeBindingKeys(opts *SubscribeOptions, exchangeType string) error {
	kind := strings.ToLower(strings.TrimSpace(exchangeType))
	// 1. fanout 交换机不依赖路由键匹配，允许 BindingKeys 为空（默认绑定空键 ""）。
	if kind == "fanout" {
		if len(opts.BindingKeys) == 0 {
			opts.BindingKeys = []string{""}
		} else {
			cloned := make([]string, len(opts.BindingKeys))
			for i, key := range opts.BindingKeys {
				cloned[i] = strings.TrimSpace(key)
			}
			opts.BindingKeys = cloned
		}
		return nil
	}

	// 2. topic 与 direct 交换机至少需要一个 binding key。
	if len(opts.BindingKeys) == 0 {
		return fmt.Errorf("normalize subscribe options failed: binding keys are required")
	}
	opts.BindingKeys = slicesCloneStrings(opts.BindingKeys)

	// 3. 逐项校验绑定键：direct 仅要求非空，topic 额外校验 * 与 # 通配符单词合法性。
	for i, bindingKey := range opts.BindingKeys {
		trimmed := strings.TrimSpace(bindingKey)
		if kind == "direct" {
			if trimmed == "" {
				return fmt.Errorf("normalize subscribe options failed: binding key at index %d is empty", i)
			}
		} else if err := validateBindingKey(trimmed, i); err != nil {
			return err
		}
		opts.BindingKeys[i] = trimmed
	}
	return nil
}

// slicesCloneStrings 浅拷贝字符串切片，避免修改调用方传入的底层数组。
func slicesCloneStrings(in []string) []string {
	out := make([]string, len(in))
	copy(out, in)
	return out
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
