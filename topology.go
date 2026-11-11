package mq

import (
	"context"
	"fmt"
	"math"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const directExchangeType = "direct"

// EnsureTopology 预声明订阅所需的 RabbitMQ 交换机、队列与路由绑定。
// 包含主 exchange、retry exchange、DLX，以及主队列、延迟重试队列、DLQ 和对应 binding。
// 不启动消费者；适用于先建队列再发布，避免 Subscribe 启动前消息无处落库的场景。
// Subscribe 内部也会声明相同拓扑；对已存在的同名资源重复声明为幂等操作。
// EnsureTopology 只使用 opts 中的 Queue、BindingKeys 和 RetryDelay 声明拓扑，
// 忽略 Prefetch、Concurrency、MaxDeliveries 等运行时消费参数。
func (c *Client) EnsureTopology(ctx context.Context, opts SubscribeOptions) error {
	if c.isClosed() {
		return fmt.Errorf("ensure rabbitmq topology failed: client already closed")
	}
	if !c.hasTransportConfig() {
		return fmt.Errorf("ensure rabbitmq topology failed: client is not initialized")
	}
	if err := c.ensureConnected(ctx); err != nil {
		return fmt.Errorf("ensure rabbitmq topology failed: %w", err)
	}

	// 1. 规范化订阅参数，并校验 RetryDelay 可转换为 RabbitMQ x-message-ttl。
	normalized, err := normalizeSubscribeOptionsForExchange(opts, c.exchangeType)
	if err != nil {
		return fmt.Errorf("ensure rabbitmq topology failed: %w", err)
	}
	if _, err := ttlMilliseconds(normalized.RetryDelay); err != nil {
		return fmt.Errorf("ensure rabbitmq topology failed: %w", err)
	}

	// 2. 使用临时 channel 执行声明，避免长期占用连接上的 channel。
	ch, err := c.openChannel()
	if err != nil {
		c.invalidateTransport()
		return fmt.Errorf("ensure rabbitmq topology failed: open channel: %w", err)
	}
	defer func() {
		discardCloseErr(ch)
	}()

	// 3. 声明主/retry/DLQ 队列及 exchange binding。
	if err := c.declareTopology(ctx, ch, normalized); err != nil {
		return fmt.Errorf("ensure rabbitmq topology failed: %w", err)
	}
	return nil
}

// declareExchange 以持久化方式声明 exchange；重复声明相同属性是幂等操作。
func declareExchange(ch amqpChannel, name, kind string) error {
	return ch.ExchangeDeclare(name, kind, true, false, false, false, nil)
}

// declareTopology 在 broker 上声明单条订阅链路所需的交换机、队列与路由绑定。
// 消息流转：主 exchange -> 主队列消费；失败 -> retry exchange (direct, key=queue) -> retryQueue
// -> TTL 到期经默认交换机 "" (key=queue) 精准回主队列；超过 MaxDeliveries -> DLX (direct, key=queue) -> DLQ。
func (c *Client) declareTopology(ctx context.Context, ch amqpChannel, opts SubscribeOptions) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("context canceled before declare topology: %w", ctx.Err())
	default:
	}

	// 1. 声明三条 exchange：主业务交换机使用配置类型，延迟重试与死信交换机固定使用 direct 按队列名点对点路由。
	if err := declareExchange(ch, c.exchange, c.exchangeType); err != nil {
		return fmt.Errorf("declare main exchange %s: %w", c.exchange, err)
	}
	if err := declareExchange(ch, retryExchangeName(c.exchange), directExchangeType); err != nil {
		return fmt.Errorf("declare retry exchange %s: %w", retryExchangeName(c.exchange), err)
	}
	if err := declareExchange(ch, deadLetterExchangeName(c.exchange), directExchangeType); err != nil {
		return fmt.Errorf("declare dead letter exchange %s: %w", deadLetterExchangeName(c.exchange), err)
	}

	// 2. 声明主消费队列；持久化，供 Subscribe 的 consumer 拉取。
	if _, err := ch.QueueDeclare(opts.Queue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare queue %s: %w", opts.Queue, err)
	}

	// 3. 声明延迟重试队列；TTL 到期后经默认交换机精准死信回当前主队列，避免再次经过主 exchange 扇出污染其它订阅队列。
	retryTTL, err := ttlMilliseconds(opts.RetryDelay)
	if err != nil {
		return err
	}
	retryQueue := retryQueueName(opts.Queue, opts.RetryDelay)
	retryArgs := amqp.Table{
		"x-message-ttl":             retryTTL,
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": opts.Queue,
	}
	if _, err := ch.QueueDeclare(retryQueue, true, false, false, false, retryArgs); err != nil {
		return fmt.Errorf("declare retry queue %s: %w", retryQueue, err)
	}

	// 4. 声明死信队列；handler 失败且达到 MaxDeliveries 后由 DLX 点对点路由至此。
	dlq := dlqName(opts.Queue)
	if _, err := ch.QueueDeclare(dlq, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dlq %s: %w", dlq, err)
	}

	// 5. 主队列按 BindingKeys 绑定到主 exchange；retryQueue 与 DLQ 按主队列名 opts.Queue 点对点绑定。
	for _, bindingKey := range opts.BindingKeys {
		if err := ch.QueueBind(opts.Queue, bindingKey, c.exchange, false, nil); err != nil {
			return fmt.Errorf("bind queue %s key=%s: %w", opts.Queue, bindingKey, err)
		}
	}
	if err := ch.QueueBind(retryQueue, opts.Queue, retryExchangeName(c.exchange), false, nil); err != nil {
		return fmt.Errorf("bind retry queue %s key=%s: %w", retryQueue, opts.Queue, err)
	}
	if err := ch.QueueBind(dlq, opts.Queue, deadLetterExchangeName(c.exchange), false, nil); err != nil {
		return fmt.Errorf("bind dlq %s key=%s: %w", dlq, opts.Queue, err)
	}
	return nil
}

// retryExchangeName 返回延迟重试 exchange 名称。
// handler 返回错误且未达 MaxDeliveries 时，消息发布到此 exchange，再路由进 retry 队列等待重投。
func retryExchangeName(exchange string) string {
	return exchange + ".retry"
}

// deadLetterExchangeName 返回死信 exchange 名称。
// handler 失败且已达 MaxDeliveries 时，消息发布到此 exchange，再路由进 DLQ 供人工排查或补偿。
func deadLetterExchangeName(exchange string) string {
	return exchange + ".dlx"
}

// retryQueueName 返回延迟重试队列名称；按 RetryDelay 隔离，避免不同延迟策略互相影响。
// 消息在此排队等待 TTL 到期，到期后经默认交换机精准死信回原主消费队列再次消费。
func retryQueueName(queue string, delay time.Duration) string {
	return fmt.Sprintf("%s.retry.%d", queue, delay.Milliseconds())
}

// dlqName 返回死信队列名称，与主消费队列一一对应。
// 存放多次重试仍失败的消息，避免阻塞正常消费，便于运维告警与人工处理。
func dlqName(queue string) string {
	return queue + ".dlq"
}

// ttlMilliseconds 把 retry delay 转成 RabbitMQ x-message-ttl 需要的毫秒整数。
func ttlMilliseconds(delay time.Duration) (int32, error) {
	milliseconds := delay.Milliseconds()
	if milliseconds <= 0 {
		return 0, fmt.Errorf("retry delay must be at least 1ms")
	}
	if milliseconds > math.MaxInt32 {
		return 0, fmt.Errorf("retry delay %s exceeds rabbitmq ttl limit", delay)
	}
	return int32(milliseconds), nil
}
