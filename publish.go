package mq

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

// messagePublisher 抽象底层消息发布器，便于在单元测试中替换真实 AMQP 通道。
type messagePublisher interface {
	Publish(ctx context.Context, exchange, routingKey string, publishing amqp.Publishing, mandatory bool) error
}

// amqpPublisher 封装单个启用了 publisher confirm 的 AMQP 发布通道。
// 通过 sendMu 串行化底层写帧，允许多个调用方在锁外并发流水线等待各自消息的 Confirm；
// 通过 waitersMu 与 waiters 按内部序号 x-mq-publish-seq 精确分发 mandatory basic.return。
type amqpPublisher struct {
	ch            amqpChannel
	returns       <-chan amqp.Return
	timeout       time.Duration
	sendMu        sync.Mutex
	nextSeq       atomic.Uint64
	waitersMu     sync.Mutex
	waiters       map[uint64]chan amqp.Return
	returnsClosed bool
}

// publishOutcomeError 标记发布错误是否已把消息提交给 broker。
// submitted=false 表示请求未发出，可在重连后安全重试；
// submitted=true 表示结果不确定，不能自动补发。
type publishOutcomeError struct {
	err       error
	submitted bool
}

func (e *publishOutcomeError) Error() string {
	if e == nil || e.err == nil {
		return "publish outcome error"
	}
	if e.submitted {
		return fmt.Sprintf("delivery outcome unknown: %v", e.err)
	}
	return e.err.Error()
}

func (e *publishOutcomeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

// Publish 发布一条 RabbitMQ 消息，并等待 broker publisher confirm。
// 入参 routingKey 表示该条消息的具体路由标签（不可包含 * 或 # 通配符）；订阅端则在 SubscribeOptions.BindingKeys 中配置匹配规则。
func (c *Client) Publish(ctx context.Context, routingKey string, data []byte, opts ...PublishOption) error {
	// 1. 检查客户端关闭状态，快速拒绝已关闭客户端的新发布请求。
	if c.isClosed() {
		return fmt.Errorf("publish rabbitmq message failed: client already closed")
	}

	// 2. 按当前 exchange 类型校验并规范化发布 routing key（禁止通配符）。
	normalizedRoutingKey, err := normalizePublishRoutingKeyForExchange(routingKey, c.exchangeType)
	if err != nil {
		return fmt.Errorf("publish rabbitmq message failed: %w", err)
	}

	// 3. 解析函数式发布选项；若未指定 MessageID 则自动生成 UUID 供消费端幂等使用。
	cfg := buildPublishOptions(opts...)
	if cfg.MessageID == "" {
		cfg.MessageID = uuid.NewString()
	}

	// 4. 组装持久化 AMQP 消息体并执行带重连恢复的发布确认流程。
	publishing := amqp.Publishing{
		Headers:      headersToTable(cfg.Headers),
		DeliveryMode: amqp.Persistent,
		MessageId:    cfg.MessageID,
		Timestamp:    time.Now(),
		AppId:        c.name,
		Body:         data,
	}
	if err := c.publishMessage(ctx, c.exchange, normalizedRoutingKey, publishing, cfg.Mandatory); err != nil {
		return fmt.Errorf("publish rabbitmq message failed: exchange=%s routing_key=%s: %w", c.exchange, normalizedRoutingKey, err)
	}

	return nil
}

// publishMessage 执行消息发布并在满足安全条件（消息尚未写出且遇到传输层故障）时自动重连重试一次。
func (c *Client) publishMessage(ctx context.Context, exchange, routingKey string, publishing amqp.Publishing, mandatory bool) error {
	// 1. 发送前确保底层连接与 publish channel 处于就绪状态；若已断开则触发单飞重连。
	if c.hasTransportConfig() {
		if err := c.ensureConnected(ctx); err != nil {
			return err
		}
	}

	// 2. 执行发布；仅当请求尚未写出到网络（submitted=false）且发生传输故障时，重连并安全重试一次。
	retried := false
	for {
		err := c.publishOnce(ctx, exchange, routingKey, publishing, mandatory)
		if err == nil {
			return nil
		}

		// 3. 若错误携带投递状态标记：已提交（submitted=true）说明结果未知，仅标记连接失效并直接返回，绝不自动重发。
		var outcome *publishOutcomeError
		if errors.As(err, &outcome) {
			if outcome.submitted {
				c.invalidateTransport()
				return err
			}
			if !retried && isTransportFailure(outcome.err) {
				if c.hasTransportConfig() {
					c.invalidateTransport()
					if reconnErr := c.ensureConnected(ctx); reconnErr != nil {
						return outcome.err
					}
				}
				retried = true
				continue
			}
			return outcome.err
		}

		// 4. 普通未提交传输层故障同样允许触发一次重连与安全重发。
		if !retried && isTransportFailure(err) {
			if c.hasTransportConfig() {
				c.invalidateTransport()
				if reconnErr := c.ensureConnected(ctx); reconnErr != nil {
					return err
				}
			}
			retried = true
			continue
		}
		return err
	}
}

// publishOnce 获取当前就绪的 publisher 快照并执行单次发布。
func (c *Client) publishOnce(ctx context.Context, exchange, routingKey string, publishing amqp.Publishing, mandatory bool) error {
	c.publishMu.Lock()
	if c.isClosed() {
		c.publishMu.Unlock()
		return fmt.Errorf("client already closed")
	}
	publisher := c.publisher
	if publisher == nil {
		c.publishMu.Unlock()
		return fmt.Errorf("client is not initialized")
	}
	if c.hasTransportConfig() && !c.transportReady() {
		c.publishMu.Unlock()
		return &publishOutcomeError{err: amqp.ErrClosed, submitted: false}
	}
	c.publishMu.Unlock()

	return publisher.Publish(ctx, exchange, routingKey, publishing, mandatory)
}

// newAMQPPublisher 创建基于 AMQP channel 的发布器，集中处理 confirm 和 mandatory return。
func newAMQPPublisher(ch amqpChannel, timeout time.Duration) *amqpPublisher {
	return &amqpPublisher{
		ch:      ch,
		returns: ch.NotifyReturn(make(chan amqp.Return, 256)),
		timeout: timeout,
		waiters: make(map[uint64]chan amqp.Return),
	}
}

// Publish 发送消息并等待当前消息专属的 publisher confirm。
// 发送帧通过 sendMu 串行化，所有消息（含 mandatory）均在锁外并发流水线等待 confirm；
// mandatory 消息通过内部序号头 x-mq-publish-seq 精确匹配各自的 basic.return，避免跨请求串扰。
func (p *amqpPublisher) Publish(ctx context.Context, exchange, routingKey string, publishing amqp.Publishing, mandatory bool) error {
	// 1. 绑定单次发布等待 confirm 的超时上下文。
	if ctx == nil {
		ctx = context.Background()
	}
	if p.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.timeout)
		defer cancel()
	}

	// 2. 若启用 mandatory，分配唯一发布序号并注册 return 等待通道，将序号注入克隆后的消息头供回退精确匹配。
	var seq uint64
	var retCh chan amqp.Return
	if mandatory {
		seq, retCh = p.registerReturnWaiter()
		defer p.unregisterReturnWaiter(seq)

		publishing.Headers = cloneTable(publishing.Headers)
		publishing.Headers[publishSeqHeader] = strconv.FormatUint(seq, 10)
	}

	// 3. 在 sendMu 保护下将 AMQP 发布帧写入通道，并获取当前消息专属的 DeferredConfirmation。
	confirm, err := p.sendPublishing(ctx, exchange, routingKey, publishing, mandatory)
	if err != nil {
		return err
	}

	// 4. 在锁外并发等待 Broker 返回 basic.ack/basic.nack；超时或断连时标记 submitted=true（结果未知）。
	acked, err := confirm.WaitContext(ctx)
	if err != nil {
		if isTransportFailure(err) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return &publishOutcomeError{err: err, submitted: true}
		}
		return fmt.Errorf("wait publish confirm failed: %w", err)
	}
	// 5. RabbitMQ 协议保证不可路由的 basic.return 先于 basic.ack 到达，排空并检查当前序号是否被退回。
	if mandatory {
		if returned := p.takeReturn(retCh); returned != nil {
			return publishReturnError(*returned)
		}
	}
	if !acked {
		return fmt.Errorf("message nacked by broker: exchange=%s routing_key=%s", exchange, routingKey)
	}
	return nil
}

// sendPublishing 在 sendMu 保护下串行调用底层 PublishWithDeferredConfirmWithContext。
func (p *amqpPublisher) sendPublishing(
	ctx context.Context,
	exchange, routingKey string,
	publishing amqp.Publishing,
	mandatory bool,
) (publishConfirm, error) {
	p.sendMu.Lock()
	defer p.sendMu.Unlock()

	if p.ch == nil || p.ch.IsClosed() {
		return nil, &publishOutcomeError{err: amqp.ErrClosed, submitted: false}
	}
	confirm, err := p.ch.PublishDeferred(ctx, exchange, routingKey, mandatory, false, publishing)
	if err != nil {
		if isTransportFailure(err) {
			return nil, &publishOutcomeError{err: err, submitted: false}
		}
		return nil, fmt.Errorf("publish exchange=%s routing_key=%s: %w", exchange, routingKey, err)
	}
	if confirm == nil {
		return nil, fmt.Errorf("publish exchange=%s routing_key=%s: publisher confirm is not enabled", exchange, routingKey)
	}
	return confirm, nil
}

// registerReturnWaiter 分配递增发布序号并注册对应的 basic.return 接收通道。
func (p *amqpPublisher) registerReturnWaiter() (uint64, chan amqp.Return) {
	seq := p.nextSeq.Add(1)
	ch := make(chan amqp.Return, 1)

	p.waitersMu.Lock()
	defer p.waitersMu.Unlock()
	if p.waiters == nil {
		p.waiters = make(map[uint64]chan amqp.Return)
	}
	p.flushReturnsLocked()
	p.waiters[seq] = ch
	return seq, ch
}

// unregisterReturnWaiter 注销指定发布序号的等待通道，并顺带排空底层待处理的 return 事件。
func (p *amqpPublisher) unregisterReturnWaiter(seq uint64) {
	p.waitersMu.Lock()
	defer p.waitersMu.Unlock()
	delete(p.waiters, seq)
	p.flushReturnsLocked()
}

// takeReturn 排空底层 returns 通道并读取当前发布序号匹配的 mandatory return；没有 return 表示消息已成功路由。
func (p *amqpPublisher) takeReturn(retCh <-chan amqp.Return) *amqp.Return {
	p.waitersMu.Lock()
	p.flushReturnsLocked()
	closed := p.returnsClosed
	p.waitersMu.Unlock()

	select {
	case returned := <-retCh:
		return &returned
	default:
		if closed {
			return &amqp.Return{ReplyText: "rabbitmq return channel closed"}
		}
		return nil
	}
}

// flushReturnsLocked 将底层 returns 通道中的 basic.return 按序号路由到对应 waiter；
// 已超时注销或无匹配序号的旧 return 会被安全丢弃，不会污染其它并发请求。
func (p *amqpPublisher) flushReturnsLocked() {
	if p.returns == nil || p.returnsClosed {
		return
	}
	for {
		select {
		case returned, ok := <-p.returns:
			if !ok {
				p.returnsClosed = true
				return
			}
			seq, hasSeq := publishSeqFromHeaders(returned.Headers)
			if !hasSeq {
				continue
			}
			if ch, exists := p.waiters[seq]; exists {
				select {
				case ch <- returned:
				default:
				}
			}
		default:
			return
		}
	}
}

// publishReturnError 把 RabbitMQ basic.return 转成带路由上下文的错误。
func publishReturnError(returned amqp.Return) error {
	return fmt.Errorf(
		"message returned by broker: exchange=%s routing_key=%s reply_code=%d reply_text=%s",
		returned.Exchange,
		returned.RoutingKey,
		returned.ReplyCode,
		returned.ReplyText,
	)
}

// normalizePublishRoutingKey 校验发布 routing key；发布端必须使用具体路由键，不能使用通配符。
func normalizePublishRoutingKey(routingKey string) (string, error) {
	routingKey = strings.TrimSpace(routingKey)
	if routingKey == "" {
		return "", fmt.Errorf("routing key is required")
	}
	if strings.ContainsAny(routingKey, "*#>") {
		return "", fmt.Errorf("routing key %q must not contain wildcard", routingKey)
	}
	return routingKey, nil
}

// normalizePublishRoutingKeyForExchange 按交换机类型校验并规范化发布端 routing key。
func normalizePublishRoutingKeyForExchange(routingKey, exchangeType string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(exchangeType)) {
	case "fanout":
		return strings.TrimSpace(routingKey), nil
	case "direct":
		trimmed := strings.TrimSpace(routingKey)
		if trimmed == "" {
			return "", fmt.Errorf("routing key is required")
		}
		return trimmed, nil
	default:
		return normalizePublishRoutingKey(routingKey)
	}
}
