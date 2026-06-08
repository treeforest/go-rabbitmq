// Package mq 基于 RabbitMQ AMQP 0-9-1 提供服务间异步消息发布与订阅能力。
// 该文件实现 RabbitMQ 连接、拓扑、发布确认、手动 ack、retry queue 和 DLQ。
package mq

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
)

const (
	defaultModuleName = "mq.rabbitmq"

	retryCountHeader = "x-mq-retry-count"
)

// Client 表示基于 RabbitMQ 的消息队列客户端。
// Client 必须通过 NewClient 创建；零值不包含 AMQP 连接，不能直接使用。
type Client struct {
	conn      *amqp.Connection
	publishCh *amqp.Channel

	name           string
	exchange       string
	exchangeType   string
	publishTimeout time.Duration
	closeTimeout   time.Duration
	log            Logger

	publisher messagePublisher
	publishMu sync.Mutex

	mu     sync.Mutex
	subs   map[*subscription]struct{}
	closed bool
}

type messagePublisher interface {
	Publish(ctx context.Context, exchange, routingKey string, publishing amqp.Publishing, mandatory bool) error
}

type amqpPublisher struct {
	ch      *amqp.Channel
	returns <-chan amqp.Return
	timeout time.Duration
}

type subscriptionChannel interface {
	Cancel(consumer string, noWait bool) error
	Close() error
}

type lockedAcknowledger struct {
	mu   *sync.Mutex
	next amqp.Acknowledger
}

type subscription struct {
	channel      subscriptionChannel
	consumerTag  string
	stop         context.CancelFunc
	done         chan struct{}
	closeTimeout time.Duration
	channelMu    sync.Mutex
	once         sync.Once
	drainOnce    sync.Once
}

// NewClient 创建新的 RabbitMQ 客户端。
func NewClient(cfg Config) (*Client, error) {
	normalized, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}

	conn, err := amqp.DialConfig(normalized.URL, dialConfig(normalized))
	if err != nil {
		return nil, fmt.Errorf("create rabbitmq client failed: connect %s: %w", normalized.URL, err)
	}

	publishCh, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("create rabbitmq client failed: open publish channel: %w", err)
	}
	if err := declareExchange(publishCh, normalized.Exchange, normalized.ExchangeType); err != nil {
		_ = publishCh.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("create rabbitmq client failed: declare exchange %s: %w", normalized.Exchange, err)
	}
	if err := publishCh.Confirm(false); err != nil {
		_ = publishCh.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("create rabbitmq client failed: enable publisher confirm: %w", err)
	}

	return &Client{
		conn:           conn,
		publishCh:      publishCh,
		name:           normalized.Name,
		exchange:       normalized.Exchange,
		exchangeType:   normalized.ExchangeType,
		publishTimeout: normalized.PublishTimeout,
		closeTimeout:   normalized.CloseTimeout,
		log:            normalized.Logger,
		publisher:      newAMQPPublisher(publishCh, normalized.PublishTimeout),
		subs:           make(map[*subscription]struct{}),
	}, nil
}

// EnsureTopology 预声明订阅所需的 RabbitMQ 交换机、队列与路由绑定。
// 包含主 exchange、retry exchange、DLX，以及主队列、延迟重试队列、DLQ 和对应 binding。
// 不启动消费者；适用于先建队列再发布，避免 Subscribe 启动前消息无处落库的场景。
// Subscribe 内部也会调用本方法；对已存在的同名资源重复声明为幂等操作。
func (c *Client) EnsureTopology(ctx context.Context, opts SubscribeOptions) error {
	if c.isClosed() {
		return fmt.Errorf("ensure rabbitmq topology failed: client already closed")
	}
	if c.conn == nil {
		return fmt.Errorf("ensure rabbitmq topology failed: client is not initialized")
	}

	// 1. 规范化订阅参数，并校验 RetryDelay 可转换为 RabbitMQ x-message-ttl。
	normalized, err := NormalizeSubscribeOptions(opts)
	if err != nil {
		return fmt.Errorf("ensure rabbitmq topology failed: %w", err)
	}
	if _, err := ttlMilliseconds(normalized.RetryDelay); err != nil {
		return fmt.Errorf("ensure rabbitmq topology failed: %w", err)
	}

	// 2. 使用临时 channel 执行声明，避免长期占用连接上的 channel。
	ch, err := c.conn.Channel()
	if err != nil {
		return fmt.Errorf("ensure rabbitmq topology failed: open channel: %w", err)
	}
	defer func() {
		_ = ch.Close()
	}()

	// 3. 声明主/retry/DLQ 队列及 exchange binding。
	if err := c.declareTopology(ctx, ch, normalized); err != nil {
		return fmt.Errorf("ensure rabbitmq topology failed: %w", err)
	}
	return nil
}

// Publish 发布一条 RabbitMQ 消息，并等待 broker publisher confirm。
func (c *Client) Publish(ctx context.Context, routingKey string, data []byte, opts ...PublishOption) error {
	if c.isClosed() {
		return fmt.Errorf("publish rabbitmq message failed: client already closed")
	}

	normalizedRoutingKey, err := normalizePublishRoutingKey(routingKey)
	if err != nil {
		return fmt.Errorf("publish rabbitmq message failed: %w", err)
	}

	cfg := BuildPublishOptions(opts...)
	if cfg.MessageID == "" {
		cfg.MessageID = uuid.NewString()
	}

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

	c.log.Infof(
		"module=%s action=publish exchange=%s routing_key=%s message_id=%s",
		defaultModuleName,
		c.exchange,
		normalizedRoutingKey,
		cfg.MessageID,
	)
	return nil
}

// Subscribe 创建 RabbitMQ 持久化队列订阅，并使用 handler 处理消息。
func (c *Client) Subscribe(ctx context.Context, opts SubscribeOptions, handler Handler) (Subscription, error) {
	if handler == nil {
		return nil, fmt.Errorf("subscribe rabbitmq failed: handler is required")
	}
	if c.isClosed() {
		return nil, fmt.Errorf("subscribe rabbitmq failed: client already closed")
	}
	if c.conn == nil {
		return nil, fmt.Errorf("subscribe rabbitmq failed: client is not initialized")
	}

	normalized, err := NormalizeSubscribeOptions(opts)
	if err != nil {
		return nil, fmt.Errorf("subscribe rabbitmq failed: %w", err)
	}
	if err := c.EnsureTopology(ctx, normalized); err != nil {
		return nil, fmt.Errorf("subscribe rabbitmq failed: %w", err)
	}

	ch, err := c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("subscribe rabbitmq failed: open channel: %w", err)
	}
	if err := ch.Qos(normalized.Prefetch, 0, false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("subscribe rabbitmq failed: set qos queue=%s: %w", normalized.Queue, err)
	}

	subCtx, cancel := context.WithCancel(ctx)
	sub := &subscription{
		channel:      ch,
		consumerTag:  normalized.Queue,
		stop:         cancel,
		done:         make(chan struct{}),
		closeTimeout: c.closeTimeout,
	}

	deliveries, err := ch.Consume(normalized.Queue, normalized.Queue, false, false, false, false, nil)
	if err != nil {
		cancel()
		_ = ch.Close()
		return nil, fmt.Errorf("subscribe rabbitmq failed: consume queue=%s: %w", normalized.Queue, err)
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		cancel()
		_ = ch.Close()
		return nil, fmt.Errorf("subscribe rabbitmq failed: client already closed")
	}
	c.subs[sub] = struct{}{}
	c.mu.Unlock()

	go c.consumeLoop(subCtx, sub, normalized, deliveries, handler)

	c.log.Infof(
		"module=%s action=subscribe exchange=%s queue=%s routing_key=%s",
		defaultModuleName,
		c.exchange,
		normalized.Queue,
		normalized.RoutingKey,
	)
	return sub, nil
}

// Close 关闭客户端及其所有订阅。
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true

	subs := make([]*subscription, 0, len(c.subs))
	for sub := range c.subs {
		subs = append(subs, sub)
	}
	c.mu.Unlock()

	var group errgroup.Group
	for _, sub := range subs {
		current := sub
		group.Go(func() error {
			return current.Close()
		})
	}
	subErr := group.Wait()

	c.publishMu.Lock()
	chErr := closeAMQP(c.publishCh)
	connErr := closeAMQP(c.conn)
	c.publishMu.Unlock()

	if err := errors.Join(subErr, chErr, connErr); err != nil {
		return fmt.Errorf("close rabbitmq client failed: %w", err)
	}
	return nil
}

// Drain 优雅停止订阅，并等待已投递消息处理完成。
func (s *subscription) Drain(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	var cancelErr error
	s.drainOnce.Do(func() {
		cancelErr = s.cancelConsumer()
	})
	if cancelErr != nil {
		return fmt.Errorf("drain rabbitmq subscription failed: %w", cancelErr)
	}

	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("drain rabbitmq subscription failed: wait done: %w", ctx.Err())
	}
}

// Close 立即停止订阅并释放相关资源。
func (s *subscription) Close() error {
	var closeErr error
	s.once.Do(func() {
		s.drainOnce.Do(func() {
			closeErr = errors.Join(closeErr, s.cancelConsumer())
		})
		if s.stop != nil {
			s.stop()
		}
	})

	timeout := s.closeTimeout
	if timeout <= 0 {
		timeout = DefaultCloseTimeout
	}

	select {
	case <-s.done:
		if closeErr != nil {
			return fmt.Errorf("close rabbitmq subscription failed: %w", closeErr)
		}
		return nil
	case <-time.After(timeout):
		if err := s.closeChannel(); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
		if closeErr != nil {
			return fmt.Errorf("close rabbitmq subscription failed: %w", closeErr)
		}
		return fmt.Errorf("close rabbitmq subscription failed: wait done timeout")
	}
}

func (s *subscription) cancelConsumer() error {
	s.channelMu.Lock()
	defer s.channelMu.Unlock()

	if s.channel == nil {
		return nil
	}
	if err := s.channel.Cancel(s.consumerTag, false); err != nil && !isAMQPClosed(err) {
		return fmt.Errorf("cancel consumer=%s: %w", s.consumerTag, err)
	}
	return nil
}

func (s *subscription) closeChannel() error {
	s.channelMu.Lock()
	defer s.channelMu.Unlock()

	if s.channel == nil {
		return nil
	}
	err := closeAMQP(s.channel)
	s.channel = nil
	return err
}

func dialConfig(cfg Config) amqp.Config {
	properties := amqp.NewConnectionProperties()
	if cfg.Name != "" {
		properties.SetClientConnectionName(cfg.Name)
	}

	dialer := net.Dialer{Timeout: cfg.ConnectTimeout}
	return amqp.Config{
		Properties: properties,
		Dial:       dialer.Dial,
	}
}

// declareExchange 以持久化方式声明 exchange；重复声明相同属性是幂等操作。
func declareExchange(ch *amqp.Channel, name, kind string) error {
	return ch.ExchangeDeclare(name, kind, true, false, false, false, nil)
}

// declareTopology 在 broker 上声明单条订阅链路所需的交换机、队列与路由绑定。
// 消息流转：主 exchange -> 主队列消费；失败 -> retry exchange -> TTL 到期回主 exchange；
// 超过 MaxDeliver -> DLX -> DLQ。
func (c *Client) declareTopology(ctx context.Context, ch *amqp.Channel, opts SubscribeOptions) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("context canceled before declare topology: %w", ctx.Err())
	default:
	}

	// 1. 声明三条 exchange：正常投递、延迟重试转发、死信转发。
	if err := declareExchange(ch, c.exchange, c.exchangeType); err != nil {
		return fmt.Errorf("declare main exchange %s: %w", c.exchange, err)
	}
	if err := declareExchange(ch, retryExchangeName(c.exchange), c.exchangeType); err != nil {
		return fmt.Errorf("declare retry exchange %s: %w", retryExchangeName(c.exchange), err)
	}
	if err := declareExchange(ch, dlxExchangeName(c.exchange), c.exchangeType); err != nil {
		return fmt.Errorf("declare dlx exchange %s: %w", dlxExchangeName(c.exchange), err)
	}

	// 2. 声明主消费队列；持久化，供 Subscribe 的 consumer 拉取。
	if _, err := ch.QueueDeclare(opts.Queue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare queue %s: %w", opts.Queue, err)
	}

	// 3. 声明延迟重试队列；TTL 到期后经 dead-letter 回到主 exchange 再次投递。
	retryTTL, err := ttlMilliseconds(opts.RetryDelay)
	if err != nil {
		return err
	}
	retryQueue := retryQueueName(opts.Queue, opts.RetryDelay)
	retryArgs := amqp.Table{
		"x-message-ttl":          retryTTL,
		"x-dead-letter-exchange": c.exchange,
	}
	if _, err := ch.QueueDeclare(retryQueue, true, false, false, false, retryArgs); err != nil {
		return fmt.Errorf("declare retry queue %s: %w", retryQueue, err)
	}

	// 4. 声明死信队列；handler 失败且达到 MaxDeliver 后由 DLX 路由至此。
	dlq := dlqName(opts.Queue)
	if _, err := ch.QueueDeclare(dlq, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dlq %s: %w", dlq, err)
	}

	// 5. 按 BindingKeys 绑定三条链路，保证同一 routing key 可路由到主/retry/DLQ 队列。
	for _, bindingKey := range opts.BindingKeys {
		if err := ch.QueueBind(opts.Queue, bindingKey, c.exchange, false, nil); err != nil {
			return fmt.Errorf("bind queue %s key=%s: %w", opts.Queue, bindingKey, err)
		}
		if err := ch.QueueBind(retryQueue, bindingKey, retryExchangeName(c.exchange), false, nil); err != nil {
			return fmt.Errorf("bind retry queue %s key=%s: %w", retryQueue, bindingKey, err)
		}
		if err := ch.QueueBind(dlq, bindingKey, dlxExchangeName(c.exchange), false, nil); err != nil {
			return fmt.Errorf("bind dlq %s key=%s: %w", dlq, bindingKey, err)
		}
	}
	return nil
}

// consumeLoop 持续读取 RabbitMQ delivery，并把 ack/retry/DLQ 决策委托给 handleDelivery。
func (c *Client) consumeLoop(
	ctx context.Context,
	sub *subscription,
	opts SubscribeOptions,
	deliveries <-chan amqp.Delivery,
	handler Handler,
) {
	defer func() {
		_ = sub.closeChannel()
		close(sub.done)

		c.mu.Lock()
		delete(c.subs, sub)
		c.mu.Unlock()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case delivery, ok := <-deliveries:
			if !ok {
				return
			}
			delivery = sub.lockDeliveryAcknowledger(delivery)
			if err := c.handleDelivery(ctx, opts, delivery, handler); err != nil {
				c.log.Warnf(
					"consume failed: module=%s action=handle_message exchange=%s queue=%s routing_key=%s error=%v",
					defaultModuleName,
					c.exchange,
					opts.Queue,
					delivery.RoutingKey,
					err,
				)
			}
		}
	}
}

// lockDeliveryAcknowledger 锁定 delivery 的 ack/nack 权限，避免并发处理时重复 ack/nack。
func (s *subscription) lockDeliveryAcknowledger(delivery amqp.Delivery) amqp.Delivery {
	if delivery.Acknowledger == nil {
		return delivery
	}
	delivery.Acknowledger = &lockedAcknowledger{
		mu:   &s.channelMu,
		next: delivery.Acknowledger,
	}
	return delivery
}

func (a *lockedAcknowledger) Ack(tag uint64, multiple bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.next.Ack(tag, multiple)
}

func (a *lockedAcknowledger) Nack(tag uint64, multiple, requeue bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.next.Nack(tag, multiple, requeue)
}

func (a *lockedAcknowledger) Reject(tag uint64, requeue bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.next.Reject(tag, requeue)
}

// handleDelivery 执行业务 handler，并根据处理结果确认消息或进入失败处理。
func (c *Client) handleDelivery(ctx context.Context, opts SubscribeOptions, delivery amqp.Delivery, handler Handler) error {
	message := toMessage(c.exchange, opts.Queue, delivery)

	err := handler(ctx, message)

	// 处理成功，直接 ack 消息
	if err == nil {
		if ackErr := delivery.Ack(false); ackErr != nil {
			return fmt.Errorf("ack rabbitmq message failed: queue=%s routing_key=%s: %w", opts.Queue, delivery.RoutingKey, ackErr)
		}
		return nil
	} 
	
	// 处理失败，且上下文取消，直接 nack 消息，由消息队列重新投递
	if ctx.Err() != nil {
		if nackErr := delivery.Nack(false, true); nackErr != nil {
			return fmt.Errorf("nack canceled rabbitmq message failed: queue=%s routing_key=%s: %w", opts.Queue, delivery.RoutingKey, nackErr)
		}
		return fmt.Errorf("handle rabbitmq message canceled: %w", ctx.Err())
	} 
	
	// 处理失败，投递到 retry exchange 或 DLX
	if retryErr := c.handleFailure(ctx, opts, delivery, err); retryErr != nil {
		return retryErr
	}

	return nil
}

// handleFailure 将失败消息转发到 retry exchange 或 DLX，成功转发后再 ack 原消息。
func (c *Client) handleFailure(ctx context.Context, opts SubscribeOptions, delivery amqp.Delivery, handlerErr error) error {
	nextRetryCount := retryCountFromHeaders(delivery.Headers) + 1
	
	// 重试次数小于 MaxDeliver，转发到 retry exchange
	targetExchange := retryExchangeName(c.exchange)

	// 重试次数大于等于 MaxDeliver，转发到 DLX
	if nextRetryCount >= opts.MaxDeliver {
		targetExchange = dlxExchangeName(c.exchange)
	}

	// 转发消息
	publishing := publishingFromDelivery(delivery, nextRetryCount)
	if err := c.publishMessage(ctx, targetExchange, delivery.RoutingKey, publishing, true); err != nil {
		if nackErr := delivery.Nack(false, true); nackErr != nil {
			return fmt.Errorf(
				"handle rabbitmq message failed: %w; publish failure target failed: %v; nack requeue failed: %v",
				handlerErr,
				err,
				nackErr,
			)
		}
		return fmt.Errorf("handle rabbitmq message failed: %w; publish failure target failed: %v", handlerErr, err)
	}

	// 转发成功，ack 原消息
	if ackErr := delivery.Ack(false); ackErr != nil {
		return fmt.Errorf("ack failed rabbitmq message failed after retry publish: %w", ackErr)
	}
	return fmt.Errorf("handle rabbitmq message failed: %w", handlerErr)
}

func (c *Client) publishMessage(ctx context.Context, exchange, routingKey string, publishing amqp.Publishing, mandatory bool) error {
	c.publishMu.Lock()
	defer c.publishMu.Unlock()

	if c.isClosed() {
		return fmt.Errorf("client already closed")
	}
	if c.publisher == nil {
		return fmt.Errorf("client is not initialized")
	}
	return c.publisher.Publish(ctx, exchange, routingKey, publishing, mandatory)
}

// newAMQPPublisher 创建基于 AMQP channel 的发布器，集中处理 confirm 和 mandatory return。
func newAMQPPublisher(ch *amqp.Channel, timeout time.Duration) *amqpPublisher {
	return &amqpPublisher{
		ch:      ch,
		returns: ch.NotifyReturn(make(chan amqp.Return, 1)),
		timeout: timeout,
	}
}

// Publish 串行发布消息，并等待当前消息专属的 publisher confirm。
// RabbitMQ 对 mandatory 不可路由消息会先发送 basic.return，再发送 confirm；
// 因此这里在发布前清理旧 return，confirm 后再读取 return，避免把前一次发布结果串到当前消息。
func (p *amqpPublisher) Publish(ctx context.Context, exchange, routingKey string, publishing amqp.Publishing, mandatory bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if p.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.timeout)
		defer cancel()
	}

	p.drainReturns()

	confirm, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, mandatory, false, publishing)
	if err != nil {
		return fmt.Errorf("publish exchange=%s routing_key=%s: %w", exchange, routingKey, err)
	}
	if confirm == nil {
		return fmt.Errorf("publish exchange=%s routing_key=%s: publisher confirm is not enabled", exchange, routingKey)
	}

	acked, err := confirm.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("wait publish confirm failed: %w", err)
	}
	if returned := p.popReturned(); returned != nil {
		return publishReturnError(*returned)
	}
	if !acked {
		return fmt.Errorf("message nacked by broker: exchange=%s routing_key=%s", exchange, routingKey)
	}
	return nil
}

// drainReturns 清空旧的 mandatory return，避免后续发布误读历史不可路由结果。
func (p *amqpPublisher) drainReturns() {
	for {
		select {
		case _, ok := <-p.returns:
			if !ok {
				return
			}
		default:
			return
		}
	}
}

// popReturned 尝试读取当前发布产生的 mandatory return；没有 return 表示消息已成功路由。
func (p *amqpPublisher) popReturned() *amqp.Return {
	select {
	case returned, ok := <-p.returns:
		if !ok {
			return &amqp.Return{ReplyText: "rabbitmq return channel closed"}
		}
		return &returned
	default:
		return nil
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

// toMessage 把 AMQP delivery 转成对业务暴露的消息模型，并保留常用路由字段。
func toMessage(exchange, queue string, delivery amqp.Delivery) Message {
	headers := tableToHeaders(delivery.Headers)
	messageID := strings.TrimSpace(delivery.MessageId)
	if messageID == "" {
		messageID = fmt.Sprintf("%s:%d", delivery.RoutingKey, delivery.DeliveryTag)
	}

	return Message{
		Exchange:    exchange,
		Queue:       queue,
		RoutingKey:  delivery.RoutingKey,
		Data:        delivery.Body,
		Headers:     headers,
		ID:          messageID,
		PublishedAt: delivery.Timestamp,
	}
}

// publishingFromDelivery 复制原消息属性并递增 retry count，用于失败后转发到 retry/DLQ。
func publishingFromDelivery(delivery amqp.Delivery, retryCount int) amqp.Publishing {
	headers := cloneTable(delivery.Headers)
	headers[retryCountHeader] = int32(retryCount)

	return amqp.Publishing{
		Headers:         headers,
		ContentType:     delivery.ContentType,
		ContentEncoding: delivery.ContentEncoding,
		DeliveryMode:    amqp.Persistent,
		Priority:        delivery.Priority,
		CorrelationId:   delivery.CorrelationId,
		ReplyTo:         delivery.ReplyTo,
		MessageId:       delivery.MessageId,
		Timestamp:       delivery.Timestamp,
		Type:            delivery.Type,
		AppId:           delivery.AppId,
		Body:            delivery.Body,
	}
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

// retryExchangeName 返回延迟重试 exchange 名称。
// handler 返回错误且未达 MaxDeliver 时，消息发布到此 exchange，再路由进 retry 队列等待重投。
func retryExchangeName(exchange string) string {
	return exchange + ".retry"
}

// dlxExchangeName 返回死信 exchange 名称。
// handler 失败且已达 MaxDeliver 时，消息发布到此 exchange，再路由进 DLQ 供人工排查或补偿。
func dlxExchangeName(exchange string) string {
	return exchange + ".dlx"
}

// retryQueueName 返回延迟重试队列名称；按 RetryDelay 隔离，避免不同延迟策略互相影响。
// 消息在此排队等待 TTL 到期，到期后由 dead-letter 回到主 exchange 再次消费。
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
		return 0, fmt.Errorf("retry delay must be greater than 0")
	}
	if milliseconds > math.MaxInt32 {
		return 0, fmt.Errorf("retry delay %s exceeds rabbitmq ttl limit", delay)
	}
	return int32(milliseconds), nil
}

// headersToTable 把公共 string header 转成 AMQP table。
func headersToTable(headers map[string]string) amqp.Table {
	table := make(amqp.Table, len(headers))
	for key, value := range headers {
		table[key] = value
	}
	return table
}

// tableToHeaders 把 AMQP table 转成公共 string header，复杂类型按字符串表达。
func tableToHeaders(table amqp.Table) map[string]string {
	headers := make(map[string]string, len(table))
	for key, value := range table {
		switch typed := value.(type) {
		case string:
			headers[key] = typed
		case []byte:
			headers[key] = string(typed)
		case fmt.Stringer:
			headers[key] = typed.String()
		default:
			headers[key] = fmt.Sprint(typed)
		}
	}
	return headers
}

// cloneTable 复制 AMQP table，避免修改原 delivery header。
func cloneTable(table amqp.Table) amqp.Table {
	cloned := make(amqp.Table, len(table)+1)
	for key, value := range table {
		cloned[key] = value
	}
	return cloned
}

// retryCountFromHeaders 兼容解析 RabbitMQ header 中的 retry count。
func retryCountFromHeaders(table amqp.Table) int {
	value, ok := table[retryCountHeader]
	if !ok {
		return 0
	}

	switch typed := value.(type) {
	case int:
		return typed
	case int8:
		return int(typed)
	case int16:
		return int(typed)
	case int32:
		return int(typed)
	case int64:
		return int(typed)
	case uint16:
		return int(typed)
	case uint32:
		return int(typed)
	case string:
		parsed, err := strconv.Atoi(typed)
		if err != nil {
			return 0
		}
		return parsed
	default:
		return 0
	}
}

// closeAMQP 关闭 AMQP 资源，并忽略已经关闭的正常状态。
func closeAMQP(closer interface{ Close() error }) error {
	if closer == nil {
		return nil
	}
	value := reflect.ValueOf(closer)
	if value.Kind() == reflect.Ptr && value.IsNil() {
		return nil
	}
	if err := closer.Close(); err != nil && !isAMQPClosed(err) {
		return err
	}
	return nil
}

// isAMQPClosed 判断错误是否表示 AMQP 资源已经关闭。
func isAMQPClosed(err error) bool {
	return errors.Is(err, amqp.ErrClosed) || err == amqp.ErrClosed
}

// isClosed 返回客户端是否已经关闭。
func (c *Client) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}
