// Package mq 基于 RabbitMQ AMQP 0-9-1 提供服务间异步消息发布与订阅能力。
// 该文件实现 RabbitMQ 连接、拓扑、发布确认、手动 ack、retry queue、DLQ 与透明重连。
package mq

import (
	"context"
	crand "crypto/rand"
	"math/big"
	"errors"
	"fmt"
	"maps"
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

// dialAMQP 允许测试替换 Dial 实现；生产路径默认调用 amqp.DialConfig。
var dialAMQP = amqp.DialConfig

// Client 表示基于 RabbitMQ 的消息队列客户端。
// Client 必须通过 NewClient 创建；零值不包含 AMQP 连接，不能直接使用。
// 连接或 channel 被动关闭后，Client 会按需透明重连并恢复发布与订阅。
type Client struct {
	cfg Config

	name           string
	exchange       string
	exchangeType   string
	publishTimeout time.Duration
	closeTimeout   time.Duration
	log            Logger

	publisher messagePublisher
	publishMu sync.Mutex

	reconnectMu   sync.Mutex
	conn          *amqp.Connection
	publishCh     *amqp.Channel
	generation    uint64
	broken        bool
	reconnectWait chan struct{}
	reconnectErr  error

	lifeCtx    context.Context
	lifeCancel context.CancelFunc

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

type subscriptionChannel interface {
	Cancel(consumer string, noWait bool) error
	Close() error
}

type lockedAcknowledger struct {
	mu   *sync.Mutex
	next amqp.Acknowledger
}

type subscription struct {
	client       *Client
	opts         SubscribeOptions
	handler      Handler
	parentCtx    context.Context
	channel      subscriptionChannel
	consumerTag  string
	runCancel    context.CancelFunc
	loopCancel   context.CancelFunc
	done         chan struct{}
	stopCh       chan struct{}
	closeTimeout time.Duration
	channelMu    sync.Mutex
	once         sync.Once
	drainOnce    sync.Once

	stopMu  sync.Mutex
	stopped bool
}

// NewClient 创建新的 RabbitMQ 客户端。
func NewClient(cfg Config) (*Client, error) {
	normalized, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}

	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	client := &Client{
		cfg:            normalized,
		name:           normalized.Name,
		exchange:       normalized.Exchange,
		exchangeType:   normalized.ExchangeType,
		publishTimeout: normalized.PublishTimeout,
		closeTimeout:   normalized.CloseTimeout,
		log:            normalized.Logger,
		lifeCtx:        lifeCtx,
		lifeCancel:     lifeCancel,
		subs:           make(map[*subscription]struct{}),
	}

	if err := client.establishTransport(); err != nil {
		lifeCancel()
		return nil, fmt.Errorf("create rabbitmq client failed: %w", err)
	}
	return client, nil
}

// EnsureTopology 预声明订阅所需的 RabbitMQ 交换机、队列与路由绑定。
// 包含主 exchange、retry exchange、DLX，以及主队列、延迟重试队列、DLQ 和对应 binding。
// 不启动消费者；适用于先建队列再发布，避免 Subscribe 启动前消息无处落库的场景。
// Subscribe 内部也会调用本方法；对已存在的同名资源重复声明为幂等操作。
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
	normalized, err := NormalizeSubscribeOptions(opts)
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
// 返回的 Subscription 在连接被动断开后会自动恢复消费，无需调用方重新 Subscribe。
// 首次拓扑声明、Qos 和 Consume 在返回前同步完成；失败时直接返回错误。
func (c *Client) Subscribe(ctx context.Context, opts SubscribeOptions, handler Handler) (Subscription, error) {
	if handler == nil {
		return nil, fmt.Errorf("subscribe rabbitmq failed: handler is required")
	}
	if c.isClosed() {
		return nil, fmt.Errorf("subscribe rabbitmq failed: client already closed")
	}
	if !c.hasTransportConfig() {
		return nil, fmt.Errorf("subscribe rabbitmq failed: client is not initialized")
	}

	normalized, err := NormalizeSubscribeOptions(opts)
	if err != nil {
		return nil, fmt.Errorf("subscribe rabbitmq failed: %w", err)
	}
	if err := c.EnsureTopology(ctx, normalized); err != nil {
		return nil, fmt.Errorf("subscribe rabbitmq failed: %w", err)
	}

	runCtx, runCancel := context.WithCancel(ctx)
	sub := &subscription{
		client:       c,
		opts:         normalized,
		handler:      handler,
		parentCtx:    runCtx,
		runCancel:    runCancel,
		consumerTag:  normalized.Queue,
		done:         make(chan struct{}),
		stopCh:       make(chan struct{}),
		closeTimeout: c.closeTimeout,
	}

	ch, deliveries, err := c.startConsume(sub)
	if err != nil {
		runCancel()
		return nil, fmt.Errorf("subscribe rabbitmq failed: %w", err)
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		runCancel()
		discardCloseErr(ch)
		return nil, fmt.Errorf("subscribe rabbitmq failed: client already closed")
	}
	c.subs[sub] = struct{}{}
	c.mu.Unlock()

	sub.setChannel(ch)
	go c.runSubscription(sub, deliveries)

	c.log.Infof(
		"module=%s action=subscribe exchange=%s queue=%s routing_key=%s",
		defaultModuleName,
		c.exchange,
		normalized.Queue,
		normalized.RoutingKey,
	)
	return sub, nil
}

// Close 关闭客户端及其所有订阅，并停止后续重连。
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

	if c.lifeCancel != nil {
		c.lifeCancel()
	}

	var group errgroup.Group
	for _, sub := range subs {
		current := sub
		group.Go(func() error {
			return current.Close()
		})
	}
	subErr := group.Wait()

	c.publishMu.Lock()
	c.reconnectMu.Lock()
	chErr := closeAMQP(c.publishCh)
	connErr := closeAMQP(c.conn)
	c.publishCh = nil
	c.conn = nil
	c.publisher = nil
	c.broken = true
	c.reconnectMu.Unlock()
	c.publishMu.Unlock()

	if err := errors.Join(subErr, chErr, connErr); err != nil {
		return fmt.Errorf("close rabbitmq client failed: %w", err)
	}
	return nil
}

// Drain 优雅停止订阅，并等待已投递消息处理完成。
// Drain 只停止接收新消息并禁止自动恢复，不会取消正在执行的 handler context。
func (s *subscription) Drain(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	s.markStopped()

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
// Close 会取消订阅运行 context，打断重连等待与当前消费循环。
func (s *subscription) Close() error {
	var closeErr error
	s.once.Do(func() {
		s.markStopped()
		s.cancelRunning()
		s.drainOnce.Do(func() {
			closeErr = errors.Join(closeErr, s.cancelConsumer())
		})
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

// markStopped 标记订阅不再自动恢复，并唤醒可能阻塞在重连等待中的监督循环。
// 不会取消 handler 使用的运行 context；立即取消由 cancelRunning 负责。
func (s *subscription) markStopped() {
	s.stopMu.Lock()
	if s.stopped {
		s.stopMu.Unlock()
		return
	}
	s.stopped = true
	if s.stopCh != nil {
		close(s.stopCh)
	}
	s.stopMu.Unlock()
}

// cancelRunning 取消订阅运行 context 与当前消费循环，供 Close 使用。
func (s *subscription) cancelRunning() {
	s.stopMu.Lock()
	loopCancel := s.loopCancel
	runCancel := s.runCancel
	s.stopMu.Unlock()
	if loopCancel != nil {
		loopCancel()
	}
	if runCancel != nil {
		runCancel()
	}
}

func (s *subscription) isStopped() bool {
	s.stopMu.Lock()
	defer s.stopMu.Unlock()
	return s.stopped
}

func (s *subscription) setLoopCancel(cancel context.CancelFunc) {
	s.stopMu.Lock()
	defer s.stopMu.Unlock()
	s.loopCancel = cancel
	// 仅在 Close 已取消 parentCtx 时立即取消新循环，避免 Drain 误杀 in-flight handler。
	if s.parentCtx != nil && s.parentCtx.Err() != nil {
		cancel()
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

func (s *subscription) setChannel(ch *amqp.Channel) {
	s.channelMu.Lock()
	defer s.channelMu.Unlock()
	s.channel = ch
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

func (c *Client) hasTransportConfig() bool {
	return strings.TrimSpace(c.cfg.URL) != ""
}

// ensureConnected 确保当前 connection/publish channel 可用；失效时单飞重连。
func (c *Client) ensureConnected(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if c.isClosed() {
		return fmt.Errorf("client already closed")
	}
	if !c.hasTransportConfig() {
		return fmt.Errorf("client is not initialized")
	}

	for {
		if c.transportReady() {
			return nil
		}

		wait, err := c.beginOrJoinReconnect()
		if err != nil {
			return err
		}
		if wait == nil {
			// 当前调用方负责执行重连。
			err = c.reconnectWithBackoff(ctx)
			c.finishReconnect(err)
			if err != nil {
				return err
			}
			if c.transportReady() {
				return nil
			}
			continue
		}

		select {
		case <-wait:
			c.reconnectMu.Lock()
			err = c.reconnectErr
			c.reconnectMu.Unlock()
			if err != nil {
				return err
			}
			if c.transportReady() {
				return nil
			}
		case <-ctx.Done():
			return fmt.Errorf("wait rabbitmq reconnect: %w", ctx.Err())
		case <-c.lifeCtx.Done():
			return fmt.Errorf("client already closed")
		}
	}
}

func (c *Client) transportReady() bool {
	c.reconnectMu.Lock()
	defer c.reconnectMu.Unlock()
	return c.conn != nil && !c.conn.IsClosed() &&
		c.publishCh != nil && !c.publishCh.IsClosed() &&
		c.publisher != nil && !c.broken
}

func (c *Client) beginOrJoinReconnect() (<-chan struct{}, error) {
	c.reconnectMu.Lock()
	defer c.reconnectMu.Unlock()

	select {
	case <-c.lifeCtx.Done():
		return nil, fmt.Errorf("client already closed")
	default:
	}
	if c.reconnectWait != nil {
		return c.reconnectWait, nil
	}
	c.reconnectWait = make(chan struct{})
	c.reconnectErr = nil
	return nil, nil
}

func (c *Client) finishReconnect(err error) {
	c.reconnectMu.Lock()
	defer c.reconnectMu.Unlock()
	c.reconnectErr = err
	if c.reconnectWait != nil {
		close(c.reconnectWait)
		c.reconnectWait = nil
	}
}

func (c *Client) reconnectWithBackoff(ctx context.Context) error {
	delay := defaultReconnectInitial
	var lastErr error
	for {
		if c.isClosed() {
			return fmt.Errorf("client already closed")
		}
		if err := c.establishTransport(); err == nil {
			return nil
		} else {
			lastErr = err
			c.log.Warnf(
				"module=%s action=reconnect status=failed error=%v next_retry=%s",
				defaultModuleName,
				err,
				delay,
			)
		}

		timer := time.NewTimer(reconnectDelayWithJitter(delay))
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			if lastErr != nil {
				return fmt.Errorf("reconnect rabbitmq failed: %w: %w", ctx.Err(), lastErr)
			}
			return fmt.Errorf("reconnect rabbitmq failed: %w", ctx.Err())
		case <-c.lifeCtx.Done():
			timer.Stop()
			return fmt.Errorf("client already closed")
		}

		if delay < defaultReconnectMax {
			delay *= 2
			if delay > defaultReconnectMax {
				delay = defaultReconnectMax
			}
		}
	}
}

// establishTransport 建立或修复 AMQP connection 与 publish channel。
// 若连接仍可用则仅重建 publish channel；否则重新 Dial。
func (c *Client) establishTransport() error {
	c.reconnectMu.Lock()
	conn := c.conn
	oldCh := c.publishCh
	c.reconnectMu.Unlock()

	if conn != nil && !conn.IsClosed() {
		pubCh, publisher, err := openPublishChannel(conn, c.exchange, c.exchangeType, c.publishTimeout)
		if err == nil {
			return c.installTransport(conn, pubCh, publisher, oldCh, false)
		}
		c.log.Warnf(
			"module=%s action=reconnect status=reopen_publish_channel_failed error=%v",
			defaultModuleName,
			err,
		)
	}

	newConn, err := dialAMQP(c.cfg.URL, dialConfig(c.cfg))
	if err != nil {
		return fmt.Errorf("connect %s: %w", c.cfg.URL, err)
	}

	pubCh, publisher, err := openPublishChannel(newConn, c.exchange, c.exchangeType, c.publishTimeout)
	if err != nil {
		discardCloseErr(newConn)
		return err
	}

	return c.installTransport(newConn, pubCh, publisher, oldCh, true)
}

func openPublishChannel(conn *amqp.Connection, exchange, exchangeType string, publishTimeout time.Duration) (*amqp.Channel, messagePublisher, error) {
	publishCh, err := conn.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("open publish channel: %w", err)
	}
	if err := declareExchange(publishCh, exchange, exchangeType); err != nil {
		discardCloseErr(publishCh)
		return nil, nil, fmt.Errorf("declare exchange %s: %w", exchange, err)
	}
	if err := publishCh.Confirm(false); err != nil {
		discardCloseErr(publishCh)
		return nil, nil, fmt.Errorf("enable publisher confirm: %w", err)
	}
	return publishCh, newAMQPPublisher(publishCh, publishTimeout), nil
}

func (c *Client) installTransport(
	conn *amqp.Connection,
	publishCh *amqp.Channel,
	publisher messagePublisher,
	oldPublishCh *amqp.Channel,
	replaceConn bool,
) error {
	// 锁顺序：publishMu -> reconnectMu，与 Close/publishOnce 保持一致。
	c.publishMu.Lock()
	c.reconnectMu.Lock()
	if c.lifeCtx.Err() != nil {
		c.reconnectMu.Unlock()
		c.publishMu.Unlock()
		discardCloseErr(publishCh)
		if replaceConn {
			discardCloseErr(conn)
		}
		return fmt.Errorf("client already closed")
	}

	oldConn := c.conn
	if !replaceConn {
		oldConn = nil
	}
	if oldPublishCh == nil {
		oldPublishCh = c.publishCh
	}
	c.generation++
	gen := c.generation
	c.conn = conn
	c.publishCh = publishCh
	c.publisher = publisher
	c.broken = false
	c.reconnectMu.Unlock()
	c.publishMu.Unlock()

	c.watchTransport(conn, publishCh, gen)

	if oldPublishCh != nil && oldPublishCh != publishCh {
		discardCloseErr(oldPublishCh)
	}
	if oldConn != nil && oldConn != conn {
		discardCloseErr(oldConn)
	}

	if gen == 1 {
		c.log.Infof("module=%s action=connect status=success generation=%d", defaultModuleName, gen)
	} else {
		c.log.Infof("module=%s action=reconnect status=success generation=%d", defaultModuleName, gen)
	}
	return nil
}

func (c *Client) watchTransport(conn *amqp.Connection, ch *amqp.Channel, gen uint64) {
	connClose := conn.NotifyClose(make(chan *amqp.Error, 1))
	chClose := ch.NotifyClose(make(chan *amqp.Error, 1))
	go func() {
		var err *amqp.Error
		select {
		case err = <-connClose:
		case err = <-chClose:
		case <-c.lifeCtx.Done():
			return
		}
		c.markBroken(gen, err)
	}()
}

func (c *Client) markBroken(gen uint64, err *amqp.Error) {
	c.reconnectMu.Lock()
	defer c.reconnectMu.Unlock()
	if c.generation != gen {
		return
	}
	c.broken = true
	if err != nil {
		c.log.Warnf(
			"module=%s action=transport_closed generation=%d error=%v",
			defaultModuleName,
			gen,
			err,
		)
	}
}

func (c *Client) invalidateTransport() {
	c.reconnectMu.Lock()
	defer c.reconnectMu.Unlock()
	c.broken = true
}

func (c *Client) openChannel() (*amqp.Channel, error) {
	c.reconnectMu.Lock()
	conn := c.conn
	c.reconnectMu.Unlock()
	if conn == nil || conn.IsClosed() {
		return nil, amqp.ErrClosed
	}
	return conn.Channel()
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

// runSubscription 监督单个订阅意图：首次消费由 Subscribe 同步建立，后续断连后自动恢复。
func (c *Client) runSubscription(sub *subscription, initial <-chan amqp.Delivery) {
	defer c.finishSubscription(sub)

	deliveries := initial
	for {
		if c.subscriptionEnded(sub) {
			return
		}

		loopCtx, cancel, activeDeliveries, action := c.prepareSubscriptionLoop(sub, deliveries)
		switch action {
		case subscriptionLoopExit:
			return
		case subscriptionLoopRetry:
			continue
		case subscriptionLoopReady:
			if loopCtx == nil || activeDeliveries == nil {
				return
			}
			c.consumeLoop(loopCtx, sub, sub.opts, activeDeliveries, sub.handler)
			cancel()
			discardErr(sub.closeChannel())
			deliveries = nil
		}

		if c.subscriptionEnded(sub) {
			return
		}

		c.invalidateTransport()
		c.log.Warnf(
			"module=%s action=subscribe_recover status=channel_closed queue=%s",
			defaultModuleName,
			sub.opts.Queue,
		)
	}
}

type subscriptionLoopAction int

const (
	subscriptionLoopExit  subscriptionLoopAction = 0
	subscriptionLoopRetry subscriptionLoopAction = 1
	subscriptionLoopReady subscriptionLoopAction = 2
)

func (c *Client) finishSubscription(sub *subscription) {
	discardErr(sub.closeChannel())
	close(sub.done)

	c.mu.Lock()
	delete(c.subs, sub)
	c.mu.Unlock()
}

func (c *Client) subscriptionEnded(sub *subscription) bool {
	if sub.isStopped() || c.isClosed() {
		return true
	}
	return sub.parentCtx != nil && sub.parentCtx.Err() != nil
}

func (c *Client) prepareSubscriptionLoop(
	sub *subscription,
	deliveries <-chan amqp.Delivery,
) (context.Context, context.CancelFunc, <-chan amqp.Delivery, subscriptionLoopAction) {
	if deliveries != nil {
		loopCtx, cancel := context.WithCancel(sub.parentCtx)
		sub.setLoopCancel(cancel)
		return loopCtx, cancel, deliveries, subscriptionLoopReady
	}

	waitCtx, cancelWait := c.subscriptionWaitContext(sub)
	err := c.ensureConnected(waitCtx)
	cancelWait()
	if err != nil {
		if c.subscriptionEnded(sub) {
			return nil, nil, nil, subscriptionLoopExit
		}
		return nil, nil, nil, subscriptionLoopRetry
	}

	loopCtx, cancel := context.WithCancel(sub.parentCtx)
	sub.setLoopCancel(cancel)

	ch, nextDeliveries, startErr := c.startConsume(sub)
	if startErr != nil {
		cancel()
		if c.subscriptionEnded(sub) {
			return nil, nil, nil, subscriptionLoopExit
		}
		c.invalidateTransport()
		c.log.Warnf(
			"module=%s action=subscribe_recover status=start_failed queue=%s error=%v",
			defaultModuleName,
			sub.opts.Queue,
			startErr,
		)
		if c.waitAfterSubscribeRecoverFailure(sub) {
			return nil, nil, nil, subscriptionLoopExit
		}
		return nil, nil, nil, subscriptionLoopRetry
	}

	sub.setChannel(ch)
	return loopCtx, cancel, nextDeliveries, subscriptionLoopReady
}

func (c *Client) waitAfterSubscribeRecoverFailure(sub *subscription) bool {
	select {
	case <-time.After(defaultReconnectInitial):
		return false
	case <-sub.stopCh:
		return true
	case <-sub.parentCtx.Done():
		return true
	case <-c.lifeCtx.Done():
		return true
	}
}

// subscriptionWaitContext 返回可被 Drain/Close 或 parent context 取消的等待 context。
// Drain 只会关闭 stopCh，不会取消 parentCtx，从而避免打断 in-flight handler。
func (c *Client) subscriptionWaitContext(sub *subscription) (context.Context, context.CancelFunc) {
	parent := sub.parentCtx
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	stopCh := sub.stopCh
	go func() {
		if stopCh == nil {
			return
		}
		select {
		case <-stopCh:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

func (c *Client) startConsume(sub *subscription) (*amqp.Channel, <-chan amqp.Delivery, error) {
	if err := c.EnsureTopology(sub.parentCtx, sub.opts); err != nil {
		return nil, nil, err
	}

	ch, err := c.openChannel()
	if err != nil {
		return nil, nil, fmt.Errorf("open channel: %w", err)
	}
	if err := ch.Qos(sub.opts.Prefetch, 0, false); err != nil {
		discardCloseErr(ch)
		return nil, nil, fmt.Errorf("set qos queue=%s: %w", sub.opts.Queue, err)
	}

	deliveries, err := ch.Consume(sub.opts.Queue, sub.consumerTag, false, false, false, false, nil)
	if err != nil {
		discardCloseErr(ch)
		return nil, nil, fmt.Errorf("consume queue=%s: %w", sub.opts.Queue, err)
	}
	return ch, deliveries, nil
}

// consumeLoop 持续读取 RabbitMQ delivery，并把 ack/retry/DLQ 决策委托给 handleDelivery。
func (c *Client) consumeLoop(
	ctx context.Context,
	sub *subscription,
	opts SubscribeOptions,
	deliveries <-chan amqp.Delivery,
	handler Handler,
) {
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
	return c.handleFailure(ctx, opts, delivery, err)
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
				"handle rabbitmq message failed: %w; publish failure target failed: %w; nack requeue failed: %w",
				handlerErr,
				err,
				nackErr,
			)
		}
		return fmt.Errorf("handle rabbitmq message failed: %w; publish failure target failed: %w", handlerErr, err)
	}

	// 转发成功，ack 原消息
	if ackErr := delivery.Ack(false); ackErr != nil {
		return fmt.Errorf("ack failed rabbitmq message failed after retry publish: %w", ackErr)
	}
	return fmt.Errorf("handle rabbitmq message failed: %w", handlerErr)
}

func (c *Client) publishMessage(ctx context.Context, exchange, routingKey string, publishing amqp.Publishing, mandatory bool) error {
	if c.hasTransportConfig() {
		if err := c.ensureConnected(ctx); err != nil {
			return err
		}
	}

	retried := false
	for {
		err := c.publishOnce(ctx, exchange, routingKey, publishing, mandatory)
		if err == nil {
			return nil
		}

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

func (c *Client) publishOnce(ctx context.Context, exchange, routingKey string, publishing amqp.Publishing, mandatory bool) error {
	c.publishMu.Lock()
	defer c.publishMu.Unlock()

	if c.isClosed() {
		return fmt.Errorf("client already closed")
	}
	if c.publisher == nil {
		return fmt.Errorf("client is not initialized")
	}
	if c.hasTransportConfig() && !c.transportReady() {
		return &publishOutcomeError{err: amqp.ErrClosed, submitted: false}
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

	if p.ch == nil || p.ch.IsClosed() {
		return &publishOutcomeError{err: amqp.ErrClosed, submitted: false}
	}

	p.drainReturns()

	confirm, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, mandatory, false, publishing)
	if err != nil {
		if isTransportFailure(err) {
			return &publishOutcomeError{err: err, submitted: false}
		}
		return fmt.Errorf("publish exchange=%s routing_key=%s: %w", exchange, routingKey, err)
	}
	if confirm == nil {
		return fmt.Errorf("publish exchange=%s routing_key=%s: publisher confirm is not enabled", exchange, routingKey)
	}

	acked, err := confirm.WaitContext(ctx)
	if err != nil {
		if isTransportFailure(err) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return &publishOutcomeError{err: err, submitted: true}
		}
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
	headers[retryCountHeader] = int64(retryCount)

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
	maps.Copy(cloned, table)
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

// discardCloseErr 在清理路径关闭资源，忽略已关闭等可预期错误。
func discardCloseErr(closer interface{ Close() error }) {
	if err := closeAMQP(closer); err != nil {
		return
	}
}

// discardErr 在清理路径忽略可预期错误。
func discardErr(err error) {
	if err != nil {
		return
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
	return errors.Is(err, amqp.ErrClosed)
}

// isTransportFailure 判断错误是否表示 connection/channel 传输层失败。
func isTransportFailure(err error) bool {
	if err == nil {
		return false
	}
	if isAMQPClosed(err) {
		return true
	}
	var amqpErr *amqp.Error
	if errors.As(err, &amqpErr) {
		switch amqpErr.Code {
		case 320, 501, 504:
			return true
		}
	}
	msg := err.Error()
	return strings.Contains(msg, "channel/connection is not open") ||
		strings.Contains(msg, "channel is not open") ||
		strings.Contains(msg, "connection not open")
}

func reconnectDelayWithJitter(base time.Duration) time.Duration {
	if base <= 0 {
		return defaultReconnectInitial
	}
	maxJitter := int64(base/5) + 1
	n, err := crand.Int(crand.Reader, big.NewInt(maxJitter))
	if err != nil {
		return base
	}
	return base + time.Duration(n.Int64())
}

// isClosed 返回客户端是否已经关闭。
func (c *Client) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}
