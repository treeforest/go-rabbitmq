package mq

import (
	"context"
	crand "crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
)

const defaultModuleName = "mq.rabbitmq"

// dialAMQP 允许测试替换 Dial 实现；生产路径默认调用 amqp.DialConfig。
var dialAMQP = amqp.DialConfig

// publishConfirm 抽象 AMQP 发布确认句柄（*amqp.DeferredConfirmation 原生实现该接口）。
type publishConfirm interface {
	WaitContext(ctx context.Context) (bool, error)
}

// amqpChannel 抽象本包使用的 AMQP channel 能力，便于生产绑定真实 *amqp.Channel 并在单测中注入替身。
type amqpChannel interface {
	IsClosed() bool
	Close() error
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
	NotifyReturn(c chan amqp.Return) chan amqp.Return
	Confirm(noWait bool) error
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	Qos(prefetchCount, prefetchSize int, global bool) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Cancel(consumer string, noWait bool) error
	PublishDeferred(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (publishConfirm, error)
}

// amqpConnection 抽象本包使用的 AMQP connection 能力。
type amqpConnection interface {
	Channel() (amqpChannel, error)
	IsClosed() bool
	Close() error
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
}

type realAMQPConnection struct {
	conn *amqp.Connection
}

func (r *realAMQPConnection) Channel() (amqpChannel, error) {
	ch, err := r.conn.Channel()
	if err != nil {
		return nil, err
	}
	return &realAMQPChannel{ch: ch}, nil
}

func (r *realAMQPConnection) IsClosed() bool {
	return r.conn.IsClosed()
}

func (r *realAMQPConnection) Close() error {
	return r.conn.Close()
}

func (r *realAMQPConnection) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	return r.conn.NotifyClose(c)
}

type realAMQPChannel struct {
	ch *amqp.Channel
}

func (r *realAMQPChannel) IsClosed() bool {
	return r.ch.IsClosed()
}

func (r *realAMQPChannel) Close() error {
	return r.ch.Close()
}

func (r *realAMQPChannel) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	return r.ch.NotifyClose(c)
}

func (r *realAMQPChannel) NotifyReturn(c chan amqp.Return) chan amqp.Return {
	return r.ch.NotifyReturn(c)
}

func (r *realAMQPChannel) Confirm(noWait bool) error {
	return r.ch.Confirm(noWait)
}

func (r *realAMQPChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	return r.ch.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}

func (r *realAMQPChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	return r.ch.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

func (r *realAMQPChannel) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	return r.ch.QueueBind(name, key, exchange, noWait, args)
}

func (r *realAMQPChannel) Qos(prefetchCount, prefetchSize int, global bool) error {
	return r.ch.Qos(prefetchCount, prefetchSize, global)
}

func (r *realAMQPChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	return r.ch.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}

func (r *realAMQPChannel) Cancel(consumer string, noWait bool) error {
	return r.ch.Cancel(consumer, noWait)
}

func (r *realAMQPChannel) PublishDeferred(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (publishConfirm, error) {
	confirm, err := r.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, key, mandatory, immediate, msg)
	if confirm == nil {
		return nil, err
	}
	return confirm, err
}

// dialTransport 建立 AMQP 连接并包装为内部接口；默认委托给 dialAMQP，便于单测注入。
var dialTransport = func(url string, cfg amqp.Config) (amqpConnection, error) {
	conn, err := dialAMQP(url, cfg)
	if err != nil {
		return nil, err
	}
	return &realAMQPConnection{conn: conn}, nil
}

// Client 表示基于 RabbitMQ 的消息队列客户端。
// Client 必须通过 NewClient 创建；零值不包含 AMQP 连接，不能直接使用。
// 连接或 channel 被动关闭后，Client 会按需透明重连并恢复发布与订阅。
//
// 内部互斥锁顺序约定：
//   - 当同时需要获取 publishMu 与 reconnectMu 时，固定顺序为：publishMu -> reconnectMu。
//   - mu 仅保护客户端关闭状态（closed）与活跃订阅集合（subs），绝不持锁调用 Subscription.Close。
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
	conn          amqpConnection
	publishCh     amqpChannel
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

// NewClient 创建新的 RabbitMQ 客户端。
func NewClient(cfg Config) (*Client, error) {
	// 1. 校验配置并填充默认超时、交换机类型和日志器。
	normalized, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}

	// 2. 初始化客户端结构体与全局生命周期上下文。
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

	// 3. 建立初始 AMQP 连接、发布通道并声明主交换机。
	if err := client.establishTransport(); err != nil {
		lifeCancel()
		return nil, fmt.Errorf("create rabbitmq client failed: %w", err)
	}
	return client, nil
}

// Close 立即关闭客户端及其所有订阅，并停止后续重连。
// 注意：Close 会直接调用各订阅的 Close（取消在途 handler context）；
// 如需等待在途消息自然处理完成后再退出，请在调用 Client.Close 前显式调用各订阅的 Drain(ctx)。
func (c *Client) Close() error {
	// 1. 标记客户端已关闭并在锁内拷出当前所有活跃订阅快照，随后立即释放 c.mu 避免与 finishSubscription 死锁。
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

	// 2. 取消客户端全局生命周期上下文，立即中断所有后台重连退避等待。
	if c.lifeCancel != nil {
		c.lifeCancel()
	}

	// 3. 在锁外并发关闭所有活跃订阅。
	var group errgroup.Group
	for _, sub := range subs {
		current := sub
		group.Go(func() error {
			return current.Close()
		})
	}
	subErr := group.Wait()

	// 4. 按固定锁顺序（publishMu -> reconnectMu）关闭底层 publish channel 与 AMQP connection。
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

// dialConfig 构造带客户端连接名称、建连超时和默认 AMQP 心跳（10s）的拨号配置。
func dialConfig(cfg Config) amqp.Config {
	properties := amqp.NewConnectionProperties()
	if cfg.Name != "" {
		properties.SetClientConnectionName(cfg.Name)
	}

	dialer := net.Dialer{Timeout: cfg.ConnectTimeout}
	return amqp.Config{
		Heartbeat:  DefaultHeartbeat,
		Locale:     "en_US",
		Properties: properties,
		Dial:       dialer.Dial,
	}
}

// hasTransportConfig 判断客户端是否通过 NewClient 配置了有效连接地址。
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
		// 1. 快路径：当前连接与发布通道均健康可用，直接返回。
		if c.transportReady() {
			return nil
		}

		// 2. 单飞协调：首个发现断连的调用方成为领头协程（wait == nil），其余并发调用方获取同一个等待通道。
		wait, err := c.beginOrJoinReconnect()
		if err != nil {
			return err
		}
		if wait == nil {
			// 3. 领头协程：执行带抖动指数退避的重连，完成后广播唤醒所有等待协程。
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

		// 4. 跟随协程：阻塞等待领头协程完成重连，或受调用方自身 ctx / 客户端关闭信号提前退出。
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

// transportReady 检查当前 AMQP 连接、发布通道与发布器是否均处于未关闭且未标记损坏状态。
func (c *Client) transportReady() bool {
	c.reconnectMu.Lock()
	defer c.reconnectMu.Unlock()
	return c.conn != nil && !c.conn.IsClosed() &&
		c.publishCh != nil && !c.publishCh.IsClosed() &&
		c.publisher != nil && !c.broken
}

// beginOrJoinReconnect 发起或加入正在进行的单飞重连流程；返回 nil 表示调用方需负责执行重连。
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

// finishReconnect 记录本次单飞重连结果并关闭等待通道以唤醒所有跟随协程。
func (c *Client) finishReconnect(err error) {
	c.reconnectMu.Lock()
	defer c.reconnectMu.Unlock()
	c.reconnectErr = err
	if c.reconnectWait != nil {
		close(c.reconnectWait)
		c.reconnectWait = nil
	}
}

// reconnectWithBackoff 使用加密随机抖动的指数退避循环重建传输层，直到成功、ctx 取消或客户端关闭。
func (c *Client) reconnectWithBackoff(ctx context.Context) error {
	delay := defaultReconnectInitial
	var lastErr error
	for {
		// 1. 每次尝试前检查客户端是否已主动关闭。
		if c.isClosed() {
			return fmt.Errorf("client already closed")
		}
		// 2. 尝试修复 publish channel 或重新拨号建立 AMQP 连接。
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

		// 3. 按当前退避时长叠加 0~20% 随机抖动等待，防止集群重连风暴。
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

		// 4. 指数倍增退避间隔，上限封顶为 defaultReconnectMax（30s）。
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

	// 1. 若底层 TCP/AMQP 连接仍存活，优先在原连接上重建 publish channel，减少完整握手开销。
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

	// 2. 原连接不可用或重建 channel 失败时，重新拨号建立新 AMQP 连接与 publish channel。
	newConn, err := dialTransport(c.cfg.URL, dialConfig(c.cfg))
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

// openPublishChannel 在给定连接上打开并初始化发布通道（声明主交换机 + 开启 Publisher Confirm）。
func openPublishChannel(conn amqpConnection, exchange, exchangeType string, publishTimeout time.Duration) (amqpChannel, messagePublisher, error) {
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

// installTransport 原子切换客户端底层连接与发布通道，递增代数（generation），并在锁外清理旧资源。
func (c *Client) installTransport(
	conn amqpConnection,
	publishCh amqpChannel,
	publisher messagePublisher,
	oldPublishCh amqpChannel,
	replaceConn bool,
) error {
	// 1. 按固定锁顺序（publishMu -> reconnectMu）加锁，若客户端已在建连期间关闭则立即释放新资源。
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

	// 2. 记录待淘汰的旧连接/旧通道，递增传输层代数（generation）并切换新句柄。
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

	// 3. 启动针对当前代数的异步关闭监听协程，并在锁外安全关闭已被替换的旧通道与旧连接。
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

// watchTransport 监听指定代数（gen）的 connection 与 publish channel 被动关闭事件。
func (c *Client) watchTransport(conn amqpConnection, ch amqpChannel, gen uint64) {
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

// markBroken 在代数匹配时将当前传输层标记为不可用（防止旧连接的延迟关闭事件误伤新连接）。
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

// invalidateTransport 主动将当前传输层标记为需要重建。
func (c *Client) invalidateTransport() {
	c.reconnectMu.Lock()
	defer c.reconnectMu.Unlock()
	c.broken = true
}

// openChannel 在当前健康连接上打开一个新的 AMQP channel（供 EnsureTopology 或 Subscribe 使用）。
func (c *Client) openChannel() (amqpChannel, error) {
	c.reconnectMu.Lock()
	conn := c.conn
	c.reconnectMu.Unlock()
	if conn == nil || conn.IsClosed() {
		return nil, amqp.ErrClosed
	}
	return conn.Channel()
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

// reconnectDelayWithJitter 在基准退避时间上附加 0~20% 的加密随机正向抖动，避免多实例同时重连打满 Broker。
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
