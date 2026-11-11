package mq

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// subscriptionChannel 抽象订阅使用的 AMQP channel 取消与关闭操作，便于单元测试注入。
type subscriptionChannel interface {
	Cancel(consumer string, noWait bool) error
	Close() error
}

// lockedAcknowledger 使用订阅专属互斥锁包装底层 Acknowledger，
// 保证同一消费 channel 上的 Ack/Nack/Reject 与 Cancel/Close 不会并发交错。
type lockedAcknowledger struct {
	mu   *sync.Mutex
	next amqp.Acknowledger
}

// subscription 表示一个活跃订阅的内部状态机与并发控制句柄。
// 锁职责划分：
//   - stateMu：仅保护 channel 引用字段的读写与原子交换，绝不跨网络 I/O 持有。
//   - channelMu：串行化同一消费 channel 上的 Ack/Nack/Reject 与 Cancel RPC。
//   - stopMu：保护 stopped 状态、stopCh 关闭以及 loopCancel/runCancel 的读取与更新。
type subscription struct {
	client       *Client
	opts         SubscribeOptions
	handler      Handler
	parentCtx    context.Context
	stateMu      sync.Mutex
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

	cancelFinished chan struct{}
	cancelErr      error
}

type subscriptionLoopAction int

const (
	subscriptionLoopExit  subscriptionLoopAction = 0
	subscriptionLoopRetry subscriptionLoopAction = 1
	subscriptionLoopReady subscriptionLoopAction = 2
)

// Subscribe 创建 RabbitMQ 持久化队列订阅，并使用 handler 处理消息。
// 入参 ctx 仅控制首次拓扑声明、Qos 和 Consume 的同步启动阶段；
// 返回的 Subscription 后台生命周期绑定客户端生命周期，在连接被动断开后会自动恢复消费，直到调用 Drain、Close 或 Client.Close。
// 注意：取消传入 Subscribe 的 ctx 不会停止已建立的后台订阅；如需停止消费，请显式调用 sub.Drain(ctx)、sub.Close() 或 client.Close()。
func (c *Client) Subscribe(ctx context.Context, opts SubscribeOptions, handler Handler) (Subscription, error) {
	// 1. 校验入参 handler 与客户端当前运行状态。
	if ctx == nil {
		ctx = context.Background()
	}
	if handler == nil {
		return nil, fmt.Errorf("subscribe rabbitmq failed: handler is required")
	}
	if c.isClosed() {
		return nil, fmt.Errorf("subscribe rabbitmq failed: client already closed")
	}
	if !c.hasTransportConfig() {
		return nil, fmt.Errorf("subscribe rabbitmq failed: client is not initialized")
	}

	// 2. 规范化订阅参数（自动对齐 Prefetch >= Concurrency）并校验重试延迟 TTL 范围。
	normalized, err := normalizeSubscribeOptionsForExchange(opts, c.exchangeType)
	if err != nil {
		return nil, fmt.Errorf("subscribe rabbitmq failed: %w", err)
	}
	if _, err := ttlMilliseconds(normalized.RetryDelay); err != nil {
		return nil, fmt.Errorf("subscribe rabbitmq failed: %w", err)
	}

	// 3. 创建绑定到 Client 生命周期的后台运行上下文（不随启动期入参 ctx 取消而中断）。
	baseCtx := c.lifeCtx
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	runCtx, runCancel := context.WithCancel(baseCtx)
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

	// 4. 同步建立首次消费通道（声明拓扑 + 设置 Qos + 启动 Consume），确保返回前订阅已就绪。
	ch, deliveries, err := c.startConsume(ctx, sub)
	if err != nil {
		runCancel()
		return nil, fmt.Errorf("subscribe rabbitmq failed: %w", err)
	}

	// 5. 二次检查 Client 是否在建连期间被并发关闭；未关闭则登记订阅并启动后台消费与自愈监督协程。
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
		"module=%s action=subscribe exchange=%s queue=%s binding_keys=%s",
		defaultModuleName,
		c.exchange,
		normalized.Queue,
		strings.Join(normalized.BindingKeys, ","),
	)
	return sub, nil
}

// Drain 优雅停止订阅，并等待已投递消息处理完成。
// Drain 只停止接收新消息并禁止自动恢复，不会取消正在执行的 handler context。
func (s *subscription) Drain(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	// 1. 标记订阅停止自动重连，但保留正在执行的 handler context。
	s.markStopped()

	// 2. 在 ctx 约束下向 Broker 发送 basic.cancel 停止投递新消息。
	if err := s.cancelConsumerWithContext(ctx); err != nil {
		return fmt.Errorf("drain rabbitmq subscription failed: %w", err)
	}

	// 3. 等待已拉取到本地的所有在途消息由 handler 处理并确认完毕。
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("drain rabbitmq subscription failed: wait done: %w", ctx.Err())
	}
}

// cancelConsumerWithContext 在给定 context 超时约束下执行 basic.cancel；
// 若半开连接导致底层 Cancel 阻塞超时，主动关闭底层 channel 以解除阻塞。
func (s *subscription) cancelConsumerWithContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("cancel consumer: %w", err)
	}

	errCh := make(chan error, 1)
	go func() {
		var err error
		s.drainOnce.Do(func() {
			err = s.cancelConsumer()
		})
		errCh <- err
	}()

	select {
	case err := <-errCh:
		return err
	case <-s.done:
		return nil
	case <-ctx.Done():
		// 若底层 Cancel RPC 因半开连接阻塞超时，主动关闭底层 channel 以唤醒阻塞协程并释放 channelMu。
		discardErr(s.closeChannel())
		return fmt.Errorf("cancel consumer: %w", ctx.Err())
	}
}

// Close 立即停止订阅并释放相关资源。
// Close 会取消订阅运行 context，打断重连等待与当前消费循环。
// 若 basic.cancel 因半开连接阻塞，Close 仍按 closeTimeout 返回，并强制关闭底层 channel 唤醒阻塞 RPC。
func (s *subscription) Close() error {
	// 1. 首次调用时标记停止恢复、取消运行期 context（通知 in-flight handler 退出），并异步触发 basic.cancel。
	s.once.Do(func() {
		s.markStopped()
		s.cancelRunning()
		s.cancelFinished = make(chan struct{})
		go func() {
			var err error
			s.drainOnce.Do(func() {
				err = s.cancelConsumer()
			})
			s.cancelErr = err
			close(s.cancelFinished)
		}()
	})

	// 2. 启动关闭超时定时器，等待消费循环退出（done）与取消请求完成（cancelFinished）。
	timeout := s.closeTimeout
	if timeout <= 0 {
		timeout = DefaultCloseTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	doneCh := s.done
	cancelCh := s.cancelFinished
	var (
		sawDone   bool
		sawCancel bool
		cancelErr error
	)
	for {
		select {
		case <-doneCh:
			sawDone = true
			doneCh = nil
		case <-cancelCh:
			sawCancel = true
			cancelErr = s.cancelErr
			cancelCh = nil
		case <-timer.C:
			// 3. 若超时仍未退出，非阻塞采集已有取消错误并强制关闭底层 channel 释放资源。
			if !sawCancel {
				select {
				case <-s.cancelFinished:
					sawCancel = true
					cancelErr = s.cancelErr
				default:
				}
			}
			if err := s.closeChannel(); err != nil {
				cancelErr = errors.Join(cancelErr, err)
			}
			if cancelErr != nil {
				return fmt.Errorf("close rabbitmq subscription failed: %w", cancelErr)
			}
			return fmt.Errorf("close rabbitmq subscription failed: wait done timeout")
		}
		if sawDone && sawCancel {
			if cancelErr != nil {
				return fmt.Errorf("close rabbitmq subscription failed: %w", cancelErr)
			}
			return nil
		}
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

// isStopped 返回当前订阅是否已被标记为停止恢复。
func (s *subscription) isStopped() bool {
	s.stopMu.Lock()
	defer s.stopMu.Unlock()
	return s.stopped
}

// setLoopCancel 更新当前单轮消费循环的取消函数。
func (s *subscription) setLoopCancel(cancel context.CancelFunc) {
	s.stopMu.Lock()
	defer s.stopMu.Unlock()
	s.loopCancel = cancel
	// 仅在 Close 已取消 parentCtx 时立即取消新循环，避免 Drain 误杀 in-flight handler。
	if s.parentCtx != nil && s.parentCtx.Err() != nil {
		cancel()
	}
}

// cancelConsumer 在 channelMu 保护下向 Broker 发送 basic.cancel 停止当前消费者投递。
func (s *subscription) cancelConsumer() error {
	s.channelMu.Lock()
	defer s.channelMu.Unlock()

	ch := s.getChannel()
	if ch == nil {
		return nil
	}
	if err := ch.Cancel(s.consumerTag, false); err != nil && !isAMQPClosed(err) {
		return fmt.Errorf("cancel consumer=%s: %w", s.consumerTag, err)
	}
	return nil
}

// closeChannel 原子取出并关闭当前订阅的底层 AMQP channel。
func (s *subscription) closeChannel() error {
	ch := s.takeChannel()
	if ch == nil {
		return nil
	}
	// 正常路径下获取 channelMu 串行关闭；若 Cancel 因半开连接正阻塞持有 channelMu，
	// 则直接关闭底层 channel 以打断阻塞调用，避免 Close() 在超时后死锁。
	if s.channelMu.TryLock() {
		defer s.channelMu.Unlock()
	}
	return closeAMQP(ch)
}

// setChannel 线程安全地保存当前订阅使用的 AMQP channel。
func (s *subscription) setChannel(ch subscriptionChannel) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.channel = ch
}

// getChannel 线程安全地读取当前订阅使用的 AMQP channel。
func (s *subscription) getChannel() subscriptionChannel {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.channel
}

// takeChannel 线程安全地取出并清空当前订阅的 AMQP channel，确保底层 channel 仅被关闭一次。
func (s *subscription) takeChannel() subscriptionChannel {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	ch := s.channel
	s.channel = nil
	return ch
}

// runSubscription 监督单个订阅意图：首次消费由 Subscribe 同步建立，后续断连后自动恢复。
func (c *Client) runSubscription(sub *subscription, initial <-chan amqp.Delivery) {
	defer c.finishSubscription(sub)

	deliveries := initial
	for {
		// 1. 每次循环前检查订阅或客户端是否已终止。
		if c.subscriptionEnded(sub) {
			return
		}

		// 2. 准备当前轮次的消费上下文与 deliveries 通道（首次直接使用 initial，断连后自动重连重建）。
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
			// 3. 阻塞消费直到 deliveries 通道关闭（如断连或 basic.cancel）或 loopCtx 被取消。
			c.consumeLoop(loopCtx, sub, sub.opts, activeDeliveries, sub.handler)
			cancel()
			discardErr(sub.closeChannel())
			deliveries = nil
		}

		// 4. 若因主动 Drain/Close 结束则直接退出；否则标记传输层失效并进入下一轮自动重连恢复。
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

// finishSubscription 在监督协程退出时做最终资源兜底清理，并从 Client 活跃订阅表中移除。
func (c *Client) finishSubscription(sub *subscription) {
	discardErr(sub.closeChannel())
	close(sub.done)

	c.mu.Lock()
	delete(c.subs, sub)
	c.mu.Unlock()
}

// subscriptionEnded 判断订阅是否因主动停止或 Client 关闭而应当终止监督循环。
func (c *Client) subscriptionEnded(sub *subscription) bool {
	if sub.isStopped() || c.isClosed() {
		return true
	}
	return sub.parentCtx != nil && sub.parentCtx.Err() != nil
}

// prepareSubscriptionLoop 为单轮消费准备上下文与投递通道；当 deliveries 为 nil 时执行断连恢复。
func (c *Client) prepareSubscriptionLoop(
	sub *subscription,
	deliveries <-chan amqp.Delivery,
) (context.Context, context.CancelFunc, <-chan amqp.Delivery, subscriptionLoopAction) {
	// 1. 首次启动已由 Subscribe 同步拿到 deliveries，直接绑定 loopCtx 进入消费。
	if deliveries != nil {
		loopCtx, cancel := context.WithCancel(sub.parentCtx)
		sub.setLoopCancel(cancel)
		return loopCtx, cancel, deliveries, subscriptionLoopReady
	}

	// 2. 断连恢复路径：等待底层 AMQP 连接就绪（可被 Drain/Close 随时打断）。
	waitCtx, cancelWait := c.subscriptionWaitContext(sub)
	err := c.ensureConnected(waitCtx)
	if err != nil {
		cancelWait()
		if c.subscriptionEnded(sub) {
			return nil, nil, nil, subscriptionLoopExit
		}
		return nil, nil, nil, subscriptionLoopRetry
	}

	loopCtx, cancel := context.WithCancel(sub.parentCtx)
	sub.setLoopCancel(cancel)

	// 3. 重新打开消费 channel、声明拓扑、设置 Qos 并启动 Consume。
	ch, nextDeliveries, startErr := c.startConsume(waitCtx, sub)
	cancelWait()
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

// waitAfterSubscribeRecoverFailure 在重建消费通道失败后短暂退避等待，期间可被停止信号立即唤醒。
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

// startConsume 打开独立消费 channel，依次完成拓扑声明、Qos 设置与 basic.consume 注册。
func (c *Client) startConsume(ctx context.Context, sub *subscription) (amqpChannel, <-chan amqp.Delivery, error) {
	// 1. 确保底层连接处于健康可用状态。
	if err := c.ensureConnected(ctx); err != nil {
		return nil, nil, err
	}

	// 2. 为当前订阅打开独占的 AMQP channel。
	ch, err := c.openChannel()
	if err != nil {
		c.invalidateTransport()
		return nil, nil, fmt.Errorf("open channel: %w", err)
	}
	// 3. 幂等声明主交换机、重试交换机、死信交换机以及对应的三类队列与绑定。
	if err := c.declareTopology(ctx, ch, sub.opts); err != nil {
		discardCloseErr(ch)
		return nil, nil, fmt.Errorf("ensure rabbitmq topology failed: %w", err)
	}
	// 4. 设置消费者预取上限（Prefetch）并注册手动确认（autoAck=false）消费者。
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
	if opts.Concurrency > 1 {
		c.consumeLoopConcurrent(ctx, sub, opts, deliveries, handler)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case delivery, ok := <-deliveries:
			if !ok {
				return
			}
			c.processDelivery(ctx, sub, opts, delivery, handler)
		}
	}
}

// consumeLoopConcurrent 使用信号量池控制最多 opts.Concurrency 个协程并发处理消息，并在退出前等待所有在途任务完成。
func (c *Client) consumeLoopConcurrent(
	ctx context.Context,
	sub *subscription,
	opts SubscribeOptions,
	deliveries <-chan amqp.Delivery,
	handler Handler,
) {
	sem := make(chan struct{}, opts.Concurrency)
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		// 1. 先获取并发令牌，达到 Concurrency 上限时阻塞背压。
		select {
		case <-ctx.Done():
			return
		case sem <- struct{}{}:
		}

		// 2. 读取下一条投递消息；若通道已关闭或上下文已取消则归还令牌并退出循环（defer wg.Wait 等待在途协程）。
		select {
		case <-ctx.Done():
			<-sem
			return
		case delivery, ok := <-deliveries:
			if !ok {
				<-sem
				return
			}
			// 3. 启动工作协程处理该条消息，完成后释放并发令牌。
			wg.Add(1)
			go func(d amqp.Delivery) {
				defer wg.Done()
				defer func() { <-sem }()
				c.processDelivery(ctx, sub, opts, d, handler)
			}(delivery)
		}
	}
}

// processDelivery 包装单条投递的并发安全确认器，执行业务处理并统一记录失败告警日志。
func (c *Client) processDelivery(
	ctx context.Context,
	sub *subscription,
	opts SubscribeOptions,
	delivery amqp.Delivery,
	handler Handler,
) {
	delivery = sub.lockDeliveryAcknowledger(delivery)
	if err := c.handleDelivery(ctx, opts, delivery, handler); err != nil {
		var ce *consumeError
		if errors.As(err, &ce) && ce.terminal {
			c.log.Errorf(
				"consume failed: module=%s action=handle_message exchange=%s queue=%s routing_key=%s error=%v",
				defaultModuleName,
				c.exchange,
				opts.Queue,
				originalRoutingKeyFromDelivery(delivery),
				err,
			)
			return
		}
		c.log.Warnf(
			"consume failed: module=%s action=handle_message exchange=%s queue=%s routing_key=%s error=%v",
			defaultModuleName,
			c.exchange,
			opts.Queue,
			originalRoutingKeyFromDelivery(delivery),
			err,
		)
	}
}

// lockDeliveryAcknowledger 使用订阅 channelMu 包装 delivery.Acknowledger，
// 确保同一 AMQP channel 上的 ack/nack/reject 与 consumer cancel/close 串行执行。
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

	err := invokeHandler(ctx, message, handler)

	// 1. 业务处理成功，直接 Ack 确认消息。
	if err == nil {
		if ackErr := delivery.Ack(false); ackErr != nil {
			return &consumeError{
				terminal: true,
				err:      fmt.Errorf("ack rabbitmq message failed: queue=%s routing_key=%s: %w", opts.Queue, message.RoutingKey, ackErr),
			}
		}
		return nil
	}

	// 2. 业务处理失败且当前订阅上下文已取消（如正在 Close），直接 Nack(requeue=true) 重新入队，不消耗重试次数。
	if ctx.Err() != nil {
		if nackErr := delivery.Nack(false, true); nackErr != nil {
			return &consumeError{
				terminal: true,
				err:      fmt.Errorf("nack canceled rabbitmq message failed: queue=%s routing_key=%s: %w", opts.Queue, message.RoutingKey, nackErr),
			}
		}
		return fmt.Errorf("handle rabbitmq message canceled: %w", ctx.Err())
	}

	// 3. 业务正常处理失败（或 panic），按 MaxDeliveries 策略转发到 retry exchange 或 DLX。
	return c.handleFailure(ctx, opts, delivery, err)
}

// consumeError 标记消费失败属于可重试异常（terminal=false -> Warnf）还是终态故障/死信（terminal=true -> Errorf）。
type consumeError struct {
	err      error
	terminal bool
}

func (e *consumeError) Error() string {
	if e == nil || e.err == nil {
		return "consume error"
	}
	return e.err.Error()
}

func (e *consumeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

// invokeHandler 调用业务 handler 并捕获可能发生的 panic，将其转化为带调用栈的普通 error 走重试/死信流程，避免单条毒消息导致进程崩溃。
func invokeHandler(ctx context.Context, message Message, handler Handler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panicked: %v\n%s", r, debug.Stack())
		}
	}()
	return handler(ctx, message)
}

// handleFailure 将失败消息按队列名点对点转发到 retry exchange 或 DLX，成功转发后再 ack 原消息。
// 注意：这是至少一次（at-least-once）投递保证——若转发成功但随后的 Ack 因连接断开失败，
// 原消息与转发消息会同时存在，业务侧应基于 Message.ID 保持消费幂等。
func (c *Client) handleFailure(ctx context.Context, opts SubscribeOptions, delivery amqp.Delivery, handlerErr error) error {
	// 1. 计算当前是第几次投递（1-based）；未达 MaxDeliveries 时进 retry exchange，否则进死信 exchange。
	currentDelivery := retryCountFromHeaders(delivery.Headers) + 1
	enteredDLQ := currentDelivery >= opts.MaxDeliveries

	targetExchange := retryExchangeName(c.exchange)
	if enteredDLQ {
		targetExchange = deadLetterExchangeName(c.exchange)
	}

	// 2. 构造携带递增 x-mq-retry-count 和原始业务 routing key 的转发消息，并以队列名 opts.Queue 点对点发布。
	publishing := publishingFromDelivery(delivery, currentDelivery)
	if err := c.publishMessage(ctx, targetExchange, opts.Queue, publishing, true); err != nil {
		// 3. 若转发本身失败，Nack(requeue=true) 让原消息重新回主队列，杜绝消息静默丢失（标记为终态告警 Errorf）。
		if nackErr := delivery.Nack(false, true); nackErr != nil {
			return &consumeError{
				terminal: true,
				err: fmt.Errorf(
					"handle rabbitmq message failed: %w; publish failure target failed: %w; nack requeue failed: %w",
					handlerErr,
					err,
					nackErr,
				),
			}
		}
		return &consumeError{
			terminal: true,
			err:      fmt.Errorf("handle rabbitmq message failed: %w; publish failure target failed: %w", handlerErr, err),
		}
	}

	// 4. 转发并得到 Broker Confirm 成功后，Ack 移除原队列中的旧消息。
	if ackErr := delivery.Ack(false); ackErr != nil {
		return &consumeError{
			terminal: true,
			err:      fmt.Errorf("ack failed rabbitmq message failed after retry publish: %w", ackErr),
		}
	}
	return &consumeError{
		terminal: enteredDLQ,
		err:      fmt.Errorf("handle rabbitmq message failed: %w", handlerErr),
	}
}
