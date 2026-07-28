// Package mq 的 mq_test 覆盖 RabbitMQ MQ 封装的 option、拓扑命名和消息确认行为。
package mq

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

// TestBuildPublishOptions 验证发布选项构造逻辑。
func TestBuildPublishOptions(t *testing.T) {
	tests := []struct {
		name   string
		opts   []PublishOption
		verify func(t *testing.T, got PublishOptions)
	}{
		{
			name: "defaults",
			verify: func(t *testing.T, got PublishOptions) {
				require.Empty(t, got.MessageID)
				require.Empty(t, got.Headers)
				require.True(t, got.Mandatory)
			},
		},
		{
			name: "message_id_headers_and_mandatory",
			opts: []PublishOption{
				WithMessageID("msg-1"),
				WithHeader("trace_id", "trace-1"),
				WithHeaders(map[string]string{"event_type": "order.created"}),
				WithMandatory(false),
			},
			verify: func(t *testing.T, got PublishOptions) {
				require.Equal(t, "msg-1", got.MessageID)
				require.Equal(t, "trace-1", got.Headers["trace_id"])
				require.Equal(t, "order.created", got.Headers["event_type"])
				require.False(t, got.Mandatory)
			},
		},
		{
			name: "nil_option_ignored",
			opts: []PublishOption{nil, WithHeader("request_id", "req-1")},
			verify: func(t *testing.T, got PublishOptions) {
				require.Equal(t, "req-1", got.Headers["request_id"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BuildPublishOptions(tt.opts...)
			tt.verify(t, got)
		})
	}
}

// TestNormalizeSubscribeOptions 验证订阅参数默认值与非法输入校验。
func TestNormalizeSubscribeOptions(t *testing.T) {
	tests := []struct {
		name    string
		input   SubscribeOptions
		wantErr string
		verify  func(t *testing.T, got SubscribeOptions)
	}{
		{
			name: "defaults_applied",
			input: SubscribeOptions{
				Queue:      "example-service.order-created",
				RoutingKey: "test.order.created",
			},
			verify: func(t *testing.T, got SubscribeOptions) {
				require.Equal(t, []string{"test.order.created"}, got.BindingKeys)
				require.Equal(t, DefaultPrefetch, got.Prefetch)
				require.Equal(t, DefaultMaxDeliver, got.MaxDeliver)
				require.Equal(t, DefaultRetryDelay, got.RetryDelay)
			},
		},
		{
			name: "custom_values_preserved",
			input: SubscribeOptions{
				Queue:       "example-service",
				RoutingKey:  "test.order.created",
				BindingKeys: []string{"test.order.*", "test.payment.#"},
				Prefetch:    64,
				MaxDeliver:  9,
				RetryDelay:  3 * time.Second,
			},
			verify: func(t *testing.T, got SubscribeOptions) {
				require.Equal(t, []string{"test.order.*", "test.payment.#"}, got.BindingKeys)
				require.Equal(t, 64, got.Prefetch)
				require.Equal(t, 9, got.MaxDeliver)
				require.Equal(t, 3*time.Second, got.RetryDelay)
			},
		},
		{
			name: "missing_queue",
			input: SubscribeOptions{
				RoutingKey: "test.order.created",
			},
			wantErr: "queue is required",
		},
		{
			name: "missing_routing_key",
			input: SubscribeOptions{
				Queue: "example-service",
			},
			wantErr: "routing key is required",
		},
		{
			name: "empty_binding_key",
			input: SubscribeOptions{
				Queue:       "example-service",
				RoutingKey:  "test.order.created",
				BindingKeys: []string{"test.order.*", " "},
			},
			wantErr: "binding key at index 1 is empty",
		},
		{
			name: "unsupported_legacy_wildcard_rejected",
			input: SubscribeOptions{
				Queue:       "example-service",
				RoutingKey:  "test.order.created",
				BindingKeys: []string{"test.>"},
			},
			wantErr: "unsupported wildcard",
		},
		{
			name: "empty_binding_word_rejected",
			input: SubscribeOptions{
				Queue:       "example-service",
				RoutingKey:  "test.order.created",
				BindingKeys: []string{"test..key"},
			},
			wantErr: "contains empty word",
		},
		{
			name: "wildcard_must_be_standalone_word",
			input: SubscribeOptions{
				Queue:       "example-service",
				RoutingKey:  "test.order.created",
				BindingKeys: []string{"test.order*"},
			},
			wantErr: "invalid wildcard word",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NormalizeSubscribeOptions(tt.input)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			tt.verify(t, got)
		})
	}
}

// TestNormalizeConfig 验证客户端配置默认值与必填校验。
func TestNormalizeConfig(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
		verify  func(t *testing.T, got Config)
	}{
		{name: "missing_url", cfg: Config{Exchange: "test.events"}, wantErr: "url is required"},
		{name: "missing_exchange", cfg: Config{URL: "amqp://guest:guest@127.0.0.1:5672/"}, wantErr: "exchange is required"},
		{
			name: "defaults_applied",
			cfg: Config{
				URL:      "amqp://guest:guest@127.0.0.1:5672/",
				Exchange: "test.events",
			},
			verify: func(t *testing.T, got Config) {
				require.Equal(t, DefaultExchangeType, got.ExchangeType)
				require.Equal(t, DefaultConnectTimeout, got.ConnectTimeout)
				require.Equal(t, DefaultPublishTimeout, got.PublishTimeout)
				require.Equal(t, DefaultCloseTimeout, got.CloseTimeout)
				require.NotNil(t, got.Logger)
			},
		},
		{
			name: "custom_values_preserved",
			cfg: Config{
				URL:            "amqp://guest:guest@127.0.0.1:5672/",
				Exchange:       "test.events",
				ExchangeType:   "direct",
				ConnectTimeout: time.Second,
				PublishTimeout: 2 * time.Second,
				CloseTimeout:   3 * time.Second,
				Logger:         &fakeLogger{},
			},
			verify: func(t *testing.T, got Config) {
				require.Equal(t, "direct", got.ExchangeType)
				require.Equal(t, time.Second, got.ConnectTimeout)
				require.Equal(t, 2*time.Second, got.PublishTimeout)
				require.Equal(t, 3*time.Second, got.CloseTimeout)
				require.IsType(t, &fakeLogger{}, got.Logger)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeConfig(tt.cfg)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			tt.verify(t, got)
		})
	}
}

// TestPublishUsesInfofLogger 验证发布成功时使用最小 Logger 接口记录日志。
func TestPublishUsesInfofLogger(t *testing.T) {
	logger := &fakeLogger{}
	client := &Client{
		exchange:  "test.events",
		name:      "mq-test",
		log:       logger,
		publisher: &stubPublisher{},
	}

	err := client.Publish(context.Background(), "test.order.created", []byte("payload"), WithMessageID("msg-1"))

	require.NoError(t, err)
	require.Len(t, logger.infos, 1)
	require.Contains(t, logger.infos[0], "action=publish")
	require.Contains(t, logger.infos[0], "message_id=msg-1")
}

// TestRoutingKeyValidation 验证发布 routing key 不允许使用通配符。
func TestRoutingKeyValidation(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr string
	}{
		{name: "valid", input: "test.order.created", want: "test.order.created"},
		{name: "trim", input: " test.order.deleted ", want: "test.order.deleted"},
		{name: "empty", input: " ", wantErr: "routing key is required"},
		{name: "star", input: "test.order.*", wantErr: "must not contain wildcard"},
		{name: "hash", input: "test.#", wantErr: "must not contain wildcard"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizePublishRoutingKey(tt.input)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestTopologyNames 验证 retry 和 DLQ 拓扑命名规则。
func TestTopologyNames(t *testing.T) {
	require.Equal(t, "test.events.retry", retryExchangeName("test.events"))
	require.Equal(t, "test.events.dlx", dlxExchangeName("test.events"))
	require.Equal(t, "example-service.retry.1500", retryQueueName("example-service", 1500*time.Millisecond))
	require.Equal(t, "example-service.dlq", dlqName("example-service"))
}

// TestTTLMilliseconds 验证延迟重试 TTL 取值。
func TestTTLMilliseconds(t *testing.T) {
	got, err := ttlMilliseconds(1500 * time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, int32(1500), got)

	_, err = ttlMilliseconds(0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "greater than 0")
}

// TestRetryCountFromHeaders 验证 retry count header 的兼容解析。
func TestRetryCountFromHeaders(t *testing.T) {
	tests := []struct {
		name string
		in   amqp.Table
		want int
	}{
		{name: "missing", in: amqp.Table{}, want: 0},
		{name: "int32", in: amqp.Table{retryCountHeader: int32(2)}, want: 2},
		{name: "int64", in: amqp.Table{retryCountHeader: int64(3)}, want: 3},
		{name: "string", in: amqp.Table{retryCountHeader: "4"}, want: 4},
		{name: "bad_string", in: amqp.Table{retryCountHeader: "bad"}, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, retryCountFromHeaders(tt.in))
		})
	}
}

// TestToMessage 验证 AMQP delivery 到通用消息模型的转换。
func TestToMessage(t *testing.T) {
	publishedAt := time.Unix(1710000000, 0)
	got := toMessage("test.events", "example-service", amqp.Delivery{
		RoutingKey:  "test.order.created",
		Body:        []byte("payload"),
		MessageId:   "msg-1",
		Timestamp:   publishedAt,
		DeliveryTag: 9,
		Headers: amqp.Table{
			"trace_id": "trace-1",
			"raw":      []byte("bytes"),
			"attempt":  int32(2),
		},
	})

	require.Equal(t, "test.events", got.Exchange)
	require.Equal(t, "example-service", got.Queue)
	require.Equal(t, "test.order.created", got.RoutingKey)
	require.Equal(t, []byte("payload"), got.Data)
	require.Equal(t, "msg-1", got.ID)
	require.Equal(t, "trace-1", got.Headers["trace_id"])
	require.Equal(t, "bytes", got.Headers["raw"])
	require.Equal(t, "2", got.Headers["attempt"])
	require.Equal(t, publishedAt, got.PublishedAt)
}

// TestToMessageIDFallback 验证无 message id 时使用 routing key 和 delivery tag 作为 fallback。
func TestToMessageIDFallback(t *testing.T) {
	got := toMessage("test.events", "example-service", amqp.Delivery{
		RoutingKey:  "test.order.created",
		DeliveryTag: 42,
	})
	require.Equal(t, "test.order.created:42", got.ID)
}

// TestPublishReturnHelpers 验证 mandatory return 的读取和清理逻辑。
func TestPublishReturnHelpers(t *testing.T) {
	returns := make(chan amqp.Return, 2)
	returns <- amqp.Return{Exchange: "test.events", RoutingKey: "test.order.missing", ReplyCode: 312, ReplyText: "NO_ROUTE"}
	publisher := &amqpPublisher{returns: returns}

	got := publisher.popReturned()
	require.NotNil(t, got)
	require.Equal(t, "NO_ROUTE", got.ReplyText)
	require.Contains(t, publishReturnError(*got).Error(), "message returned by broker")

	returns <- amqp.Return{ReplyText: "stale"}
	publisher.drainReturns()
	require.Nil(t, publisher.popReturned())

	closedReturns := make(chan amqp.Return)
	close(closedReturns)
	closedPublisher := &amqpPublisher{returns: closedReturns}
	require.NotPanics(t, closedPublisher.drainReturns)
	require.Contains(t, publishReturnError(*closedPublisher.popReturned()).Error(), "return channel closed")
}

// TestHandleDeliverySuccessAck 验证 handler 成功时会手动 ack。
func TestHandleDeliverySuccessAck(t *testing.T) {
	client := &Client{exchange: "test.events"}
	acker := &spyAcknowledger{}

	err := client.handleDelivery(context.Background(), SubscribeOptions{
		Queue:      "example-service",
		RoutingKey: "test.order.created",
		MaxDeliver: 2,
	}, amqp.Delivery{
		Acknowledger: acker,
		DeliveryTag:  7,
		RoutingKey:   "test.order.created",
		MessageId:    "msg-1",
	}, func(context.Context, Message) error {
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, uint64(7), acker.ackTag)
	require.False(t, acker.nackCalled)
}

// TestHandleDeliveryContextCanceledNacks 验证 handler 失败且上下文取消时不会进入 retry，而是重新入队。
func TestHandleDeliveryContextCanceledNacks(t *testing.T) {
	client := &Client{exchange: "test.events"}
	acker := &spyAcknowledger{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := client.handleDelivery(ctx, SubscribeOptions{
		Queue:      "example-service",
		RoutingKey: "test.order.created",
		MaxDeliver: 2,
	}, amqp.Delivery{
		Acknowledger: acker,
		DeliveryTag:  11,
		RoutingKey:   "test.order.created",
		MessageId:    "msg-1",
	}, func(context.Context, Message) error {
		return errors.New("shutdown")
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "canceled")
	require.True(t, acker.nackCalled)
	require.Equal(t, uint64(11), acker.nackTag)
	require.True(t, acker.requeue)
	require.False(t, acker.ackCalled)
}

// TestConsumeLoopUsesWarnfLogger 验证消费失败时使用最小 Logger 接口记录告警日志。
func TestConsumeLoopUsesWarnfLogger(t *testing.T) {
	logger := &fakeLogger{}
	client := &Client{
		exchange:  "test.events",
		log:       logger,
		publisher: &stubPublisher{err: errors.New("publish failed")},
		subs:      make(map[*subscription]struct{}),
	}
	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- amqp.Delivery{
		Acknowledger: &spyAcknowledger{},
		DeliveryTag:  12,
		RoutingKey:   "test.order.created",
		MessageId:    "msg-1",
	}
	close(deliveries)

	sub := &subscription{done: make(chan struct{})}
	client.consumeLoop(context.Background(), sub, SubscribeOptions{
		Queue:      "example-service",
		RoutingKey: "test.order.created",
		MaxDeliver: 2,
	}, deliveries, func(context.Context, Message) error {
		return errors.New("boom")
	})

	require.Len(t, logger.warns, 1)
	require.Contains(t, logger.warns[0], "action=handle_message")
	require.Contains(t, logger.warns[0], "routing_key=test.order.created")
}

// TestSubscriptionCloseWaitsForConsumeLoop 验证订阅关闭先取消 consumer，再等待消费循环释放 channel。
func TestSubscriptionCloseWaitsForConsumeLoop(t *testing.T) {
	channel := &spySubscriptionChannel{}
	done := make(chan struct{})
	close(done)
	sub := &subscription{
		channel:      channel,
		consumerTag:  "consumer-1",
		done:         done,
		closeTimeout: time.Second,
	}

	require.NoError(t, sub.Close())
	require.True(t, channel.cancelCalled)
	require.False(t, channel.closeCalled)
}

// TestSubscriptionCloseClosesChannelOnTimeout 验证消费循环未退出时关闭订阅会兜底释放 channel。
func TestSubscriptionCloseClosesChannelOnTimeout(t *testing.T) {
	channel := &spySubscriptionChannel{}
	sub := &subscription{
		channel:      channel,
		consumerTag:  "consumer-1",
		done:         make(chan struct{}),
		closeTimeout: time.Millisecond,
	}

	err := sub.Close()

	require.Error(t, err)
	require.Contains(t, err.Error(), "wait done timeout")
	require.True(t, channel.cancelCalled)
	require.True(t, channel.closeCalled)
}

// TestLockedAcknowledgerUsesSubscriptionChannelLock 验证消息确认与 consumer cancel 使用同一把锁。
func TestLockedAcknowledgerUsesSubscriptionChannelLock(t *testing.T) {
	sub := &subscription{}
	acker := &spyAcknowledger{}
	delivery := sub.lockDeliveryAcknowledger(amqp.Delivery{
		Acknowledger: acker,
		DeliveryTag:  13,
	})

	sub.channelMu.Lock()
	ackDone := make(chan error, 1)
	go func() {
		ackDone <- delivery.Ack(false)
	}()

	select {
	case err := <-ackDone:
		t.Fatalf("ack completed before channel lock released: %v", err)
	case <-time.After(10 * time.Millisecond):
	}

	sub.channelMu.Unlock()
	require.NoError(t, <-ackDone)
	require.True(t, acker.ackCalled)
	require.Equal(t, uint64(13), acker.ackTag)
}

// TestHandleDeliveryFailurePublishesRetry 验证首次失败时发布到 retry exchange 后 ack 原消息。
func TestHandleDeliveryFailurePublishesRetry(t *testing.T) {
	publisher := &stubPublisher{}
	client := &Client{exchange: "test.events", publisher: publisher}
	acker := &spyAcknowledger{}

	err := client.handleDelivery(context.Background(), SubscribeOptions{
		Queue:      "example-service",
		RoutingKey: "test.order.created",
		MaxDeliver: 3,
	}, amqp.Delivery{
		Acknowledger: acker,
		DeliveryTag:  8,
		RoutingKey:   "test.order.created",
		MessageId:    "msg-1",
		Body:         []byte("payload"),
	}, func(context.Context, Message) error {
		return errors.New("boom")
	})

	require.Error(t, err)
	require.Len(t, publisher.published, 1)
	require.Equal(t, "test.events.retry", publisher.published[0].exchange)
	require.Equal(t, "test.order.created", publisher.published[0].routingKey)
	require.True(t, publisher.published[0].mandatory)
	require.Equal(t, int32(1), publisher.published[0].publishing.Headers[retryCountHeader])
	require.Equal(t, uint64(8), acker.ackTag)
	require.False(t, acker.nackCalled)
}

// TestHandleDeliveryFailurePublishesDLQ 验证达到最大投递次数时发布到 DLX。
func TestHandleDeliveryFailurePublishesDLQ(t *testing.T) {
	publisher := &stubPublisher{}
	client := &Client{exchange: "test.events", publisher: publisher}
	acker := &spyAcknowledger{}

	err := client.handleDelivery(context.Background(), SubscribeOptions{
		Queue:      "example-service",
		RoutingKey: "test.order.created",
		MaxDeliver: 2,
	}, amqp.Delivery{
		Acknowledger: acker,
		DeliveryTag:  9,
		RoutingKey:   "test.order.created",
		MessageId:    "msg-1",
		Headers:      amqp.Table{retryCountHeader: int32(1)},
	}, func(context.Context, Message) error {
		return errors.New("boom")
	})

	require.Error(t, err)
	require.Len(t, publisher.published, 1)
	require.Equal(t, "test.events.dlx", publisher.published[0].exchange)
	require.Equal(t, int32(2), publisher.published[0].publishing.Headers[retryCountHeader])
	require.Equal(t, uint64(9), acker.ackTag)
}

// TestHandleDeliveryFailureNacksWhenRetryPublishFails 验证转发失败时原消息会重新入队。
func TestHandleDeliveryFailureNacksWhenRetryPublishFails(t *testing.T) {
	client := &Client{exchange: "test.events", publisher: &stubPublisher{err: errors.New("publish failed")}}
	acker := &spyAcknowledger{}

	err := client.handleDelivery(context.Background(), SubscribeOptions{
		Queue:      "example-service",
		RoutingKey: "test.order.created",
		MaxDeliver: 2,
	}, amqp.Delivery{
		Acknowledger: acker,
		DeliveryTag:  10,
		RoutingKey:   "test.order.created",
		MessageId:    "msg-1",
	}, func(context.Context, Message) error {
		return errors.New("boom")
	})

	require.Error(t, err)
	require.True(t, acker.nackCalled)
	require.Equal(t, uint64(10), acker.nackTag)
	require.True(t, acker.requeue)
	require.False(t, acker.ackCalled)
}

// TestClosedClientRejectsOperations 验证关闭状态会拒绝新操作。
func TestClosedClientRejectsOperations(t *testing.T) {
	client := &Client{closed: true, exchange: "test.events"}

	err := client.EnsureTopology(context.Background(), SubscribeOptions{
		Queue:      "example-service",
		RoutingKey: "test.order.created",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "client already closed")

	err = client.Publish(context.Background(), "test.order.created", []byte("payload"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "client already closed")

	sub, err := client.Subscribe(context.Background(), SubscribeOptions{
		Queue:      "example-service",
		RoutingKey: "test.order.created",
	}, func(context.Context, Message) error {
		return nil
	})
	require.Nil(t, sub)
	require.Error(t, err)
	require.Contains(t, err.Error(), "client already closed")
}

// TestUninitializedClientRejectsOperations 验证零值客户端会返回明确错误。
func TestUninitializedClientRejectsOperations(t *testing.T) {
	client := &Client{exchange: "test.events"}

	err := client.EnsureTopology(context.Background(), SubscribeOptions{
		Queue:      "example-service",
		RoutingKey: "test.order.created",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "client is not initialized")

	err = client.Publish(context.Background(), "test.order.created", []byte("payload"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "client is not initialized")

	sub, err := client.Subscribe(context.Background(), SubscribeOptions{
		Queue:      "example-service",
		RoutingKey: "test.order.created",
	}, func(context.Context, Message) error {
		return nil
	})
	require.Nil(t, sub)
	require.Error(t, err)
	require.Contains(t, err.Error(), "client is not initialized")
}

type publishedMessage struct {
	exchange   string
	routingKey string
	publishing amqp.Publishing
	mandatory  bool
}

type stubPublisher struct {
	published []publishedMessage
	err       error
}

func (s *stubPublisher) Publish(_ context.Context, exchange, routingKey string, publishing amqp.Publishing, mandatory bool) error {
	s.published = append(s.published, publishedMessage{
		exchange:   exchange,
		routingKey: routingKey,
		publishing: publishing,
		mandatory:  mandatory,
	})
	return s.err
}

type fakeLogger struct {
	infos  []string
	warns  []string
	errors []string
}

var _ Logger = (*fakeLogger)(nil)

func (f *fakeLogger) Infof(format string, args ...interface{}) {
	f.infos = append(f.infos, formatLog(format, args...))
}

func (f *fakeLogger) Warnf(format string, args ...interface{}) {
	f.warns = append(f.warns, formatLog(format, args...))
}

func (f *fakeLogger) Errorf(format string, args ...interface{}) {
	f.errors = append(f.errors, formatLog(format, args...))
}

func formatLog(format string, args ...interface{}) string {
	return fmt.Sprintf(format, args...)
}

type spyAcknowledger struct {
	ackCalled  bool
	ackTag     uint64
	nackCalled bool
	nackTag    uint64
	requeue    bool
}

func (s *spyAcknowledger) Ack(tag uint64, multiple bool) error {
	s.ackCalled = true
	s.ackTag = tag
	return nil
}

func (s *spyAcknowledger) Nack(tag uint64, multiple, requeue bool) error {
	s.nackCalled = true
	s.nackTag = tag
	s.requeue = requeue
	return nil
}

func (s *spyAcknowledger) Reject(uint64, bool) error {
	return nil
}

type spySubscriptionChannel struct {
	cancelCalled bool
	cancelTag    string
	closeCalled  bool
	cancelErr    error
	closeErr     error
}

func (s *spySubscriptionChannel) Cancel(consumer string, _ bool) error {
	s.cancelCalled = true
	s.cancelTag = consumer
	return s.cancelErr
}

func (s *spySubscriptionChannel) Close() error {
	s.closeCalled = true
	return s.closeErr
}

// TestIsTransportFailure 验证连接/channel 关闭类错误识别。
func TestIsTransportFailure(t *testing.T) {
	require.True(t, isTransportFailure(amqp.ErrClosed))
	require.True(t, isTransportFailure(fmt.Errorf("wrap: %w", amqp.ErrClosed)))
	require.True(t, isTransportFailure(&amqp.Error{Code: 504, Reason: "channel/connection is not open"}))
	require.True(t, isTransportFailure(errors.New(`Exception (504) Reason: "channel/connection is not open"`)))
	require.False(t, isTransportFailure(errors.New("message nacked by broker")))
}

// TestPublishOutcomeErrorSubmittedSemantics 验证已提交消息不会被自动重发。
func TestPublishOutcomeErrorSubmittedSemantics(t *testing.T) {
	publisher := &sequencePublisher{errs: []error{
		&publishOutcomeError{err: amqp.ErrClosed, submitted: true},
	}}
	client := &Client{
		exchange:  "test.events",
		publisher: publisher,
	}

	err := client.publishMessage(context.Background(), "test.events", "test.key", amqp.Publishing{Body: []byte("x")}, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "delivery outcome unknown")
	require.Equal(t, 1, publisher.calls)
}

// TestPublishRetriesWhenNotSubmitted 验证请求未发出时的关闭错误会重试一次。
func TestPublishRetriesWhenNotSubmitted(t *testing.T) {
	publisher := &sequencePublisher{errs: []error{
		&publishOutcomeError{err: amqp.ErrClosed, submitted: false},
		nil,
	}}
	client := &Client{
		exchange:  "test.events",
		publisher: publisher,
	}

	err := client.publishMessage(context.Background(), "test.events", "test.key", amqp.Publishing{Body: []byte("x")}, true)
	require.NoError(t, err)
	require.Equal(t, 2, publisher.calls)
}

// TestPublishDoesNotRetryOnNonTransportError 验证非传输层错误不会重试。
func TestPublishDoesNotRetryOnNonTransportError(t *testing.T) {
	publisher := &sequencePublisher{errs: []error{errors.New("boom")}}
	client := &Client{exchange: "test.events", publisher: publisher}

	err := client.publishMessage(context.Background(), "test.events", "test.key", amqp.Publishing{Body: []byte("x")}, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "boom")
	require.Equal(t, 1, publisher.calls)
}

// TestEnsureConnectedRejectsAfterClose 验证 Close 后不再重连。
func TestEnsureConnectedRejectsAfterClose(t *testing.T) {
	lifeCtx, cancel := context.WithCancel(context.Background())
	client := &Client{
		cfg:        Config{URL: "amqp://guest:guest@127.0.0.1:5672/"},
		lifeCtx:    lifeCtx,
		lifeCancel: cancel,
		log:        &fakeLogger{},
		subs:       make(map[*subscription]struct{}),
		broken:     true,
	}
	require.NoError(t, client.Close())

	err := client.ensureConnected(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "client already closed")
}

// TestBeginOrJoinReconnectSingleflight 验证并发 ensure 只会启动一次重连等待通道。
func TestBeginOrJoinReconnectSingleflight(t *testing.T) {
	lifeCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &Client{
		cfg:     Config{URL: "amqp://guest:guest@127.0.0.1:5672/"},
		lifeCtx: lifeCtx,
		log:     &fakeLogger{},
		broken:  true,
	}

	wait1, err := client.beginOrJoinReconnect()
	require.NoError(t, err)
	require.Nil(t, wait1)

	wait2, err := client.beginOrJoinReconnect()
	require.NoError(t, err)
	require.NotNil(t, wait2)

	finished := make(chan struct{})
	go func() {
		<-wait2
		close(finished)
	}()

	client.finishReconnect(nil)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("waiters were not released")
	}
}

// TestReconnectDelayWithJitter 验证退避抖动不会低于基准间隔。
func TestReconnectDelayWithJitter(t *testing.T) {
	base := 200 * time.Millisecond
	for i := 0; i < 20; i++ {
		got := reconnectDelayWithJitter(base)
		require.GreaterOrEqual(t, got, base)
		require.LessOrEqual(t, got, base+base/5+time.Millisecond)
	}
}

// TestMarkStoppedDoesNotCancelRunningContext 验证 Drain 使用的 markStopped 不会取消 handler context。
func TestMarkStoppedDoesNotCancelRunningContext(t *testing.T) {
	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()
	loopCtx, loopCancel := context.WithCancel(runCtx)

	sub := &subscription{
		parentCtx:  runCtx,
		runCancel:  runCancel,
		loopCancel: loopCancel,
		stopCh:     make(chan struct{}),
	}

	sub.markStopped()
	require.True(t, sub.isStopped())
	require.NoError(t, runCtx.Err())
	require.NoError(t, loopCtx.Err())
	select {
	case <-sub.stopCh:
	default:
		t.Fatal("stopCh should be closed after markStopped")
	}

	sub.cancelRunning()
	require.Error(t, runCtx.Err())
	require.Error(t, loopCtx.Err())
}

// TestDrainDoesNotCancelInFlightHandler 验证 Drain 等待 handler 完成且不取消其 context。
func TestDrainDoesNotCancelInFlightHandler(t *testing.T) {
	logger := &fakeLogger{}
	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	defer lifeCancel()
	client := &Client{
		exchange: "test.events",
		log:      logger,
		subs:     make(map[*subscription]struct{}),
		lifeCtx:  lifeCtx,
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	handlerStarted := make(chan struct{}, 1)
	releaseHandler := make(chan struct{})
	var canceledDuringHandler atomic.Bool
	sub := &subscription{
		client:    client,
		opts:      SubscribeOptions{Queue: "example-service", RoutingKey: "test.order.created", MaxDeliver: 2},
		parentCtx: runCtx,
		runCancel: runCancel,
		handler: func(ctx context.Context, _ Message) error {
			handlerStarted <- struct{}{}
			<-releaseHandler
			if ctx.Err() != nil {
				canceledDuringHandler.Store(true)
			}
			return nil
		},
		consumerTag:  "example-service",
		done:         make(chan struct{}),
		stopCh:       make(chan struct{}),
		closeTimeout: time.Second,
		channel:      &spySubscriptionChannel{},
	}

	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- amqp.Delivery{
		Acknowledger: &spyAcknowledger{},
		DeliveryTag:  1,
		RoutingKey:   "test.order.created",
		MessageId:    "msg-1",
	}

	client.mu.Lock()
	client.subs[sub] = struct{}{}
	client.mu.Unlock()
	go client.runSubscription(sub, deliveries)

	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}

	drainErr := make(chan error, 1)
	go func() {
		drainErr <- sub.Drain(context.Background())
	}()

	time.Sleep(50 * time.Millisecond)
	require.False(t, canceledDuringHandler.Load(), "Drain must not cancel in-flight handler context")

	close(releaseHandler)
	close(deliveries)

	select {
	case err := <-drainErr:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Drain did not finish after handler completed")
	}
	require.False(t, canceledDuringHandler.Load(), "handler context was canceled during Drain")
}

// TestCloseCancelsInFlightHandler 验证 Close 会取消正在执行的 handler context。
func TestCloseCancelsInFlightHandler(t *testing.T) {
	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	defer lifeCancel()
	client := &Client{
		exchange: "test.events",
		log:      &fakeLogger{},
		subs:     make(map[*subscription]struct{}),
		lifeCtx:  lifeCtx,
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	handlerStarted := make(chan context.Context, 1)
	sub := &subscription{
		client:    client,
		opts:      SubscribeOptions{Queue: "example-service", RoutingKey: "test.order.created", MaxDeliver: 2},
		parentCtx: runCtx,
		runCancel: runCancel,
		handler: func(ctx context.Context, _ Message) error {
			handlerStarted <- ctx
			<-ctx.Done()
			return ctx.Err()
		},
		consumerTag:  "example-service",
		done:         make(chan struct{}),
		stopCh:       make(chan struct{}),
		closeTimeout: time.Second,
		channel:      &spySubscriptionChannel{},
	}

	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- amqp.Delivery{
		Acknowledger: &spyAcknowledger{},
		DeliveryTag:  2,
		RoutingKey:   "test.order.created",
		MessageId:    "msg-2",
	}

	client.mu.Lock()
	client.subs[sub] = struct{}{}
	client.mu.Unlock()
	go client.runSubscription(sub, deliveries)

	var handlerCtx context.Context
	select {
	case handlerCtx = <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}

	require.NoError(t, sub.Close())
	select {
	case <-handlerCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel in-flight handler context")
	}
}

// TestSubscribeStartFailureDoesNotRegisterSubscription 验证首次建连失败时不会注册订阅或启动恢复。
func TestSubscribeStartFailureDoesNotRegisterSubscription(t *testing.T) {
	origDial := dialAMQP
	t.Cleanup(func() { dialAMQP = origDial })
	dialAMQP = func(string, amqp.Config) (*amqp.Connection, error) {
		return nil, errors.New("dial unavailable")
	}

	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	defer lifeCancel()
	client := &Client{
		cfg: Config{
			URL:          "amqp://guest:guest@127.0.0.1:5672/",
			Exchange:     "test.events",
			ExchangeType: DefaultExchangeType,
		},
		exchange:       "test.events",
		exchangeType:   DefaultExchangeType,
		publishTimeout: DefaultPublishTimeout,
		log:            &fakeLogger{},
		lifeCtx:        lifeCtx,
		lifeCancel:     lifeCancel,
		subs:           make(map[*subscription]struct{}),
		broken:         true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	sub, err := client.Subscribe(ctx, SubscribeOptions{
		Queue:      "example-service",
		RoutingKey: "test.order.created",
	}, func(context.Context, Message) error { return nil })

	require.Error(t, err)
	require.Nil(t, sub)
	client.mu.Lock()
	defer client.mu.Unlock()
	require.Empty(t, client.subs)
}

type sequencePublisher struct {
	errs  []error
	calls int
}

func (s *sequencePublisher) Publish(_ context.Context, _ string, _ string, _ amqp.Publishing, _ bool) error {
	idx := s.calls
	s.calls++
	if idx >= len(s.errs) {
		return nil
	}
	return s.errs[idx]
}
